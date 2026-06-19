import logging
import math
import time
from datetime import datetime, timezone as dt_timezone

from django.db import OperationalError, close_old_connections, transaction
from django.db.models import Prefetch
from django.utils import timezone

from .models import Graph, GraphExecutionEvent, GraphExecutionSession

logger = logging.getLogger(__name__)

EVENT_WRITE_ATTEMPTS = 3
EVENT_WRITE_RETRY_DELAY_SECONDS = 0.05


def create_execution_session(graph: Graph, session_id: str, initial_data: dict) -> GraphExecutionSession:
    """
    Что делает: стартовая запись истории перед отправкой графа в Celery.
    Место: стартовая запись истории перед отправкой графа в Celery.
    Вход: граф, UUID-сессия исполнения и начальные данные запуска.
    Выход: созданная GraphExecutionSession со статусом pending.
    """
    return GraphExecutionSession.objects.create(
        graph=graph,
        session_id=session_id,
        initial_data=_json_safe_dict(initial_data or {}),
    )


def list_graph_execution_sessions(graph: Graph, limit: int = 12):
    """
    Что делает: API истории запусков на странице просмотра графа.
    Место: API истории запусков на странице просмотра графа.
    Вход: граф и максимальное число сессий.
    Выход: список последних GraphExecutionSession с заранее подгруженными событиями.
    """
    events_qs = GraphExecutionEvent.objects.order_by("sequence")
    queryset = (
        GraphExecutionSession.objects.filter(graph=graph)
        .prefetch_related(Prefetch("events", queryset=events_qs))
        .order_by("-created_at")
    )
    return list(queryset[:limit])


def serialize_execution_event(event: GraphExecutionEvent) -> dict:
    """
    Что делает: сериализация события истории для REST-ответа фронтенду.
    Место: сериализация события истории для REST-ответа фронтенду.
    Вход: GraphExecutionEvent из БД.
    Выход: JSON-совместимый словарь события.
    """
    serialized = dict(event.raw_event or {})
    serialized.update({
        "sequence": event.sequence,
        "event": event.event_type,
        "state": event.state,
        "message": event.message,
        "timestamp": event.occurred_at.timestamp(),
        "data": event.payload,
    })
    return serialized


def serialize_execution_session(session: GraphExecutionSession) -> dict:
    """
    Что делает: сериализация одной сессии истории исполнения.
    Место: сериализация одной сессии истории исполнения.
    Вход: GraphExecutionSession с events relation.
    Выход: JSON-совместимый словарь с метаданными сессии и списком событий.
    """
    return {
        "session_id": session.session_id,
        "status": session.status,
        "created_at": session.created_at.isoformat(),
        "finished_at": session.finished_at.isoformat() if session.finished_at else None,
        "initial_data": session.initial_data or {},
        "last_state": session.last_state,
        "event_count": session.event_count,
        "error_message": session.error_message,
        "events": [serialize_execution_event(event) for event in session.events.all()],
    }


def serialize_execution_sessions(sessions) -> list[dict]:
    """
    Что делает: сериализация списка запусков для endpoint'а истории.
    Место: сериализация списка запусков для endpoint'а истории.
    Вход: iterable GraphExecutionSession.
    Выход: список словарей, готовый для JsonResponse.
    """
    return [serialize_execution_session(session) for session in sessions]


def record_execution_event(session_id: str, event: dict) -> None:
    """
    Что делает: единая точка записи live-событий исполнения в историю.
    Место: единая точка записи live-событий исполнения в историю.
    Вход: session_id и событие из execution listener.
    Выход: None; создает GraphExecutionEvent и обновляет статус GraphExecutionSession.
    """
    record_execution_events(session_id, [event])


def record_execution_events(session_id: str, events: list[dict]) -> None:
    events = [event for event in events if isinstance(event, dict)]
    if not events:
        return

    for attempt in range(EVENT_WRITE_ATTEMPTS):
        try:
            _record_execution_events_once(session_id, events)
            return
        except GraphExecutionSession.DoesNotExist:
            logger.warning("Execution session %s was not found for event persistence", session_id)
            return
        except OperationalError:
            close_old_connections()
            if attempt == EVENT_WRITE_ATTEMPTS - 1:
                logger.exception("Could not persist execution event for session %s", session_id)
                return
            time.sleep(EVENT_WRITE_RETRY_DELAY_SECONDS * (attempt + 1))


def _record_execution_events_once(session_id: str, events: list[dict]) -> None:
    with transaction.atomic():
        session = GraphExecutionSession.objects.select_for_update().get(session_id=session_id)
        sequence = session.event_count
        event_models = []

        for event in events:
            sequence += 1
            occurred_at = _resolve_event_timestamp(event)
            event_models.append(
                GraphExecutionEvent(
                    session=session,
                    sequence=sequence,
                    event_type=str(event.get("event") or ""),
                    state=str(event.get("state") or ""),
                    message=str(event.get("message") or ""),
                    payload=_json_safe_dict(event.get("data")),
                    raw_event=_json_safe_dict(event),
                    occurred_at=occurred_at,
                )
            )
            session.last_state = str(event.get("state") or session.last_state or "")
            _update_session_status(session, event, occurred_at)

        GraphExecutionEvent.objects.bulk_create(event_models)
        session.event_count = sequence
        session.save(
            update_fields=[
                "event_count",
                "last_state",
                "status",
                "finished_at",
                "error_message",
            ]
        )


def _json_safe_dict(value) -> dict:
    value = _json_safe_value(value)
    return value if isinstance(value, dict) else {}


def _json_safe_value(value):
    if isinstance(value, dict):
        return {
            str(key): _json_safe_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return repr(value)


def _resolve_event_timestamp(event: dict):
    """
    Что делает: нормализация времени события перед записью в БД.
    Место: нормализация времени события перед записью в БД.
    Вход: словарь события с optional numeric timestamp.
    Выход: timezone-aware datetime; при отсутствии timestamp возвращает текущее время.
    """
    timestamp = event.get("timestamp")
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp, tz=dt_timezone.utc)
    return timezone.now()


def _update_session_status(session: GraphExecutionSession, event: dict, occurred_at):
    """
    Что делает: бизнес-правила статуса сессии исполнения.
    Место: бизнес-правила статуса сессии исполнения.
    Вход: mutable GraphExecutionSession, событие и время события.
    Выход: None; меняет status, finished_at и error_message на переданной модели.
    """
    event_type = str(event.get("event") or "")
    message = str(event.get("message") or "")

    if event_type == "error":
        session.status = GraphExecutionSession.STATUS_FAILED
        session.error_message = message
        session.finished_at = occurred_at
        return

    if event_type == "complete":
        session.status = GraphExecutionSession.STATUS_COMPLETED
        session.finished_at = occurred_at
        return

    if event_type:
        session.status = GraphExecutionSession.STATUS_RUNNING
