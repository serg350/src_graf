import logging
from datetime import datetime, timezone as dt_timezone

from django.db import transaction
from django.db.models import Prefetch
from django.utils import timezone

from .models import Graph, GraphExecutionEvent, GraphExecutionSession

logger = logging.getLogger(__name__)


def create_execution_session(graph: Graph, session_id: str, initial_data: dict) -> GraphExecutionSession:
    return GraphExecutionSession.objects.create(
        graph=graph,
        session_id=session_id,
        initial_data=initial_data or {},
    )


def list_graph_execution_sessions(graph: Graph, limit: int = 12):
    events_qs = GraphExecutionEvent.objects.order_by("sequence")
    queryset = (
        GraphExecutionSession.objects.filter(graph=graph)
        .prefetch_related(Prefetch("events", queryset=events_qs))
        .order_by("-created_at")
    )
    return list(queryset[:limit])


def serialize_execution_event(event: GraphExecutionEvent) -> dict:
    return {
        "sequence": event.sequence,
        "event": event.event_type,
        "state": event.state,
        "message": event.message,
        "timestamp": event.occurred_at.timestamp(),
        "data": event.payload,
    }


def serialize_execution_session(session: GraphExecutionSession) -> dict:
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
    return [serialize_execution_session(session) for session in sessions]


def record_execution_event(session_id: str, event: dict) -> None:
    try:
        with transaction.atomic():
            session = GraphExecutionSession.objects.select_for_update().get(session_id=session_id)
            sequence = session.event_count + 1
            occurred_at = _resolve_event_timestamp(event)
            payload = event.get("data")
            if not isinstance(payload, dict):
                payload = {}

            GraphExecutionEvent.objects.create(
                session=session,
                sequence=sequence,
                event_type=str(event.get("event") or ""),
                state=str(event.get("state") or ""),
                message=str(event.get("message") or ""),
                payload=payload,
                raw_event=event,
                occurred_at=occurred_at,
            )

            session.event_count = sequence
            session.last_state = str(event.get("state") or session.last_state or "")
            _update_session_status(session, event, occurred_at)
            session.save(
                update_fields=[
                    "event_count",
                    "last_state",
                    "status",
                    "finished_at",
                    "error_message",
                ]
            )
    except GraphExecutionSession.DoesNotExist:
        logger.warning("Execution session %s was not found for event persistence", session_id)


def _resolve_event_timestamp(event: dict):
    timestamp = event.get("timestamp")
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp, tz=dt_timezone.utc)
    return timezone.now()


def _update_session_status(session: GraphExecutionSession, event: dict, occurred_at):
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
