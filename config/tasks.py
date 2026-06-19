import logging
import threading

from asgiref.sync import async_to_sync
from celery import shared_task
from channels.layers import get_channel_layer

from comwpc.execution_history import record_execution_events
from comwpc.models import Graph as StoredGraph
from comwpc.runtime_ir import build_comsdk_graph_from_db

logger = logging.getLogger(__name__)

HISTORY_EVENT_BATCH_SIZE = 1


def _normalize_graph_id(graph_id):
    try:
        return int(graph_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "execute_graph_task expects graph_id, not raw DOT/aDOT text. "
            "Restart web and Celery workers after the DB IR migration and "
            "drop old queued execution tasks."
        ) from exc


def publish_execution_ws_event(session_id, event):
    channel_layers = get_channel_layer()
    if channel_layers is None:
        return
    async_to_sync(channel_layers.group_send)(
        f"execution_{session_id}",
        {
            "type": "execution.event",
            "payload": event,
        },
    )


@shared_task
def execute_graph_task(graph_id, session_id, initial_data):
    listener_lock = threading.Lock()
    history_event_buffer = []
    event_sequence = 0

    def flush_history_events():
        if not history_event_buffer:
            return
        events_to_persist = history_event_buffer[:]
        history_event_buffer.clear()
        record_execution_events(session_id, events_to_persist)

    try:
        stored_graph = StoredGraph.objects.get(pk=_normalize_graph_id(graph_id))
        comsdk_graph = build_comsdk_graph_from_db(
            stored_graph,
            execution_options=initial_data if isinstance(initial_data, dict) else {},
        )

        def event_listener(event):
            nonlocal event_sequence
            with listener_lock:
                event_sequence += 1
                event = event.copy()
                event["sequence"] = event_sequence
                event["graph_id"] = stored_graph.name
                event["graph_pk"] = stored_graph.pk
                event["session_id"] = session_id
                publish_execution_ws_event(session_id, event)

                event_type = str(event.get("event") or "")
                history_event_buffer.append(event.copy())
                if (
                    len(history_event_buffer) >= HISTORY_EVENT_BATCH_SIZE
                    or event_type in {"complete", "error"}
                ):
                    flush_history_events()

        comsdk_graph.add_listener(event_listener)

        if not comsdk_graph.run(initial_data):
            raise RuntimeError(
                str(initial_data.get("__EXCEPTION__") or "Graph execution failed")
            )
    except Exception as exc:
        event_sequence += 1
        error_event = {
            "event": "error",
            "message": str(exc),
            "session_id": session_id,
            "sequence": event_sequence,
        }
        with listener_lock:
            publish_execution_ws_event(session_id, error_event)
            history_event_buffer.append(error_event)
            flush_history_events()
        logger.exception("Graph execution failed: %s", exc)
        raise
    finally:
        with listener_lock:
            flush_history_events()
