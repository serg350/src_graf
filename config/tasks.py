import tempfile
import logging
import threading

from asgiref.sync import async_to_sync
from celery import shared_task
from channels.layers import get_channel_layer

from comsdk.parser import Parser
from comwpc.execution_history import record_execution_events

logger = logging.getLogger(__name__)

HISTORY_EVENT_BATCH_SIZE = 25
PERSISTED_EXECUTION_EVENT_TYPES = {"state_enter", "complete", "error"}


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
def execute_graph_task(dot_content, session_id, initial_data):
    """
    Что делает: Celery-задача фонового исполнения графа после start_execution.
    Место: Celery-задача фонового исполнения графа после start_execution.
    Вход: DOT/aDOT-текст графа, session_id истории запуска и initial_data для функций графа.
    Выход: None; публикует live-события, пишет историю и отправляет error-событие при исключении.
    """
    listener_lock = threading.Lock()
    history_event_buffer = []

    def flush_history_events():
        if not history_event_buffer:
            return
        events_to_persist = history_event_buffer[:]
        history_event_buffer.clear()
        record_execution_events(session_id, events_to_persist)

    try:
        parser = Parser()
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.adot') as tmp:
            tmp.write(dot_content)
            tmp.seek(0)
            comsdk_graph = parser.parse_file(tmp.name)
            def event_listener(event):
                """
                Что делает: callback comsdk-графа внутри Celery-задачи.
                Место: callback comsdk-графа внутри Celery-задачи.
                Вход: dict события state_enter/state_exit/complete/error от исполнителя.
                Выход: None; дополняет событие graph_id/session_id и публикует его в event service.
                """
                with listener_lock:
                    event['graph_id'] = parser.fact.name
                    event['session_id'] = session_id
                    publish_execution_ws_event(session_id, event)
                    event_type = str(event.get("event") or "")
                    if event_type in PERSISTED_EXECUTION_EVENT_TYPES:
                        history_event_buffer.append(event.copy())
                    if (
                        len(history_event_buffer) >= HISTORY_EVENT_BATCH_SIZE
                        or event_type in {"complete", "error"}
                    ):
                        flush_history_events()

            comsdk_graph.add_listener(event_listener)

            # Рекурсивная обработка подграфов
            def process_subgraphs(graph):
                """
                Что делает: подключение общего listener ко всем вложенным comsdk-подграфам.
                Место: подключение общего listener ко всем вложенным comsdk-подграфам.
                Вход: comsdk Graph или подграф.
                Выход: None; рекурсивно регистрирует event_listener на найденных подграфах.
                """
                for state in graph.states:
                    if hasattr(state, 'subgraph') and state.subgraph:
                        # Добавляем обработчик для подграфа
                        state.subgraph.add_listener(event_listener)
                        process_subgraphs(state.subgraph)

            process_subgraphs(comsdk_graph)
            if not comsdk_graph.run(initial_data):
                raise RuntimeError(
                    str(initial_data.get("__EXCEPTION__") or "Graph execution failed")
                )
    except Exception as e:
        # Отправляем событие об ошибке
        error_event = {
            'event': 'error',
            'message': str(e),
            'session_id': session_id
        }
        with listener_lock:
            publish_execution_ws_event(session_id, error_event)
            history_event_buffer.append(error_event)
            flush_history_events()
        logger.exception(f"Ошибка выполнения графа: {str(e)}")
        raise
    finally:
        with listener_lock:
            flush_history_events()
