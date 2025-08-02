import tempfile

from celery import shared_task
from comsdk.graph import Graph as ComsdkGraph
from comsdk.parser import Parser
from comwpc.events import get_event_service


@shared_task
def execute_graph_task(dot_content, session_id, initial_data):
    parser = Parser()
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.adot') as tmp:
        tmp.write(dot_content)
        tmp.seek(0)
        comsdk_graph = parser.parse_file(tmp.name)

        def event_listener(event):
            # Добавляем имя графа в событие
            event['graph_id'] = parser.fact.name
            event['session_id'] = session_id
            get_event_service().publish(session_id, event)

        comsdk_graph.add_listener(event_listener)

        # Рекурсивная обработка подграфов
        def process_subgraphs(graph):
            for state in graph.states:
                if state.subgraph:
                    process_subgraphs(state.subgraph)
                    # Добавляем обработчик событий для подграфа
                    state.subgraph.add_listener(event_listener)

        process_subgraphs(comsdk_graph)
        comsdk_graph.run(initial_data)