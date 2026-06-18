from django.db.models import QuerySet

from .execution_inputs import build_execution_input_context


SERVICE_STATES = ["__BEGIN__", "__END__"]


def build_graph_payload(graph, include_subgraphs: bool = False):
    """
    Что делает: REST-представление одного графа для ReactFlow-визуализации.
    Место: REST-представление одного графа для ReactFlow-визуализации.
    Вход: модель Graph и флаг рекурсивной загрузки подграфов.
    Выход: JSON-совместимый словарь с nodes, edges, схемой запуска и optional subgraphs.
    """
    nodes = []
    nested_subgraphs = {}

    for state in graph.state_set.exclude(name__in=SERVICE_STATES).select_related("subgraph"):
        subgraph_id = state.subgraph.id if state.subgraph else None
        if include_subgraphs and state.subgraph:
            nested_subgraphs[str(subgraph_id)] = build_graph_payload(
                state.subgraph,
                include_subgraphs=True,
            )

        nodes.append(
            {
                "id": str(state.id),
                "label": state.name,
                "comment": state.comment or "",
                "is_terminal": bool(getattr(state, "is_terminal", False)),
                "subgraph": subgraph_id,
                "selector_module": state.selector_module or "",
                "selector_func": state.selector_func or "",
                "parallelism": state.parallelism or "",
                "runtime_attrs": state.runtime_attrs or {},
            }
        )

    edges = []
    for transfer in graph.transfer_set.select_related("source", "target", "edge"):
        if transfer.source.name in SERVICE_STATES or transfer.target.name in SERVICE_STATES:
            continue

        edges.append(
            {
                "id": f"{transfer.source.id}-{transfer.target.id}",
                "source": str(transfer.source.id),
                "target": str(transfer.target.id),
                "label": (transfer.edge.comment if transfer.edge else "") or "",
                "comment": (transfer.edge.comment if transfer.edge else "") or "",
                "order": transfer.order,
                "arrow_type": transfer.arrow_type or "->",
                "pred_module": (transfer.edge.pred_module if transfer.edge else "") or "",
                "pred_func": (transfer.edge.pred_func if transfer.edge else "") or "",
                "morph_module": (transfer.edge.morph_module if transfer.edge else "") or "",
                "morph_func": (transfer.edge.morph_func if transfer.edge else "") or "",
                "executor_type": (transfer.edge.executor_type if transfer.edge else "") or "",
                "executor_operation": (
                    transfer.edge.executor_operation if transfer.edge else ""
                )
                or "",
                "executor_input_key": (
                    transfer.edge.executor_input_key if transfer.edge else ""
                )
                or "",
                "executor_output_key": (
                    transfer.edge.executor_output_key if transfer.edge else ""
                )
                or "",
                "executor_options": (
                    transfer.edge.executor_options if transfer.edge else {}
                )
                or {},
                "keys_mapping": (transfer.edge.keys_mapping if transfer.edge else {}) or {},
                "mandatory_keys": (
                    transfer.edge.mandatory_keys if transfer.edge else []
                )
                or [],
                "runtime_attrs": transfer.runtime_attrs or {},
            }
        )

    execution_input_schema, execution_input_error = build_execution_input_context(graph.raw_aini)
    payload = {
        "id": graph.id,
        "name": graph.name,
        "nodes": nodes,
        "edges": edges,
        "execution_input_schema": execution_input_schema,
        "execution_input_error": execution_input_error,
    }

    if include_subgraphs:
        payload["subgraphs"] = nested_subgraphs

    return payload


def build_graph_list_payload(graphs: QuerySet):
    """
    Что делает: REST-представление списка графов на стартовой странице.
    Место: REST-представление списка графов на стартовой странице.
    Вход: QuerySet Graph, обычно уже annotated счетчиками и датой последнего запуска.
    Выход: список JSON-совместимых словарей с краткими метаданными графов.
    """
    data = []

    for graph in graphs:
        last_execution_at = getattr(graph, "last_execution_at", None)
        data.append(
            {
                "id": graph.id,
                "name": graph.name,
                "is_subgraph": graph.is_subgraph,
                "nodes_count": getattr(graph, "nodes_count", None),
                "edges_count": getattr(graph, "edges_count", None),
                "execution_count": getattr(graph, "execution_count", 0),
                "last_execution_at": last_execution_at.isoformat() if last_execution_at else None,
            }
        )

    return data
