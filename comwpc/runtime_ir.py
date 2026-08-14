from __future__ import annotations

import hashlib
from typing import Any

from comsdk.edge import Edge as RuntimeEdge
from comsdk.edge import InOutMapping
from comsdk.executors import build_executor_function
from comsdk.graph import Func
from comsdk.graph import Graph as RuntimeGraph
from comsdk.graph import Selector
from comsdk.graph import State as RuntimeState
from comsdk.graph import ThreadParallelizationPolicy
from comsdk.graph import Transfer as RuntimeTransfer
from comwpc.models import Edge as StoredEdge
from comwpc.models import Graph as StoredGraph
from comwpc.models import State as StoredState
from comwpc.models import Transfer as StoredTransfer

CURRENT_IR_VERSION = 1
CURRENT_PARSER_VERSION = "comsdk.parser"


def hash_source(source: str) -> str:
    return hashlib.sha256((source or "").encode("utf-8")).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return value


def _path_tuple(value: Any) -> tuple:
    if value in (None, ""):
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(value)
    return (value,)


def _keys_mapping_from_json(value: Any) -> dict:
    if not value:
        return {}
    return {str(key): _path_tuple(path) for key, path in value.items()}


def serialize_runtime_state(state: RuntimeState) -> dict[str, Any]:
    selector = state.selector
    parallelism = (
        "threading"
        if isinstance(state.parallelization_policy, ThreadParallelizationPolicy)
        else ""
    )
    return {
        "selector_module": getattr(selector, "module", "") or "",
        "selector_func": getattr(selector, "name", "") or "",
        "parallelism": parallelism,
        "runtime_attrs": {
            "possible_branches": _json_safe(getattr(state, "possible_branches", [])),
        },
    }


def serialize_runtime_edge(edge: RuntimeEdge) -> dict[str, Any]:
    io_mapping = getattr(edge, "_io_mapping", InOutMapping())
    executor_spec = getattr(edge.morph_f.func, "_comsdk_executor_spec", None) or {}
    return {
        "executor_type": str(executor_spec.get("executor_type") or ""),
        "executor_operation": str(executor_spec.get("operation") or ""),
        "executor_input_key": str(executor_spec.get("input_key") or ""),
        "executor_output_key": str(executor_spec.get("output_key") or ""),
        "executor_options": _json_safe(executor_spec.get("options") or {}),
        "keys_mapping": _json_safe(getattr(io_mapping, "_keys_mapping", {})),
        "relative_keys": _json_safe(getattr(io_mapping, "_relative_keys", ())),
        "default_relative_key": _json_safe(
            getattr(io_mapping, "_default_relative_key", ())
        ),
        "mandatory_keys": _json_safe(getattr(edge, "mandatory_keys", ())),
        "use_proxy_data_for_pre_post_processing": bool(
            getattr(edge, "use_proxy_data_for_pre_post_processing", False)
        ),
        "runtime_attrs": {},
    }


def _build_selector(state: StoredState, transfers_count: int) -> Selector:
    return Selector(
        transfers_count,
        module=state.selector_module or "",
        name=state.selector_func or "",
    )


def _normalize_parallel_executor(execution_options: dict[str, Any] | None) -> str:
    parallel_executor = str(
        (execution_options or {}).get("parallel_executor") or "threading"
    ).strip()
    if parallel_executor in ("", "threading"):
        return "threading"
    if parallel_executor == "serial":
        return "serial"
    if parallel_executor == "grpc_worker_pool":
        raise ValueError(
            "parallel_executor=grpc_worker_pool is not implemented. "
            "Use parallel_executor=threading with executor=remote_cpp for the "
            "current HTTP C++ worker pool."
        )
    raise ValueError(f"Unsupported parallel_executor: {parallel_executor}")


def _build_parallelization_policy(state: StoredState, parallel_executor: str):
    if parallel_executor == "serial":
        return None
    if state.parallelism == "threading":
        return ThreadParallelizationPolicy()
    return None


def _executor_spec_from_db(edge: StoredEdge) -> dict[str, str]:
    if edge.executor_type:
        return {
            "executor_type": edge.executor_type,
            "operation": edge.executor_operation,
            "input_key": edge.executor_input_key,
            "output_key": edge.executor_output_key,
        }
    if (
        edge.morph_module == "comsdk.executors"
        and edge.morph_func.startswith("remote_cpp_")
    ):
        operation = edge.morph_func.removeprefix("remote_cpp_")
        return {
            "executor_type": "remote_cpp",
            "operation": operation,
            "input_key": "",
            "output_key": "",
        }
    return {}


def _build_runtime_edge(edge: StoredEdge, order: int) -> RuntimeEdge:
    io_mapping = InOutMapping(
        keys_mapping=_keys_mapping_from_json(edge.keys_mapping),
        relative_keys=_path_tuple(edge.relative_keys),
        default_relative_key=_path_tuple(edge.default_relative_key),
    )
    executor_spec = _executor_spec_from_db(edge)
    if executor_spec:
        executor_func = build_executor_function(
            executor=executor_spec["executor_type"],
            operation=executor_spec["operation"],
            input_key=executor_spec["input_key"],
            output_key=executor_spec["output_key"],
        )
        morph_func = Func(
            module="comsdk.executors",
            name=executor_func.__name__,
            func=executor_func,
        )
    else:
        morph_func = Func(edge.morph_module or "", edge.morph_func or "")
    runtime_edge = RuntimeEdge(
        Func(edge.pred_module or "", edge.pred_func or ""),
        morph_func,
        io_mapping=io_mapping,
        order=order,
        comment=edge.comment or "",
        mandatory_keys=tuple(edge.mandatory_keys or ()),
    )
    runtime_edge.use_proxy_data_for_pre_post_processing = (
        edge.use_proxy_data_for_pre_post_processing
    )
    return runtime_edge


def _choose_init_state(states: list[StoredState], transfers: list[StoredTransfer]):
    by_name = {state.name: state for state in states}
    if "__BEGIN__" in by_name:
        return by_name["__BEGIN__"]

    incoming_ids = {transfer.target_id for transfer in transfers}
    for state in states:
        if state.id not in incoming_ids:
            return state
    raise ValueError("Graph has no initial state")


def _choose_terminal_state(states: list[StoredState], transfers: list[StoredTransfer]):
    by_name = {state.name: state for state in states}
    if "__END__" in by_name:
        return by_name["__END__"]

    for state in states:
        if state.is_terminal:
            return state

    source_ids = {transfer.source_id for transfer in transfers}
    for state in states:
        if state.id not in source_ids:
            return state
    return None


def build_comsdk_graph_from_db(
    graph: StoredGraph,
    _stack: set[int] | None = None,
    execution_options: dict[str, Any] | None = None,
) -> RuntimeGraph:
    if graph.pk is None:
        raise ValueError("Cannot build runtime graph from an unsaved Graph")

    stack = set() if _stack is None else set(_stack)
    if graph.pk in stack:
        raise ValueError(f"Recursive subgraph reference detected for graph {graph.pk}")
    stack.add(graph.pk)
    parallel_executor = _normalize_parallel_executor(execution_options)

    states = list(
        graph.state_set.select_related("subgraph").order_by("id")
    )
    transfers = list(
        StoredTransfer.objects.filter(graph=graph)
        .select_related("source", "target", "edge")
        .order_by("source_id", "order", "id")
    )
    if not states:
        raise ValueError(f"Graph {graph.pk} has no stored IR states")

    state_map: dict[int, RuntimeState] = {}
    for stored_state in states:
        runtime_state = RuntimeState(
            stored_state.name,
            parallelization_policy=_build_parallelization_policy(
                stored_state,
                parallel_executor,
            ),
            array_keys_mapping=stored_state.array_keys_mapping,
        )
        runtime_state.comment = stored_state.comment or None
        runtime_state.is_term_state = stored_state.is_terminal
        state_map[stored_state.id] = runtime_state

    for stored_transfer in transfers:
        source = state_map[stored_transfer.source_id]
        target = state_map[stored_transfer.target_id]
        runtime_edge = _build_runtime_edge(stored_transfer.edge, stored_transfer.order)
        source.transfers.append(
            RuntimeTransfer(
                runtime_edge,
                target,
                order=stored_transfer.order,
                event_id=stored_transfer.id,
            )
        )

    for stored_state in states:
        runtime_state = state_map[stored_state.id]
        runtime_state.selector = _build_selector(
            stored_state,
            transfers_count=len(runtime_state.transfers),
        )

    for stored_state in states:
        if not stored_state.subgraph_id:
            continue
        subgraph_runtime = build_comsdk_graph_from_db(
            stored_state.subgraph,
            _stack=stack,
            execution_options=execution_options,
        )
        state_map[stored_state.id].replace_with_graph(subgraph_runtime)

    init_state = _choose_init_state(states, transfers)
    term_state = _choose_terminal_state(states, transfers)
    return RuntimeGraph(
        state_map[init_state.id],
        state_map[term_state.id] if term_state is not None else None,
        graph_id=str(graph.pk),
    )
