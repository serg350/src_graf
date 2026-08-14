import React, { useCallback, useEffect, useRef, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  useEdgesState,
  useNodesState,
} from "reactflow";
import "reactflow/dist/style.css";

import { loadGraphDeep } from "../services/api";
import { flattenGraphWithSubgraphs } from "../utils/graph_parser";
import { applyDagreLayout } from "../utils/dagreLayout";

import CustomEdge from "./CustomEdge";
import ExecutionStatusBar from "./ExecutionStatusBar";
import GraphNode from "./GraphNode";
import SubgraphModal from "./SubgraphModal";
import SubgraphNode from "./SubgraphNode";

const NODE_TYPES = { graph: GraphNode, subgraph: SubgraphNode };
const EDGE_TYPES = { default: CustomEdge };

function findSubgraphById(graph, targetId, breadcrumb = []) {
  if (!graph) {
    return null;
  }

  const nextBreadcrumb = [...breadcrumb, { id: graph.id, label: graph.name }];
  if (String(graph.id) === String(targetId)) {
    return { graph, breadcrumb: nextBreadcrumb };
  }

  for (const subgraph of Object.values(graph.subgraphs || {})) {
    const result = findSubgraphById(subgraph, targetId, nextBreadcrumb);
    if (result) {
      return result;
    }
  }

  return null;
}

function buildFlowState(
  rootGraph,
  orientation,
  showSubgraphs,
  handleOpenSubgraph,
  handleSelectNode,
  handleSelectEdge
) {
  const flat = flattenGraphWithSubgraphs(rootGraph, showSubgraphs);
  const dagre = applyDagreLayout(flat.nodes, flat.edges, orientation);

  const nodes = dagre.nodes.map((node) => {
    const stateId = node.data?.label || String(node.id);
    const selection = {
      type: "node",
      id: String(node.id),
      label: node.data?.label || String(node.id),
      stateId,
      descendantStates: node.data?.descendantStates || [],
      isSubgraph: node.type === "subgraph",
    };

    return {
      ...node,
      id: String(node.id),
      draggable: true,
      type: node.type === "subgraph" ? "subgraph" : "graph",
      data: {
        ...node.data,
        stateId,
        onOpenSubgraph: handleOpenSubgraph,
        onSelectNode: () => handleSelectNode?.(selection),
        selected: false,
        relatedSelected: false,
      },
      style: {
        width: node.width,
        height: node.height,
      },
    };
  });

  const nodeById = new Map(nodes.map((node) => [node.id, node]));
  let backwardLane = 0;
  const edges = dagre.edges.map((edge) => {
    const sourceNode = nodeById.get(String(edge.source));
    const targetNode = nodeById.get(String(edge.target));
    const from = sourceNode?.data.stateId;
    const to = targetNode?.data.stateId;
    const isBackEdge =
      orientation === "LR"
        ? sourceNode?.position.x >= targetNode?.position.x
        : sourceNode?.position.y >= targetNode?.position.y;
    const routingLane = isBackEdge ? backwardLane++ : 0;
    const edgeSelection = {
      type: "edge",
      id: String(edge.id),
      edgeId: String(edge.id),
      label: edge.label || edge.comment || `${from || "?"} -> ${to || "?"}`,
      fromState: from,
      toState: to,
      edgeOrder: edge.order ?? 0,
    };

    return {
      id: String(edge.id),
      source: String(edge.source),
      target: String(edge.target),
      type: "default",
      label: edge.label,
      data: {
        fromState: from,
        toState: to,
        edgeId: String(edge.id),
        edgeOrder: edge.order ?? 0,
        comment: edge.comment || edge.label || "",
        executorType: edge.executor_type || "",
        executorOperation: edge.executor_operation || "",
        orientation,
        isBackEdge,
        routingLane,
        execution: null,
        selected: false,
        relatedSelected: false,
        onSelectEdge: () => handleSelectEdge?.(edgeSelection),
      },
    };
  });

  return { nodes, edges };
}

function getEdgeTime(edge) {
  const value = edge?.duration_ms ?? edge?.worker_elapsed_ms;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function getLoadPercent(edge) {
  const candidates = [
    edge?.cpu_load,
    edge?.cpu_percent,
    edge?.worker_cpu_percent,
    edge?.compute_load,
    edge?.load_percent,
    edge?.utilization,
    edge?.data?.cpu_load,
    edge?.data?.cpu_percent,
    edge?.data?.compute_load,
    edge?.data?.utilization,
  ];
  const value = candidates.find((candidate) => candidate !== null && candidate !== undefined);
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return null;
  }

  return number <= 1 ? number * 100 : number;
}

function getMemoryBytes(edge) {
  const candidates = [
    edge?.memory_bytes,
    edge?.memory_rss_bytes,
    edge?.worker_memory_bytes,
    edge?.memory_usage_bytes,
    edge?.data?.memory_bytes,
    edge?.data?.memory_rss_bytes,
    edge?.data?.memory_usage_bytes,
  ];
  const bytesValue = candidates.find((candidate) => candidate !== null && candidate !== undefined);
  const bytesNumber = Number(bytesValue);

  if (Number.isFinite(bytesNumber)) {
    return bytesNumber;
  }

  const mbValue = edge?.memory_mb ?? edge?.data?.memory_mb;
  const mbNumber = Number(mbValue);

  return Number.isFinite(mbNumber) ? mbNumber * 1024 * 1024 : null;
}

function isCppEdge(edge) {
  return edge?.executor_type === "remote_cpp" || Boolean(edge?.worker_id);
}

function getLatestEdge(edges) {
  return [...edges].sort(
    (left, right) =>
      Number(right.finishedAt || right.startedAt || 0) -
      Number(left.finishedAt || left.startedAt || 0)
  )[0];
}

function buildEdgeMetricsForStates(stateIds, executionState) {
  const states = new Set(stateIds.filter(Boolean));
  if (states.size === 0) {
    return {};
  }

  const relatedEdges = Object.values(executionState?.edges || {}).filter(
    (edge) => states.has(edge.from_state) || states.has(edge.to_state)
  );
  if (relatedEdges.length === 0) {
    return {};
  }

  const edgeDurationMs = relatedEdges.reduce((total, edge) => {
    const edgeTime = getEdgeTime(edge);
    return edgeTime === null ? total : total + edgeTime;
  }, 0);
  const runningEdges = relatedEdges.filter((edge) => edge.phase === "running").length;
  const cppEdges = relatedEdges.filter(isCppEdge);
  const latestCppEdge = getLatestEdge(cppEdges);
  const latestEdge = getLatestEdge(relatedEdges);
  const workerIds = [...new Set(cppEdges.map((edge) => edge.worker_id).filter(Boolean))];
  const operation =
    latestCppEdge?.executor_operation ||
    latestCppEdge?.operation ||
    latestCppEdge?.morph_func ||
    latestEdge?.executor_operation ||
    "";
  const loadPercent = getLoadPercent(latestCppEdge || latestEdge);
  const memoryBytes = getMemoryBytes(latestCppEdge || latestEdge);

  return {
    edgeDurationMs: edgeDurationMs > 0 ? edgeDurationMs : undefined,
    runningEdges,
    cppOperation: operation,
    workerIds,
    loadPercent,
    memoryBytes,
    hasCppExecution: cppEdges.length > 0,
  };
}

function mergeNodeAndEdgeExecution(nodeExecution, edgeMetrics) {
  if (!nodeExecution && Object.keys(edgeMetrics).length === 0) {
    return null;
  }

  return {
    ...(nodeExecution || {}),
    ...edgeMetrics,
  };
}

function getNodeExecution(node, executionState) {
  const stateIds = [
    node.data?.stateId,
    ...(node.data?.descendantStates || []),
  ].filter(Boolean);
  const direct = executionState?.nodes?.[node.data?.stateId];
  const edgeMetrics = buildEdgeMetricsForStates(stateIds, executionState);
  if (direct) {
    return mergeNodeAndEdgeExecution(direct, edgeMetrics);
  }

  const descendants = node.data?.descendantStates || [];
  const states = descendants
    .map((name) => executionState?.nodes?.[name])
    .filter(Boolean);
  const priority = ["failed", "active", "waiting", "completed"];
  const phase = priority.find((candidate) =>
    states.some((state) => state.phase === candidate)
  );
  if (!phase) {
    return mergeNodeAndEdgeExecution(null, edgeMetrics);
  }

  return mergeNodeAndEdgeExecution(
    {
      phase,
      visits: states.reduce((total, state) => total + (state.visits || 0), 0),
      durationMs: states.reduce((total, state) => total + (state.durationMs || 0), 0),
      totalDurationMs: states.reduce((total, state) => total + (state.totalDurationMs || 0), 0),
    },
    edgeMetrics
  );
}

function getSelectedStateSet(selectedElement) {
  if (!selectedElement || selectedElement.type !== "node") {
    return new Set();
  }

  return new Set(
    [
      selectedElement.stateId,
      selectedElement.label,
      ...(selectedElement.descendantStates || []),
    ].filter(Boolean)
  );
}

function isNodeSelected(node, selectedElement) {
  if (!selectedElement || selectedElement.type !== "node") {
    return false;
  }

  return (
    String(node.id) === String(selectedElement.id) ||
    node.data?.stateId === selectedElement.stateId
  );
}

function isNodeRelatedToEdge(node, selectedElement) {
  if (!selectedElement || selectedElement.type !== "edge") {
    return false;
  }

  return (
    node.data?.stateId === selectedElement.fromState ||
    node.data?.stateId === selectedElement.toState
  );
}

function edgeMatchesSelection(edge, selectedElement) {
  if (!selectedElement || selectedElement.type !== "edge") {
    return false;
  }

  if (
    String(edge.data?.edgeId || edge.id) ===
    String(selectedElement.edgeId || selectedElement.id)
  ) {
    return true;
  }

  return (
    edge.data?.fromState === selectedElement.fromState &&
    edge.data?.toState === selectedElement.toState &&
    Number(edge.data?.edgeOrder ?? 0) === Number(selectedElement.edgeOrder ?? 0)
  );
}

function edgeTouchesSelectedNode(edge, selectedStateSet) {
  if (selectedStateSet.size === 0) {
    return false;
  }

  return (
    selectedStateSet.has(edge.data?.fromState) ||
    selectedStateSet.has(edge.data?.toState)
  );
}

function getEdgeExecution(edge, executionState) {
  const edgeId = String(edge.data?.edgeId || "");
  const routeKey = [
    edge.data?.fromState || "",
    edge.data?.toState || "",
    edge.data?.edgeOrder ?? 0,
  ].join("->");
  const aliasKey = executionState?.edgeAliases?.[routeKey];

  return (
    executionState?.edges?.[edgeId] ||
    (aliasKey ? executionState?.edges?.[aliasKey] : null) ||
    executionState?.edges?.[routeKey] ||
    null
  );
}

export default function GraphView({
  graphId,
  orientation = "TB",
  showSubgraphs = true,
  executionState,
  selectedElement,
  onSelectionChange,
  executionControls,
  onGraphMeta,
  children,
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [modalState, setModalState] = useState({
    open: false,
    graph: null,
    breadcrumb: [],
  });

  const rootGraphRef = useRef(null);

  const handleOpenSubgraph = useCallback((subgraphId) => {
    const match = findSubgraphById(rootGraphRef.current, subgraphId);
    if (!match) {
      return;
    }

    setModalState({
      open: true,
      graph: match.graph,
      breadcrumb: match.breadcrumb,
    });
  }, []);

  const handleSelectNode = useCallback(
    (selection) => {
      onSelectionChange?.(selection);
    },
    [onSelectionChange]
  );

  const handleSelectEdge = useCallback(
    (selection) => {
      onSelectionChange?.(selection);
    },
    [onSelectionChange]
  );

  useEffect(() => {
    let isActive = true;

    loadGraphDeep(graphId)
      .then((rootGraph) => {
        if (!isActive) {
          return;
        }

        rootGraphRef.current = rootGraph;
        setModalState({ open: false, graph: null, breadcrumb: [] });
        onGraphMeta?.({
          isLoaded: true,
          name: rootGraph.name || "",
          executionInputSchema: rootGraph.execution_input_schema || {
            fields: [],
            prefilled_count: 0,
          },
          executionInputError: rootGraph.execution_input_error || "",
        });

        const nextState = buildFlowState(
          rootGraph,
          orientation,
          showSubgraphs,
          handleOpenSubgraph,
          handleSelectNode,
          handleSelectEdge
        );
        setNodes(nextState.nodes);
        setEdges(nextState.edges);
      })
      .catch((error) => {
        console.error("Failed to load graph", error);
      });

    return () => {
      isActive = false;
    };
  }, [
    graphId,
    orientation,
    showSubgraphs,
    handleOpenSubgraph,
    handleSelectNode,
    handleSelectEdge,
    onGraphMeta,
    setEdges,
    setNodes,
  ]);

  useEffect(() => {
    const selectedStateSet = getSelectedStateSet(selectedElement);

    setNodes((currentNodes) =>
      currentNodes.map((node) => {
        const selected = isNodeSelected(node, selectedElement);
        const relatedSelected = isNodeRelatedToEdge(node, selectedElement);

        return {
          ...node,
          selected,
          data: {
            ...node.data,
            execution: getNodeExecution(node, executionState),
            selected,
            relatedSelected,
          },
        };
      })
    );

    setEdges((currentEdges) =>
      currentEdges.map((edge) => {
        const selected = edgeMatchesSelection(edge, selectedElement);
        const relatedSelected = edgeTouchesSelectedNode(edge, selectedStateSet);

        return {
          ...edge,
          selected,
          data: {
            ...edge.data,
            execution: getEdgeExecution(edge, executionState),
            selected,
            relatedSelected,
          },
        };
      })
    );
  }, [executionState, selectedElement, setEdges, setNodes]);

  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
      }}
    >
      {children}
      {executionControls}
      <ExecutionStatusBar executionState={executionState} />

      <div className="gv-flow-canvas" style={{ width: "100%", flex: 1, minHeight: 420 }}>
        <ReactFlow
          className="gv-react-flow"
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={(_, node) => node.data?.onSelectNode?.()}
          onEdgeClick={(_, edge) => edge.data?.onSelectEdge?.()}
          onPaneClick={() => onSelectionChange?.(null)}
          nodeTypes={NODE_TYPES}
          edgeTypes={EDGE_TYPES}
          fitView
          fitViewOptions={{ padding: 0.22, minZoom: 0.2, maxZoom: 1.1 }}
          minZoom={0.12}
          maxZoom={1.8}
          nodesDraggable
          nodesConnectable={false}
          elementsSelectable
          panOnDrag
        >
          <MiniMap
            pannable
            zoomable
            style={{ width: 150, height: 96, opacity: 0.72 }}
            nodeStrokeWidth={3}
          />
          <Controls />
          <Background color="#d8e4ee" gap={18} size={0.7} />
        </ReactFlow>
      </div>

      <SubgraphModal
        open={modalState.open}
        graph={modalState.graph}
        breadcrumb={modalState.breadcrumb}
        executionState={executionState}
        onClose={() => setModalState({ open: false, graph: null, breadcrumb: [] })}
        onOpenSubgraph={handleOpenSubgraph}
      />
    </div>
  );
}
