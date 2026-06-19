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

function buildFlowState(rootGraph, orientation, showSubgraphs, handleOpenSubgraph) {
  const flat = flattenGraphWithSubgraphs(rootGraph, showSubgraphs);
  const dagre = applyDagreLayout(flat.nodes, flat.edges, orientation);

  const nodes = dagre.nodes.map((node) => {
    return {
      ...node,
      id: String(node.id),
      draggable: true,
      type: node.type === "subgraph" ? "subgraph" : "graph",
      data: {
        ...node.data,
        stateId: node.data?.label,
        onOpenSubgraph: handleOpenSubgraph,
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
      },
    };
  });

  return { nodes, edges };
}

function getNodeExecution(node, executionState) {
  const direct = executionState?.nodes?.[node.data?.stateId];
  if (direct) {
    return direct;
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
    return null;
  }

  return {
    phase,
    visits: states.reduce((total, state) => total + (state.visits || 0), 0),
  };
}

export default function GraphView({
  graphId,
  orientation = "TB",
  showSubgraphs = true,
  executionState,
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
          handleOpenSubgraph
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
  }, [graphId, orientation, showSubgraphs, handleOpenSubgraph, onGraphMeta, setEdges, setNodes]);

  useEffect(() => {
    setNodes((currentNodes) =>
      currentNodes.map((node) => ({
        ...node,
        data: {
          ...node.data,
          execution: getNodeExecution(node, executionState),
        },
      }))
    );

    setEdges((currentEdges) =>
      currentEdges.map((edge) => ({
        ...edge,
        data: {
          ...edge.data,
          execution:
            executionState?.edges?.[String(edge.data?.edgeId)] ||
            executionState?.edges?.[
              [
                edge.data?.fromState || "",
                edge.data?.toState || "",
                edge.data?.edgeOrder ?? 0,
              ].join("->")
            ] ||
            null,
        },
      }))
    );
  }, [executionState, setEdges, setNodes]);

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

      <div style={{ width: "100%", flex: 1, minHeight: 420 }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          nodeTypes={NODE_TYPES}
          edgeTypes={EDGE_TYPES}
          fitView
          nodesDraggable
          nodesConnectable={false}
          elementsSelectable
          panOnDrag
        >
          <MiniMap />
          <Controls />
          <Background />
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
