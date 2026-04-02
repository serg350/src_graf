import React, { useEffect, useRef, useState } from "react";
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
import SubgraphModal from "./SubgraphModal";
import SubgraphNode from "./SubgraphNode";

const NODE_TYPES = { subgraph: SubgraphNode };
const EDGE_TYPES = { default: CustomEdge };

function getBaseNodeStyle(nodeType) {
  if (nodeType === "subgraph") {
    return {};
  }

  return {
    border: "1px solid #94a3b8",
    background: "#ffffff",
    borderRadius: 10,
    transition: "all 0.2s ease",
  };
}

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
    const baseStyle = getBaseNodeStyle(node.type);

    return {
      ...node,
      id: String(node.id),
      draggable: true,
      type: node.type === "subgraph" ? "subgraph" : undefined,
      data: {
        ...node.data,
        stateId: node.data?.label,
        baseStyle,
        onOpenSubgraph: handleOpenSubgraph,
      },
      style: baseStyle,
    };
  });

  const edges = dagre.edges.map((edge) => {
    const from = nodes.find((node) => node.id === String(edge.source))?.data.stateId;
    const to = nodes.find((node) => node.id === String(edge.target))?.data.stateId;

    return {
      id: String(edge.id),
      source: String(edge.source),
      target: String(edge.target),
      type: "default",
      label: edge.label,
      data: {
        fromState: from,
        toState: to,
        active: false,
      },
    };
  });

  return { nodes, edges };
}

export default function GraphView({
  graphId,
  orientation = "TB",
  showSubgraphs = true,
  executionEvent,
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

  const prevStateRef = useRef(null);
  const rootGraphRef = useRef(null);

  const handleOpenSubgraph = (subgraphId) => {
    const match = findSubgraphById(rootGraphRef.current, subgraphId);
    if (!match) {
      return;
    }

    setModalState({
      open: true,
      graph: match.graph,
      breadcrumb: match.breadcrumb,
    });
  };

  useEffect(() => {
    let isActive = true;

    loadGraphDeep(graphId)
      .then((rootGraph) => {
        if (!isActive) {
          return;
        }

        rootGraphRef.current = rootGraph;
        prevStateRef.current = null;
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
  }, [graphId, orientation, showSubgraphs]);

  useEffect(() => {
    if (!executionEvent || !executionEvent.event) {
      return;
    }

    const { event, state } = executionEvent;

    if (event !== "state_enter") {
      return;
    }

    setNodes((currentNodes) =>
      currentNodes.map((node) => {
        const baseStyle = node.data?.baseStyle || {};
        if (node.data?.stateId === state) {
          return {
            ...node,
            style: {
              ...baseStyle,
              border: "3px solid #22c55e",
              background: "#dcfce7",
            },
          };
        }

        return {
          ...node,
          style: { ...baseStyle },
        };
      })
    );

    const previousState = prevStateRef.current;
    setEdges((currentEdges) =>
      currentEdges.map((edge) => ({
        ...edge,
        data: {
          ...edge.data,
          active:
            Boolean(previousState) &&
            edge.data?.fromState === previousState &&
            edge.data?.toState === state,
        },
      }))
    );

    prevStateRef.current = state;
  }, [executionEvent]);

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
        onClose={() => setModalState({ open: false, graph: null, breadcrumb: [] })}
        onOpenSubgraph={handleOpenSubgraph}
      />
    </div>
  );
}
