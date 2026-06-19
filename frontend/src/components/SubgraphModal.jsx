import React, { useEffect } from "react";
import { createPortal } from "react-dom";
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  useEdgesState,
  useNodesState,
} from "reactflow";
import "reactflow/dist/style.css";

import { applyDagreLayout } from "../utils/dagreLayout";

import CustomEdge from "./CustomEdge";
import GraphNode from "./GraphNode";
import SubgraphNode from "./SubgraphNode";

export default function SubgraphModal({
  open,
  onClose,
  onOpenSubgraph,
  graph,
  breadcrumb = [],
  executionState,
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  useEffect(() => {
    if (!graph) {
      setNodes([]);
      setEdges([]);
      return;
    }

    const rawNodes = (graph.nodes || []).map((node) => ({
      id: String(node.id),
      draggable: true,
      data: {
        ...node,
        label: node.label,
        comment: node.comment || "",
        stateId: node.label,
        subgraphId: node.subgraph,
        onOpenSubgraph,
      },
      type: node.subgraph ? "subgraph" : "graph",
      position: { x: 0, y: 0 },
    }));

    const rawEdges = (graph.edges || []).map((edge) => ({
      id: String(edge.id),
      source: String(edge.source),
      target: String(edge.target),
      label: edge.label || "",
      data: {
        ...edge,
        edgeId: String(edge.id),
        edgeOrder: edge.order ?? 0,
        fromState: (graph.nodes || []).find(
          (node) => String(node.id) === String(edge.source)
        )?.label,
        toState: (graph.nodes || []).find(
          (node) => String(node.id) === String(edge.target)
        )?.label,
        comment: edge.comment || edge.label || "",
        executorType: edge.executor_type || "",
        executorOperation: edge.executor_operation || "",
        orientation: "LR",
      },
      markerEnd: "arrowclosed",
    }));

    const { nodes: layoutedNodes, edges: layoutedEdges } = applyDagreLayout(
      rawNodes,
      rawEdges,
      "LR"
    );

    setNodes(
      layoutedNodes.map((node) => ({
        ...node,
        id: String(node.id),
        draggable: true,
      }))
    );
    let backLane = 0;
    setEdges(
      layoutedEdges.map((edge) => {
        const sourceNode = layoutedNodes.find(
          (node) => String(node.id) === String(edge.source)
        );
        const targetNode = layoutedNodes.find(
          (node) => String(node.id) === String(edge.target)
        );
        const isBackEdge = sourceNode?.position.x >= targetNode?.position.x;
        return {
          ...edge,
          id: String(edge.id),
          source: String(edge.source),
          target: String(edge.target),
          type: "default",
          data: {
            ...edge.data,
            isBackEdge,
            routingLane: isBackEdge ? backLane++ : 0,
          },
        };
      })
    );
  }, [graph, onOpenSubgraph]);

  useEffect(() => {
    setNodes((currentNodes) =>
      currentNodes.map((node) => ({
        ...node,
        data: {
          ...node.data,
          execution: executionState?.nodes?.[node.data?.stateId] || null,
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

  if (!open) {
    return null;
  }

  const content = (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.45)",
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        zIndex: 2000,
      }}
      onClick={onClose}
    >
      <div
        style={{
          width: "82vw",
          height: "82vh",
          background: "#fff",
          borderRadius: 12,
          padding: 12,
          position: "relative",
          display: "flex",
          flexDirection: "column",
        }}
        onClick={(event) => event.stopPropagation()}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: 8,
          }}
        >
          <div style={{ fontSize: 14, color: "#334155" }}>
            {breadcrumb.length > 0
              ? breadcrumb.map((item, index) => (
                  <span key={`${item.id}-${index}`}>
                    {item.label}
                    {index < breadcrumb.length - 1 && " > "}
                  </span>
                ))
              : graph?.name ?? "Подграф"}
          </div>

          <button
            onClick={onClose}
            style={{
              border: "none",
              background: "#efefef",
              padding: "6px 10px",
              borderRadius: 8,
              cursor: "pointer",
            }}
          >
            Закрыть
          </button>
        </div>

        <div style={{ flex: 1, width: "100%", minHeight: 0 }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={{ graph: GraphNode, subgraph: SubgraphNode }}
            edgeTypes={{ default: CustomEdge }}
            fitView
            nodesDraggable
            nodesConnectable={false}
            elementsSelectable
            panOnDrag
            minZoom={0.1}
            style={{ width: "100%", height: "100%", background: "#fff" }}
          >
            <MiniMap />
            <Controls />
            <Background gap={16} size={1} />
          </ReactFlow>
        </div>
      </div>
    </div>
  );

  return createPortal(content, document.body);
}
