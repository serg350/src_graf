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
import SubgraphNode from "./SubgraphNode";

export default function SubgraphModal({
  open,
  onClose,
  onOpenSubgraph,
  graph,
  breadcrumb = [],
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
        label: node.label,
        subgraphId: node.subgraph,
        onOpenSubgraph,
      },
      type: node.subgraph ? "subgraph" : undefined,
      position: { x: 0, y: 0 },
    }));

    const rawEdges = (graph.edges || []).map((edge) => ({
      id: String(edge.id),
      source: String(edge.source),
      target: String(edge.target),
      label: edge.label || "",
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
    setEdges(
      layoutedEdges.map((edge) => ({
        ...edge,
        id: String(edge.id),
        source: String(edge.source),
        target: String(edge.target),
        markerEnd: edge.markerEnd || "arrowclosed",
      }))
    );
  }, [graph, onOpenSubgraph]);

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
            nodeTypes={{ subgraph: SubgraphNode }}
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
