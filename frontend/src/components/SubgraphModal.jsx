import React, { useEffect, useState } from "react";
import ReactFlow, { Background, Controls, MiniMap } from "reactflow";
import "reactflow/dist/style.css";

import { applyDagreLayout } from "../utils/dagreLayout";

import SubgraphNode from "./SubgraphNode";
import CustomEdge from "./CustomEdge";

export default function SubgraphModal({ open, onClose, graph, breadcrumb = [] }) {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const nodeTypes = {
    subgraph: SubgraphNode,
  };

  const edgeTypes = {
    default: CustomEdge,
  };

  useEffect(() => {
    if (!graph) {
      setNodes([]);
      setEdges([]);
      return;
    }

    //
    // 1) Raw nodes
    //
    const rawNodes = (graph.nodes || []).map((n) => ({
      id: String(n.id),
      draggable: true,
      data: { label: n.label },
      type: n.subgraph ? "subgraph" : undefined,
      position: { x: 0, y: 0 },
    }));

    //
    // 2) Raw edges
    //
    const rawEdges = (graph.edges || []).map((e) => ({
      id: String(e.id),
      source: String(e.source),
      target: String(e.target),
      label: e.label || "",
      markerEnd: "arrowclosed",
    }));

    //
    // 3) Dagre layout
    //
    const { nodes: layoutedNodes, edges: layoutedEdges } = applyDagreLayout(
      rawNodes,
      rawEdges,
      "LR"
    );

    //
    // 4) Normalize nodes
    //
    const finalNodes = layoutedNodes.map((n) => ({
      ...n,
      id: String(n.id),
      draggable: true,
    }));

    //
    // 5) Sanitize edges
    //
    const normalizedEdges = layoutedEdges.map((e) => ({
      ...e,
      source: String(e.source),
      target: String(e.target),
    }));

    const sanitizedEdges = normalizedEdges.map((e) => {
      const out = { ...e };
      if (out.sourceHandle == null) delete out.sourceHandle;
      if (out.targetHandle == null) delete out.targetHandle;
      if (!out.markerEnd) out.markerEnd = "arrowclosed";
      return out;
    });

    //
    // 6) Apply
    //
    setNodes(finalNodes);
    setEdges(sanitizedEdges);
  }, [graph]);

  if (!open) return null;

  //const nodeTypes = { subgraph: SubgraphNode };
  //const edgeTypes = { default: CustomEdge };

  return (
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
        onClick={(e) => e.stopPropagation()}
      >
        {/* HEADER */}
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
              ? breadcrumb.map((b, i) => (
                  <span key={i}>
                    {b.label}
                    {i < breadcrumb.length - 1 && " > "}
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

        {/* GRAPH */}
        <div style={{ flex: 1, width: "100%", minHeight: 0 }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            fitView

            nodesDraggable={true}
            nodesConnectable={false}
            elementsSelectable={true}
            panOnDrag={true}

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
}
