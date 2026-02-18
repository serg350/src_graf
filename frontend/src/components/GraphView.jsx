import React, { useEffect, useRef, useState } from "react";
import ReactFlow, { MiniMap, Controls, Background } from "reactflow";
import "reactflow/dist/style.css";

import { loadGraphDeep } from "../services/api";
import { flattenGraphWithSubgraphs } from "../utils/graph_parser";
import { applyDagreLayout } from "../utils/dagreLayout";

import SubgraphModal from "./SubgraphModal";
import SubgraphNode from "./SubgraphNode";
import CustomEdge from "./CustomEdge";

/* ───────────────────────────────────────────── */
const NODE_TYPES = { subgraph: SubgraphNode };
const EDGE_TYPES = { default: CustomEdge };
/* ───────────────────────────────────────────── */

export default function GraphView({
  graphId,
  orientation = "TB",
  showSubgraphs = true,
  executionEvent,
  onHistoryAdd,
  children,
}) {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);

  const prevStateRef = useRef(null);

  /* ───────────── LOAD GRAPH ───────────── */
  useEffect(() => {
    loadGraphDeep(graphId).then((rootGraph) => {
      const flat = flattenGraphWithSubgraphs(rootGraph, showSubgraphs);
      const dagre = applyDagreLayout(flat.nodes, flat.edges, orientation);

      const ns = dagre.nodes.map((n) => ({
        ...n,
        id: String(n.id),
        type: n.type === "subgraph" ? "subgraph" : undefined,
        data: {
          ...n.data,
          stateId: n.data?.label,
        },
        style: {
          border: "1px solid #999",
          background: "#fff",
          transition: "all 0.2s ease",
        },
      }));

      const es = dagre.edges.map((e) => {
        const from = ns.find((n) => n.id === String(e.source))?.data.stateId;
        const to = ns.find((n) => n.id === String(e.target))?.data.stateId;

        return {
          id: String(e.id),
          source: String(e.source),
          target: String(e.target),
          type: "default",
          data: {
            fromState: from,
            toState: to,
            active: false,
          },
        };
      });

      setNodes(ns);
      setEdges(es);
      prevStateRef.current = null;
    });
  }, [graphId, orientation, showSubgraphs]);

  /* ───────────── SSE → HIGHLIGHT ───────────── */
  useEffect(() => {
    if (!executionEvent) return;

    const { event, state, timestamp } = executionEvent;
    console.log("[HISTORY]", {
      event,
      state
    });
    // 🧾 история — стабильно
    //onHistoryAdd?.(
    //  `[${new Date(timestamp * 1000).toLocaleTimeString()}] ${event}: ${state}`
    //);
    //onHistoryAdd?.({ type: state, event, timestamp: executionEvent.timestamp, });
    onHistoryAdd?.({
      title: state,          // ← будет жирным
      payload: executionEvent.data ?? {},
      event,
      timestamp,
    });

    if (event !== "state_enter") return;

    // 🟢 узлы
    setNodes((nds) =>
      nds.map((n) =>
        n.data.stateId === state
          ? {
              ...n,
              style: {
                ...n.style,
                border: "3px solid #22c55e",
                background: "#dcfce7",
              },
            }
          : {
              ...n,
              style: {
                ...n.style,
                border: "1px solid #999",
                background: "#fff",
              },
            }
      )
    );

    // 🟠 рёбра
    const prev = prevStateRef.current;

    setEdges((eds) =>
      eds.map((e) => ({
        ...e,
        data: {
          ...e.data,
          active:
            prev &&
            e.data.fromState === prev &&
            e.data.toState === state,
        },
      }))
    );

    prevStateRef.current = state;
  }, [executionEvent]);

  /* ───────────────────────────────────────────── */

  return (
    <div style={{ width: "100%", height: "100%" }}>
      {children}

      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={NODE_TYPES}
        edgeTypes={EDGE_TYPES}
        fitView
      >
        <MiniMap />
        <Controls />
        <Background />
      </ReactFlow>

      <SubgraphModal />
    </div>
  );
}
