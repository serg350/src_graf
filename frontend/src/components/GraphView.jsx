import React, { useEffect, useState } from "react";
import ReactFlow, { MiniMap, Controls, Background } from "reactflow";
import "reactflow/dist/style.css";

import { loadGraphDeep } from "../services/api";
import { flattenGraphWithSubgraphs } from "../utils/graph_parser";
import { applyDagreLayout } from "../utils/dagreLayout";

import SubgraphModal from "./SubgraphModal";
import SubgraphNode from "./SubgraphNode";
import CustomEdge from "./CustomEdge";

export default function GraphView({
  graphId,
  orientation,
  showSubgraphs,
  onHistoryAdd,
}) {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [modalGraph, setModalGraph] = useState(null);
  const [breadcrumb, setBreadcrumb] = useState([]);
  const nodeTypes = {
      subgraph: SubgraphNode,
  };

  const edgeTypes = {
      default: CustomEdge,
  };

  const openSubgraph = (id) => {
    loadGraphDeep(id).then((g) => {
      setBreadcrumb((prev) => [...prev, { id, label: g.name }]);
      setModalGraph({ ...g, parentId: graphId });
    });
  };

  const closeSubgraph = () => {
    setBreadcrumb((prev) => prev.slice(0, -1));
    setModalGraph(null);
  };

  useEffect(() => {
    loadGraphDeep(graphId).then((rootGraph) => {
      const flat = flattenGraphWithSubgraphs(rootGraph, showSubgraphs);

      const dagre = applyDagreLayout(flat.nodes, flat.edges, orientation);

      const layoutedNodes = dagre.nodes.map((n) => ({
        ...n,
        id: String(n.id),
        draggable: true,
      }));

      const normalizedEdges = dagre.edges.map((e) => ({
        id: String(e.id),
        source: String(e.source),
        target: String(e.target),
        label: e.label || "",
      }));

      const sanitizedEdges = normalizedEdges.map((e) => ({
        ...e,
        markerEnd: "arrowclosed",
      }));

      const finalNodes = layoutedNodes.map((n) => ({
        ...n,
        type: n.type === "subgraph" ? "subgraph" : n.type,
        data: {
          ...n.data,
          onOpenSubgraph: openSubgraph,
        },
      }));

      setNodes(finalNodes);
      setEdges(sanitizedEdges);
    });
  }, [graphId, orientation, showSubgraphs]);

    return (
      <div style={{ width: "100%", height: "100%" }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          fitView
          minZoom={0.1}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
        >
          <MiniMap />
          <Controls />
          <Background />
        </ReactFlow>

        <SubgraphModal
          open={!!modalGraph}
          onClose={closeSubgraph}
          graph={modalGraph}
          breadcrumb={breadcrumb}
        />
      </div>
    );
}
