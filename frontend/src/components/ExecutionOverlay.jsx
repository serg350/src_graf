import { useExecutionStore } from "../utils/executionReducer";
import { useEffect } from "react";

export default function ExecutionOverlay({ setNodes, setEdges }) {
  const { activeNode, activeEdge } = useExecutionStore();

  useEffect(() => {
    setNodes((nodes) =>
      nodes.map((n) => ({
        ...n,
        data: {
          ...n.data,
          active: n.id === activeNode,
        },
      }))
    );
  }, [activeNode]);

  useEffect(() => {
    setEdges((edges) =>
      edges.map((e) => ({
        ...e,
        data: {
          ...e.data,
          active: e.id === activeEdge,
        },
      }))
    );
  }, [activeEdge]);

  return null;
}
