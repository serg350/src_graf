import dagre from "dagre";

export function applyDagreLayout(nodes, edges, direction = "LR") {
  const g = new dagre.graphlib.Graph();

  g.setGraph({ rankdir: direction }); // LR, TB
  g.setDefaultEdgeLabel(() => ({}));

  nodes.forEach((node) => {
    g.setNode(node.id, { width: 160, height: 50 });
  });

  edges.forEach((edge) => {
    g.setEdge(edge.source, edge.target);
  });

  dagre.layout(g);

  const isHorizontal = direction === "LR";

  const layoutedNodes = nodes.map((node) => {
    const pos = g.node(node.id);
    return {
      ...node,
      position: { x: pos.x, y: pos.y },
      targetPosition: isHorizontal ? "left" : "top",
      sourcePosition: isHorizontal ? "right" : "bottom",
    };
  });

  return { nodes: layoutedNodes, edges };
}
