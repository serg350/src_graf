import dagre from "dagre";

const DEFAULT_NODE_WIDTH = 196;
const DEFAULT_NODE_HEIGHT = 72;

function estimateEdgeLabelSize(edge) {
  const label = String(edge.comment || edge.label || "");
  return {
    width: Math.min(180, Math.max(48, label.length * 6.2)),
    height: label ? 28 : 10,
  };
}

export function applyDagreLayout(nodes, edges, direction = "LR") {
  const g = new dagre.graphlib.Graph({ multigraph: true });
  const isHorizontal = direction === "LR";

  g.setGraph({
    rankdir: direction,
    ranksep: isHorizontal ? 175 : 120,
    nodesep: isHorizontal ? 58 : 82,
    edgesep: 34,
    marginx: 36,
    marginy: 36,
    acyclicer: "greedy",
    ranker: "network-simplex",
  });
  g.setDefaultEdgeLabel(() => ({}));

  nodes.forEach((node) => {
    const width = node.type === "subgraph" ? 210 : DEFAULT_NODE_WIDTH;
    const height = DEFAULT_NODE_HEIGHT;
    g.setNode(node.id, { width, height });
  });

  edges.forEach((edge) => {
    g.setEdge(
      edge.source,
      edge.target,
      estimateEdgeLabelSize(edge),
      String(edge.id)
    );
  });

  dagre.layout(g);

  const layoutedNodes = nodes.map((node) => {
    const pos = g.node(node.id);
    const width = pos.width || DEFAULT_NODE_WIDTH;
    const height = pos.height || DEFAULT_NODE_HEIGHT;
    return {
      ...node,
      width,
      height,
      position: {
        x: pos.x - width / 2,
        y: pos.y - height / 2,
      },
      targetPosition: isHorizontal ? "left" : "top",
      sourcePosition: isHorizontal ? "right" : "bottom",
    };
  });

  const layoutedEdges = edges.map((edge) => {
    const layout = g.edge({
      v: edge.source,
      w: edge.target,
      name: String(edge.id),
    });
    return {
      ...edge,
      data: {
        ...edge.data,
        layoutLabelX: layout?.x,
        layoutLabelY: layout?.y,
      },
    };
  });

  return { nodes: layoutedNodes, edges: layoutedEdges };
}
