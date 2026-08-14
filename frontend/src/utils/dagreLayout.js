import dagre from "dagre";

const DEFAULT_NODE_WIDTH = 152;
const DEFAULT_NODE_HEIGHT = 70;
const SUBGRAPH_NODE_WIDTH = 174;
const SUBGRAPH_NODE_HEIGHT = 66;

function estimateEdgeLabelSize(edge) {
  const label = String(edge.comment || edge.label || "");
  return {
    width: Math.min(180, Math.max(48, label.length * 6.2)),
    height: label ? 28 : 10,
  };
}

export function applyDagreLayout(nodes, edges, direction = "TB") {
  const g = new dagre.graphlib.Graph({ multigraph: true });
  const isHorizontal = direction === "LR";

  g.setGraph({
    rankdir: direction,
    ranksep: isHorizontal ? 140 : 82,
    nodesep: isHorizontal ? 44 : 44,
    edgesep: 24,
    marginx: 42,
    marginy: 34,
    acyclicer: "greedy",
    ranker: "network-simplex",
  });
  g.setDefaultEdgeLabel(() => ({}));

  nodes.forEach((node) => {
    const width = node.type === "subgraph" ? SUBGRAPH_NODE_WIDTH : DEFAULT_NODE_WIDTH;
    const height = node.type === "subgraph" ? SUBGRAPH_NODE_HEIGHT : DEFAULT_NODE_HEIGHT;
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
