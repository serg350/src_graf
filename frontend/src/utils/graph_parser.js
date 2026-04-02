export function flattenGraphWithSubgraphs(graph, showSubgraphs) {
  const outNodes = [];
  const outEdges = [];

  graph.nodes.forEach((n) => {
    const isSub =
      showSubgraphs && n.subgraph !== null && n.subgraph !== undefined;

    if (isSub) {
      outNodes.push({
        id: n.id,
        type: "subgraph",
        data: {
          label: n.label,
          subgraphId: n.subgraph,
        },
        position: { x: 0, y: 0 },
      });
    } else {
      outNodes.push({
        id: n.id,
        data: { label: n.label },
        position: { x: 0, y: 0 },
      });
    }
  });

  graph.edges.forEach((e) => {
    outEdges.push({
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.label,
    });
  });

  return { nodes: outNodes, edges: outEdges };
}
