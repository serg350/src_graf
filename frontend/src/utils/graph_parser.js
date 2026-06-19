function collectStateNames(graph) {
  if (!graph) {
    return [];
  }

  const names = (graph.nodes || []).map((node) => node.label);
  for (const subgraph of Object.values(graph.subgraphs || {})) {
    names.push(...collectStateNames(subgraph));
  }
  return names;
}

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
          ...n,
          label: n.label,
          comment: n.comment || "",
          subgraphId: n.subgraph,
          descendantStates: collectStateNames(
            graph.subgraphs?.[String(n.subgraph)]
          ),
        },
        position: { x: 0, y: 0 },
      });
    } else {
      outNodes.push({
        id: n.id,
        data: {
          ...n,
          label: n.label,
          comment: n.comment || "",
        },
        position: { x: 0, y: 0 },
      });
    }
  });

  graph.edges.forEach((e) => {
    outEdges.push({
      ...e,
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.label,
      comment: e.comment || e.label || "",
    });
  });

  return { nodes: outNodes, edges: outEdges };
}
