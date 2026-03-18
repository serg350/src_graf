import { buildApiUrl } from "./apiClient";

export async function loadGraph(id) {
  const r = await fetch(buildApiUrl(`/api/graphs/${id}/`), {
    credentials: "include",
  });
  return r.json();
}

// рекурсивная глубокая загрузка
export async function loadGraphDeep(id) {
  const g = await loadGraph(id);

  const subIds = g.nodes
    .filter((n) => n.subgraph)
    .map((n) => n.subgraph);

  const subgraphs = {};
  for (const sg of subIds) {
    subgraphs[sg] = await loadGraphDeep(sg);
  }

  return { ...g, subgraphs };
}

export async function loadAllGraphs() {
  const res = await fetch(buildApiUrl("/api/graphs/"), {
    credentials: "include",
  });
  return res.json();
}
