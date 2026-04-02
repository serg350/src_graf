import { buildApiUrl } from "./apiClient";

async function fetchJson(path) {
  const response = await fetch(buildApiUrl(path), {
    credentials: "include",
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  return response.json();
}

export async function loadGraph(id) {
  return fetchJson(`/api/graphs/${id}/`);
}

// рекурсивная глубокая загрузка
export async function loadGraphDeep(id) {
  return fetchJson(`/api/graphs/${id}/deep/`);
}

export async function loadAllGraphs() {
  return fetchJson("/api/graphs/");
}
