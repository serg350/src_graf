import { buildApiUrl } from "./apiClient";

export async function loadGraphExecutionHistory(graphId, limit = 12) {
  const response = await fetch(
    buildApiUrl(`/api/graphs/${graphId}/executions/?limit=${limit}`),
    {
      credentials: "include",
    }
  );

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  return response.json();
}
