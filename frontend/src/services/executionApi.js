import { buildApiUrl } from "./apiClient";
import { getCSRFToken } from "./csrf";

export function startExecution(graphId, data = {}) {
  return fetch(buildApiUrl(`/graph/${graphId}/start/`), {
    method: "POST",
    credentials: "include", // ← ВАЖНО
    headers: {
      "Content-Type": "application/json",
      "X-CSRFToken": getCSRFToken(), // ← ВАЖНО
    },
    body: JSON.stringify({ data }),
  }).then(async (r) => {
    const payload = await r.json().catch(() => ({}));
    if (!r.ok) {
      throw new Error(payload.error || `HTTP ${r.status}`);
    }
    return payload;
  });
}
