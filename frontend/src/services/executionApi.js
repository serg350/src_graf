import { API_BASE_URL } from "./apiClient";
import { getCSRFToken } from "./csrf";

export function startExecution(graphId, data = {}) {
  return fetch(`${API_BASE_URL}/graph/${graphId}/start/`, {
    method: "POST",
    credentials: "include", // ← ВАЖНО
    headers: {
      "Content-Type": "application/json",
      "X-CSRFToken": getCSRFToken(), // ← ВАЖНО
    },
    body: JSON.stringify(data),
  }).then((r) => {
    if (!r.ok) {
      throw new Error(`HTTP ${r.status}`);
    }
    return r.json();
  });
}
