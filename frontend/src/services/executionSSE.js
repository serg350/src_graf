import { API_BASE_URL } from "./apiClient";

export function connectExecution(sessionId, onEvent) {
  const es = new EventSource(
    `${API_BASE_URL}/execution/events/${sessionId}/`,
    { withCredentials: true } // ← ВАЖНО
  );

  es.onmessage = (e) => {
    onEvent(JSON.parse(e.data));
  };

  es.onerror = (err) => {
    console.error("SSE error", err);
    es.close();
  };

  return () => es.close();
}