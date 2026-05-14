import { buildApiUrl } from "./apiClient";

export function connectExecutionSSE(sessionId, onEvent) {
  const es = new EventSource(
    buildApiUrl(`/execution/events/${sessionId}/`),
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
