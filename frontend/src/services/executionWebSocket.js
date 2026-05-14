import { API_BASE_URL } from "./apiClient";

function buildWebSocketUrl(path) {
  const baseUrl = API_BASE_URL || window.location.origin;
  const url = new URL(path, baseUrl || window.location.origin);

  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

export function connectExecutionWebSocket(sessionId, onEvent) {
  const ws = new WebSocket(
    buildWebSocketUrl(`/ws/execution/${sessionId}/`)
  );

  ws.onopen = () => {
    ws.send(JSON.stringify({ type: "ping" }));
  };

  ws.onmessage = (message) => {
    const event = JSON.parse(message.data);

    if (event.type === "connected" || event.type === "pong") {
      return;
    }

    onEvent(event);
  };

  ws.onerror = (error) => {
    console.error("WebSocket error", error);
  };

  ws.onclose = (event) => {
    console.log("WebSocket closed", event.code, event.reason);
  };

  return () => ws.close();
}
