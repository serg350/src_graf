import { API_BASE_URL } from "./apiClient";

function buildWebSocketUrl(path) {
  const baseUrl = API_BASE_URL || window.location.origin;
  const url = new URL(path, baseUrl || window.location.origin);

  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

export function connectExecutionWebSocket(sessionId, handlers = {}) {
  let socket = null;
  let reconnectTimer = null;
  let closed = false;
  let connectedOnce = false;
  let lastSequence = Number(handlers.afterSequence) || 0;
  let resolveReady;
  let rejectReady;

  const ready = new Promise((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });

  const connect = () => {
    const url = new URL(
      buildWebSocketUrl(`/ws/execution/${sessionId}/`)
    );
    url.searchParams.set("after", String(lastSequence));
    socket = new WebSocket(url.toString());

    socket.onmessage = (message) => {
      const event = JSON.parse(message.data);

      if (event.type === "connected") {
        if (!connectedOnce) {
          connectedOnce = true;
          resolveReady();
        }
        handlers.onConnected?.();
        return;
      }

      if (event.type === "pong") {
        return;
      }

      const sequence = Number(event.sequence) || 0;
      if (sequence > 0) {
        lastSequence = Math.max(lastSequence, sequence);
      }
      handlers.onEvent?.(event);
    };

    socket.onerror = (error) => {
      console.error("WebSocket error", error);
      handlers.onError?.(error);
    };

    socket.onclose = () => {
      socket = null;
      if (closed) {
        return;
      }

      if (!connectedOnce) {
        rejectReady(new Error("Не удалось подключиться к потоку событий выполнения"));
        return;
      }

      reconnectTimer = window.setTimeout(connect, 500);
    };
  };

  connect();

  return {
    ready,
    disconnect() {
      closed = true;
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
      }
      socket?.close();
      socket = null;
    },
  };
}
