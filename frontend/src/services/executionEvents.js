//import { connectExecutionSSE } from "./executionSSE";
import { connectExecutionWebSocket } from "./executionWebSocket";

const DEFAULT_PLAYBACK_DELAY_MS = 350;

function getConfiguredPlaybackDelayMs() {
  const rawDelay = import.meta.env.VITE_EXECUTION_EVENT_DELAY_MS;

  if (rawDelay === undefined || rawDelay === null || rawDelay === "") {
    return DEFAULT_PLAYBACK_DELAY_MS;
  }

  const delay = Number(rawDelay);
  return Number.isFinite(delay) && delay >= 0 ? delay : DEFAULT_PLAYBACK_DELAY_MS;
}

function createPlaybackQueue(onEvent, playbackDelayMs) {
  const queue = [];
  let timeoutId = null;
  let closed = false;

  const schedule = (delay) => {
    if (closed || timeoutId !== null) {
      return;
    }

    timeoutId = window.setTimeout(flushNext, delay);
  };

  const flushNext = () => {
    timeoutId = null;

    if (closed) {
      return;
    }

    const event = queue.shift();
    if (!event) {
      return;
    }

    onEvent(event);

    if (queue.length > 0) {
      schedule(playbackDelayMs);
    }
  };

  return {
    push(event) {
      queue.push(event);
      schedule(queue.length === 1 ? 0 : playbackDelayMs);
    },
    cancel() {
      closed = true;
      queue.length = 0;

      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
        timeoutId = null;
      }
    },
  };
}

export function connectExecution(sessionId, onEvent, options = {}) {
  const playbackDelayMs =
    options.playbackDelayMs ?? getConfiguredPlaybackDelayMs();
  const playback = createPlaybackQueue(onEvent, playbackDelayMs);
  //const disconnectTransport = connectExecutionSSE(sessionId, playback.push);
  const disconnectTransport = connectExecutionWebSocket(sessionId, playback.push);

  return () => {
    disconnectTransport();
    playback.cancel();
  };
}
