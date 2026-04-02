import json
import logging
import threading

import redis
from django.conf import settings

from .execution_history import record_execution_event

logger = logging.getLogger(__name__)


class ExecutionEventService:
    def __init__(self):
        self.redis = redis.Redis(**settings.EVENT_REDIS_CONFIG)
        self._subscriptions = {}
        self._lock = threading.Lock()

    def publish(self, session_id, event):
        try:
            record_execution_event(session_id, event)
        except Exception:  # pragma: no cover - keep the live stream working even if persistence fails
            logger.exception("Failed to persist execution event for session %s", session_id)

        try:
            compressed = json.dumps(event, ensure_ascii=False)
            self.redis.publish(f"execution:{session_id}", compressed)
        except redis.ConnectionError:
            self._reconnect()
            self.publish(session_id, event)

    def _reconnect(self):
        self.redis = redis.Redis(**settings.EVENT_REDIS_CONFIG)

    def subscribe(self, session_id, callback):
        try:
            self.redis.ping()
        except redis.ConnectionError:
            self._reconnect()

        pubsub = self.redis.pubsub()
        stop_event = threading.Event()
        pubsub.subscribe(f"execution:{session_id}")

        def listener():
            try:
                for message in pubsub.listen():
                    if stop_event.is_set():
                        break
                    if message["type"] != "message":
                        continue
                    try:
                        event = json.loads(message["data"])
                    except json.JSONDecodeError:
                        logger.warning("Failed to decode execution event for session %s", session_id)
                        continue
                    callback(event)
            finally:
                try:
                    pubsub.close()
                except Exception:
                    logger.debug("Redis pubsub close failed for session %s", session_id, exc_info=True)

        thread = threading.Thread(target=listener, daemon=True)
        with self._lock:
            self._subscriptions.setdefault(session_id, []).append(
                {
                    "callback": callback,
                    "pubsub": pubsub,
                    "stop_event": stop_event,
                    "thread": thread,
                }
            )
        thread.start()

    def unsubscribe(self, session_id, handler):
        with self._lock:
            subscriptions = self._subscriptions.get(session_id, [])
            remaining = []
            target = None

            for subscription in subscriptions:
                if subscription["callback"] is handler and target is None:
                    target = subscription
                    continue
                remaining.append(subscription)

            if remaining:
                self._subscriptions[session_id] = remaining
            else:
                self._subscriptions.pop(session_id, None)

        if not target:
            return

        target["stop_event"].set()
        try:
            target["pubsub"].unsubscribe(f"execution:{session_id}")
        except Exception:
            logger.debug("Redis unsubscribe failed for session %s", session_id, exc_info=True)


_event_service = None


def get_event_service():
    global _event_service
    if _event_service is None:
        _event_service = ExecutionEventService()
    return _event_service
