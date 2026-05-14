import json
import logging
import threading

import redis
from django.conf import settings

from .execution_history import record_execution_event

logger = logging.getLogger(__name__)


class ExecutionEventService:
    def __init__(self):
        """
        Что делает: инфраструктурный сервис live-событий исполнения.
        Место: инфраструктурный сервис live-событий исполнения.
        Вход: нет; конфигурация Redis берется из Django settings.
        Выход: экземпляр с Redis-клиентом и локальным реестром подписок.
        """
        self.redis = redis.Redis(**settings.EVENT_REDIS_CONFIG)
        self._subscriptions = {}
        self._lock = threading.Lock()

    def publish(self, session_id, event):
        """
        Что делает: публикация событий из Celery-исполнителя в историю и live-канал.
        Место: публикация событий из Celery-исполнителя в историю и live-канал.
        Вход: session_id и словарь события исполнения графа.
        Выход: None; событие записывается в БД и публикуется в Redis pub/sub.
        """
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
        """
        Что делает: восстановление Redis-соединения сервиса событий.
        Место: восстановление Redis-соединения сервиса событий.
        Вход: нет.
        Выход: None; заменяет self.redis новым клиентом из settings.
        """
        self.redis = redis.Redis(**settings.EVENT_REDIS_CONFIG)

    def subscribe(self, session_id, callback):
        """
        Что делает: подключение SSE-потока к Redis-каналу конкретной сессии исполнения.
        Место: подключение SSE-потока к Redis-каналу конкретной сессии исполнения.
        Вход: session_id и callback, который принимает dict события.
        Выход: None; запускает daemon-поток прослушивания Redis.
        """
        try:
            self.redis.ping()
        except redis.ConnectionError:
            self._reconnect()

        pubsub = self.redis.pubsub()
        stop_event = threading.Event()
        pubsub.subscribe(f"execution:{session_id}")

        def listener():
            """
            Что делает: фоновый worker одной подписки Redis pub/sub.
            Место: фоновый worker одной подписки Redis pub/sub.
            Вход: замыкание с pubsub, stop_event, callback и session_id.
            Выход: None; читает Redis-сообщения и передает decoded события в callback.
            """
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
        """
        Что делает: завершение подписки SSE-клиента на события исполнения.
        Место: завершение подписки SSE-клиента на события исполнения.
        Вход: session_id и callback-обработчик, который был передан в subscribe.
        Выход: None; удаляет подписку из реестра и отписывает pubsub от Redis-канала.
        """
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
    """
    Что делает: singleton-доступ к сервису live-событий для views и Celery tasks.
    Место: singleton-доступ к сервису live-событий для views и Celery tasks.
    Вход: нет.
    Выход: общий экземпляр ExecutionEventService в текущем процессе.
    """
    global _event_service
    if _event_service is None:
        _event_service = ExecutionEventService()
    return _event_service
