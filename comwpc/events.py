import redis
from django.conf import settings
import json
import threading


class ExecutionEventService:
    def __init__(self):
        self.redis = redis.Redis(**settings.EVENT_REDIS_CONFIG)
        self.pubsub = self.redis.pubsub()
        self.local_listeners = []

    def publish(self, session_id, event):
        try:
            # Сжимаем данные для оптимизации
            compressed = json.dumps(event)
            self.redis.publish(f'execution:{session_id}', compressed)
        except redis.ConnectionError:
            # Логика повторного подключения
            self._reconnect()
            self.publish(session_id, event)

    def _reconnect(self):
        self.redis = redis.Redis(**settings.EVENT_REDIS_CONFIG)
        self.pubsub = self.redis.pubsub()

    def subscribe(self, session_id, callback):
        def listener(message):
            if message['type'] == 'message':
                try:
                    event = json.loads(message['data'])
                    callback(event)
                except json.JSONDecodeError:
                    print("Ошибка декодирования события")

        self.pubsub.subscribe(**{f'execution:{session_id}': listener})
        thread = threading.Thread(target=self.pubsub.run_in_thread, daemon=True)
        thread.start()


    def unsubscribe(self, session_id, handler):
        if session_id in self.local_listeners:
            if handler in self.local_listeners[session_id]:
                self.local_listeners[session_id].remove(handler)

            if not self.local_listeners[session_id]:
                del self.local_listeners[session_id]

    def _start_listening(self, session_id):
        def listener():
            pubsub = self.redis.pubsub()
            pubsub.subscribe(f'execution:{session_id}')

            for message in pubsub.listen():
                if message['type'] == 'message':
                    event = json.loads(message['data'])
                    if session_id in self.local_listeners:
                        for handler in self.local_listeners[session_id]:
                            handler(event)

        threading.Thread(target=listener, daemon=True).start()

_event_service = None

def get_event_service():
    global _event_service
    if _event_service is None:
        _event_service = ExecutionEventService()
    return _event_service