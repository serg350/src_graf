import json

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer

from .models import GraphExecutionEvent


class ExecutionConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer для live-событий исполнения графа.
    Один client подключается к одной session_id:
    /ws/execution/<session_id>/
    """
    async def connect(self):
        self.session_id = self.scope["url_route"]["kwargs"]["session_id"]
        self.group_name = f"execution_{self.session_id}"
        self.after_sequence = self._get_after_sequence()

        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name
        )

        await self.accept()

        await self.send_json({
            "type": "connected",
            "session_id": self.session_id
        })

        for event in await self._load_events_after(self.after_sequence):
            await self.send_json(event)

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name
        )

    async def receive(self, text_data=None, bytes_data=None):
        if not text_data:
            return

        try:
            message = json.loads(text_data)
        except json.JSONDecodeError:
            await self.send_json({
                "type": "error",
                "message": "Invalid JSON",
            })
            return

        if message.get("type") == "ping":
            await self.send_json({
                "type": "pong",
                "session_id": self.session_id,
            })

    async def execution_event(self, event):
        await self.send_json(event["payload"])

    async def send_json(self, payload):
        await self.send(text_data=json.dumps(payload, ensure_ascii=False))

    def _get_after_sequence(self):
        query_string = self.scope.get("query_string", b"").decode("utf-8")
        for part in query_string.split("&"):
            key, _, value = part.partition("=")
            if key == "after":
                try:
                    return max(0, int(value))
                except ValueError:
                    return 0
        return 0

    @database_sync_to_async
    def _load_events_after(self, sequence):
        events = (
            GraphExecutionEvent.objects.filter(
                session__session_id=self.session_id,
                sequence__gt=sequence,
            )
            .order_by("sequence")
        )
        result = []
        for event in events:
            payload = dict(event.raw_event or {})
            payload.update({
                "sequence": event.sequence,
                "event": event.event_type,
                "state": event.state,
                "message": event.message,
                "timestamp": event.occurred_at.timestamp(),
                "data": event.payload,
                "session_id": self.session_id,
            })
            result.append(payload)
        return result
