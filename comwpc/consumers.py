import json

from channels.generic.websocket import AsyncWebsocketConsumer


class ExecutionConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer для live-событий исполнения графа.
    Один client подключается к одной session_id:
    /ws/execution/<session_id>/
    """
    async def connect(self):
        self.session_id = self.scope["url_route"]["kwargs"]["session_id"]
        self.group_name = f"execution_{self.session_id}"

        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name
        )

        await self.accept()

        await self.send_json({
            "type": "connected",
            "session_id": self.session_id
        })

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