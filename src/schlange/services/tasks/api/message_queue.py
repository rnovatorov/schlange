import json

from schlange.api import messaging as messaging_api
from schlange.services.tasks import core


class MessageQueue:
    """Adapts messaging API to tasks core MessageQueue port."""

    def __init__(self, messaging_server: messaging_api.Server) -> None:
        self.messaging_server = messaging_server

    def publish(self, request: core.TaskExecutionRequest) -> None:
        payload = json.dumps(
            {
                "task_id": request.task_id,
                "seq_num": request.seq_num,
                "kind": request.kind,
                "args": request.args,
            }
        ).encode()
        self.messaging_server.publish_message(
            messaging_api.PublishMessageRequest(
                queue=request.kind,
                payload=payload,
            )
        )
