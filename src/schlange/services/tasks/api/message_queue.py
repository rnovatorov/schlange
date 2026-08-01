import json
import logging

from schlange.api import messaging as messaging_api
from schlange.services.messaging import core as messaging_core
from schlange.services.tasks import core

LOGGER = logging.getLogger(__name__)


class MessageQueue:
    """Adapts messaging API to tasks core MessageQueue port.

    Auto-declares the task queue and its dead-letter queue on first
    publish per kind (idempotent). The DLQ name follows the convention
    ``{kind}.dlq``.
    """

    def __init__(
        self,
        messaging_server: messaging_api.Server,
        max_delivery_count: int,
    ) -> None:
        self.messaging_server = messaging_server
        self.max_delivery_count = max_delivery_count
        self._declared: set[str] = set()

    def publish(self, request: core.TaskExecutionRequest) -> None:
        if request.kind not in self._declared:
            self._ensure_queue(request.kind)
            self._declared.add(request.kind)
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
                visibility_timeout=request.visibility_timeout,
            )
        )

    def _ensure_queue(self, kind: str) -> None:
        dlq_name = f"{kind}.dlq"
        for name, dlq in [(dlq_name, None), (kind, dlq_name)]:
            try:
                self.messaging_server.declare_queue(
                    messaging_api.DeclareQueueRequest(
                        name=name,
                        dead_letter_queue=dlq,
                        max_delivery_count=self.max_delivery_count,
                    )
                )
            except messaging_core.QueueAlreadyExistsError:
                pass
