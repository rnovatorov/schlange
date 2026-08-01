import dataclasses


@dataclasses.dataclass
class RequeueMessageRequest:
    message_id: str
    version: int
