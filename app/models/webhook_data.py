from pydantic import BaseModel


class WebhookData(BaseModel):
    jid: str
    message: str
    timestamp: float = None
