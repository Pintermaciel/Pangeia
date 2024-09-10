import json
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class WebhookData(BaseModel):
    event: str
    instance: str
    data: Dict[str, Any]
    timestamp: Optional[float] = Field(
        default_factory=lambda: datetime.now().timestamp()
        )

    class Config:
        extra = "allow"

    @property
    def phone_number(self) -> str:
        return self.data['key']['remoteJid'].split('@')[0]

    @property
    def message(self) -> Optional[str]:
        message_data = self.data.get('message', {})
        if isinstance(message_data, dict):
            if 'extendedTextMessage' in message_data:
                return message_data['extendedTextMessage'].get('text')
            elif 'conversation' in message_data:
                return message_data['conversation']
            elif 'text' in message_data:
                return message_data['text']
        return None

    def get_json_representation(self) -> str:
        return json.dumps(self.dict(), indent=2)
