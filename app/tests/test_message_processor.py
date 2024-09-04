
import pytest

from app.models.webhook_data import WebhookData
from app.services.message_processor import MessageProcessor

HTTP_STATUS_OK = 200


@pytest.mark.asyncio
async def test_process_single_message():
    processor = MessageProcessor()
    data = WebhookData(jid="12345", message="Hello")
    result = await processor.process(data)
    assert result.status_code == HTTP_STATUS_OK
    assert result["result"] == "Hello"
    assert result["resultado"] == "Resultado não disponível ainda"
