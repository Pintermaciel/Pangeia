import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.message_processor import MessageProcessor

client = TestClient(app)

HTTP_STATUS_OK = 200


@pytest.fixture(autouse=True)
def _clear_message_processor():
    MessageProcessor().messages_by_phone.clear()


def test_webhook_single_message():
    response = client.post(
        "/webhook/",
        json={"jid": "12345", "message": "Hello"}
        )
    assert response.status_code == HTTP_STATUS_OK
    assert response.json()["result"] == "Hello"
    assert response.json()["resultado"] == "Resultado não disponível ainda"
