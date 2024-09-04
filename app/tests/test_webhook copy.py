import time

import pytest
from fastapi.testclient import TestClient

from app.app import (
    app,
    mensagens_por_telefone,
)

client = TestClient(app)

HTTP_STATUS_OK = 200


@pytest.fixture(autouse=True)
def _clear_mensagens_por_telefone():
    '''Limpar o dicionário antes de cada teste
    para garantir que os testes sejam independentes'''
    mensagens_por_telefone.clear()


def test_webhook_single_message():
    response = client.post(
        "/webhook/",
        json={"jid": "12345", "message": "Hello"}
        )
    assert response.status_code == HTTP_STATUS_OK
    assert "result" in response.json()
    assert response.json()["result"] == "Hello"
    assert "resultado" in response.json()
    assert response.json()["resultado"] != "Resultado não disponível ainda"


def test_webhook_multiple_messages():
    client.post("/webhook/", json={"jid": "12345", "message": "Hello"})
    client.post("/webhook/", json={"jid": "12345", "message": "Matheus"})
    client.post("/webhook/", json={"jid": "12345", "message": "Teste"})
    response = client.post(
        "/webhook/",
        json={"jid": "12345", "message": "World"}
        )
    assert response.status_code == HTTP_STATUS_OK
    result = response.json()["result"]
    assert result == "Hello Matheus Teste World"


def test_webhook_different_phones():
    # Primeira mensagem para o telefone 66666
    response_1a = client.post(
        "/webhook/",
        json={"jid": "66666", "message": "Message 1"}
        )
    assert response_1a.status_code == HTTP_STATUS_OK
    assert response_1a.json()["result"] == "Message 1"
    assert response_1a.json()["resultado"] != "Resultado não disponível ainda"

    # Primeira mensagem para o telefone 67890
    response_2a = client.post(
        "/webhook/",
        json={"jid": "67890", "message": "Message 2"}
        )
    assert response_2a.status_code == HTTP_STATUS_OK
    assert response_2a.json()["result"] == "Message 2"
    assert response_2a.json()["resultado"] != "Resultado não disponível ainda"

    # Segunda mensagem para o telefone 51515151
    response_1b = client.post(
        "/webhook/",
        json={"jid": "51515151", "message": "quanto eu gastei esse ano?"}
    )
    assert response_1b.status_code == HTTP_STATUS_OK
    assert response_1b.json()["result"] == "Message 1 quanto eu gastei esse ano?"
    assert response_1b.json()["resultado"] != "Resultado não disponível ainda"

    # Segunda mensagem para o telefone 67890
    response_2b = client.post(
        "/webhook/",
        json={"jid": "67890", "message": "qual meu maior custo?"}
    )
    assert response_2b.status_code == HTTP_STATUS_OK
    assert response_2b.json()["result"] == "Message 2 qual meu maior custo?"
    assert response_2b.json()["resultado"] != "Resultado não disponível ainda"

    # Esperar um pouco para o processamento assíncrono ocorrer
    time.sleep(11)

    # Verificar se o resultado foi processado após o tempo de espera
    response_1c = client.post(
        "/webhook/",
        json={"jid": "125125", "message": "nova mensagem"}
        )
    assert response_1c.json()["resultado"] != "Resultado não disponível ainda"

    response_2c = client.post(
        "/webhook/",
        json={"jid": "67890", "message": "nova mensagem"}
        )
    assert response_2c.json()["resultado"] != "Resultado não disponível ainda"
