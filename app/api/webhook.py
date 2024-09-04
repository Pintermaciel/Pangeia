from fastapi import APIRouter
import asyncio
from app.models.webhook_data import WebhookData
from app.services.message_processor import MessageProcessor

router = APIRouter()
message_processor = MessageProcessor()


@router.post("/webhook/")
async def webhook(data: WebhookData):
    asyncio.create_task(message_processor.process(data))
    return {"status": "Mensagem recebida com sucesso"}