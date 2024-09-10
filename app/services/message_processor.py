import asyncio
from typing import Dict

from app.models.webhook_data import WebhookData
from app.services.analysis_service import execute_analysis
from app.services.evolution_api_service import send_to_evolution_api
from app.utils.timer import Timer


class MessageProcessor:
    def __init__(self):
        self.messages_by_phone: Dict[str, list] = {}
        self.timers: Dict[str, Timer] = {}
        self.results_by_phone: Dict[str, str] = {}

    async def process(self, data: WebhookData):
        phone = data.phone_number
        message = data.message
        timestamp = asyncio.get_event_loop().time()

        if message is None:
            print(f"Mensagem não reconhecida para o número {phone}")
            print("JSON recebido:")
            print(data.get_json_representation())
            return

        if phone not in self.messages_by_phone:
            self.messages_by_phone[phone] = []
        self.messages_by_phone[phone].append(
            {"message": message, "timestamp": timestamp}
        )

        if phone in self.timers:
            self.timers[phone].cancel()

        self.timers[phone] = Timer(10, self._process_messages, phone)

        messages_received = " ".join(
            [msg["message"] for msg in self.messages_by_phone[phone]]
        )
        result = self.results_by_phone.get(
            phone,
            "Resultado não disponível ainda"
        )

        return {
            "status": "Evento recebido e será processado em breve",
            "phone": phone,
            "message": message,
            "result": messages_received,
            "resultado": result
        }

    async def _process_messages(self, phone: str):
        messages = self.messages_by_phone.pop(phone, [])
        if phone in {
            "554898523000",
            "554899952533",
            "554891069028",
            "554891890377"
            } and messages:
            message_complete = " ".join([msg["message"] for msg in messages])
            print(f"iniciando processamento para {phone}")
            result = execute_analysis(message_complete)
            self.results_by_phone[phone] = result
            print(f"Processando mensagens para {phone}: {message_complete}")
            print(f"Resultado: {result}")

            try:
                result_str = str(result)
                response = await send_to_evolution_api(phone, result_str)
                print(f"Envio para Evolution API bem-sucedido: {response}")
            except Exception as e:
                print(f"Erro ao enviar para Evolution API: {e}")
        elif phone != "55484070000":
            print(f"Número de telefone não autorizado: {phone}")
        elif not messages:
            print(f"Nenhuma mensagem para processar para {phone}")
        else:
            print(f"Condição não atendida para o número {phone}")
