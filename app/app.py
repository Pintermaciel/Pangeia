# import asyncio
# import time
# from typing import Dict

# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel

# from pangeia.ai_integration.agents import execute_analysis

# #app = FastAPI()


# class QueryRequest(BaseModel):
#     question: str


# @app.post("/execute_query/")
# async def execute_query(request: QueryRequest):
#     try:
#         result = execute_analysis(request.question)
#         return {"result": result}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# # Dicionários globais
# mensagens_por_telefone: Dict[str, list] = {}
# timers: Dict[str, asyncio.TimerHandle] = {}
# resultados_por_telefone: Dict[str, str] = {}


# class WebhookData(BaseModel):
#     jid: str
#     message: str
#     timestamp: float = None


# async def process_messages(telefone: str):
#     # Esta função será chamada após o timer expirar
#     novas_mensagens = mensagens_por_telefone.pop(telefone, [])
#     if novas_mensagens:
#         if len(novas_mensagens) == 1:
#             mensagem_completa = novas_mensagens[0]["message"]
#         else:
#             mensagem_completa = " ".join(
#                 [msg["message"] for msg in novas_mensagens]
#                 )

#         resultado = execute_analysis(mensagem_completa)
#         resultados_por_telefone[telefone] = resultado
#         print(f"Processando mensagens para {telefone}: {mensagem_completa}")
#         print(f"Resultado: {resultado}")
#     else:
#         print(f"Nenhuma mensagem para processar para {telefone}")


# @app.post("/webhook/")
# async def webhook(data: WebhookData):
#     telefone = data.jid
#     mensagem = data.message
#     timestamp = data.timestamp if data.timestamp else time.time()

#     if telefone in mensagens_por_telefone:
#         mensagens_por_telefone[telefone].append(
#             {"message": mensagem, "timestamp": timestamp}
#         )
#     else:
#         mensagens_por_telefone[telefone] = [
#             {"message": mensagem, "timestamp": timestamp}
#         ]

#     if telefone in timers:
#         timers[telefone].cancel()

#     timers[telefone] = asyncio.get_event_loop().call_later(
#         10,
#         lambda: asyncio.ensure_future(process_messages(telefone))
#     )

#     mensagens_recebidas = " ".join(
#         [msg["message"] for msg in mensagens_por_telefone.get(telefone, [])]
#     )

#     resultado = resultados_por_telefone.get(
#         telefone,
#         "Resultado não disponível ainda"
#     )

#     return {
#         "status": "Mensagem recebida e será processada em breve",
#         "result": mensagens_recebidas,
#         "resultado": resultado
#     }


# # Ponto de entrada para rodar a API
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)
