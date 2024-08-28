import asyncio
import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from pangeia.ai_integration.agents import execute_analysis

app = FastAPI()


class QueryRequest(BaseModel):
    question: str


@app.post("/execute_query/")
async def execute_query(request: QueryRequest):
    try:
        result = execute_analysis(request.question)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Dicionário para armazenar as mensagens recebidas por telefone
mensagens_por_telefone = {}


@app.post("/webhook/")
async def webhook(request: Request):
    telefone = request.query_params.get("telefone")
    if telefone is None:
        return JSONResponse(
            content={"error": "Telefone não informado"},
            status_code=400
        )

    # Verificar se já há mensagens para o telefone
    if telefone in mensagens_por_telefone:
        # Concatenar as mensagens recebidas durante os últimos 10 segundos
        mensagens = mensagens_por_telefone[telefone]
        await asyncio.sleep(10)  # Esperar 10 segundos
        novas_mensagens = [
            msg for msg in mensagens
            if msg["timestamp"] > time.time() - 10
        ]

        mensagens_por_telefone[telefone] = novas_mensagens
    else:
        mensagens_por_telefone[telefone] = []

    # Processar a requisição
    # ...

    return JSONResponse(
        content={"message": "Requisição processada com sucesso"},
        status_code=200
    )


# Ponto de entrada para rodar a API
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
