from fastapi import FastAPI
from app.api import webhook, query
from dotenv import load_dotenv
import os

# Carregar o arquivo .env padrão
load_dotenv()

app = FastAPI()

app.include_router(webhook.router)
app.include_router(query.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)