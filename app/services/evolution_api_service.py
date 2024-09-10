import os

import httpx
from dotenv import load_dotenv

load_dotenv()

EVOLUTION_API_KEY = os.getenv("EVOLUTION_API_KEY")


async def send_to_evolution_api(number: str, text: str):
    url = "https://evolution.zeuslab.app/message/sendText/banansilvanathi"
    headers = {"apiKey": f"{EVOLUTION_API_KEY}"}
    data = {
        "number": f"{number}@s.whatsapp.net",
        "textMessage": {
            "text": text
        }
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data, headers=headers)
        response.raise_for_status()
        print(f"Resposta da Evolution API: {response.json()}")
        return response.json()
