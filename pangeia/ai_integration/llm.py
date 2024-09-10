from langchain_openai import ChatOpenAI
from pangeia.cfg import Config

# Configuração do LLM
llm = ChatOpenAI(
    model_name="gpt-4o-mini-2024-07-18",
    temperature=0,
    openai_api_key=Config.OPENAI_API_KEY
)
