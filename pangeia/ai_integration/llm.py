from langchain_groq import ChatGroq

from pangeia.cfg import Config

# Configuração do LLM
llm = ChatGroq(
    temperature=0,
    groq_api_key=f"{Config.GROQ_API_KEY}",
    model_name="llama3-70b-8192"
)
