import logging

from cfg import Config
from langchain.chains import RetrievalQA
from langchain.llms import OpenAI
from langchain.memory import SimpleMemory
from langchain.prompts import PromptTemplate
from langchain.vectorstores import SQLVectorStore
from sqlalchemy import MetaData, create_engine

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RAGAgent:
    def __init__(self):
        # Carregar configurações
        self.config = Config()

        # Configurar LLM
        self.llm = OpenAI(api_key=self.config.OPENAI_API_KEY)

        # Conectar ao banco vetorizado
        self.engine = create_engine(self.config.VECTOR_DATABASE_URL)
        self.metadata = MetaData()

    def get_vector_store(self, table_name):
        logger.info(f"Configurando o SQLVectorStore para a tabela {table_name}")
        vector_store = SQLVectorStore.from_table(self.engine, table_name, 'embedding')
        return vector_store

    def create_retrieval_qa_chain(self, kpi):
        vector_store = self.get_vector_store(kpi)

        # Configurar o prompt template
        prompt_template = PromptTemplate(
            template="Use the following context to answer the question:\n\n{context}\n\nQuestion: {question}",
            input_variables=["context", "question"]
        )

        # Criar o RetrievalQA chain
        retriever = vector_store.as_retriever()
        qa_chain = RetrievalQA(
            retriever=retriever,
            llm=self.llm,
            prompt_template=prompt_template,
            memory=SimpleMemory()
        )

        return qa_chain

    def interact_with_client(self, kpi, query):
        qa_chain = self.create_retrieval_qa_chain(kpi)

        # Processar a consulta do cliente
        response = qa_chain.run(query)
        return response


# Exemplo de uso
if __name__ == "__main__":
    agent = RAGAgent()

    kpi = 'kpi1'  # Substitua pelo KPI desejado
    query = "Qual foi o valor total das vendas no último mês?"  # Exemplo de consulta

    try:
        response = agent.interact_with_client(kpi, query)
        logger.info(f"Resposta do agente: {response}")
    except Exception as e:
        logger.error(f"Erro ao interagir com o cliente: {e}")
