from crewai import Agent, Crew, Process, Task
from crewai_tools import tool

from .database import get_schema, run_query
from .llm import llm
from .utils import query_request_agent_logic


@tool("Execute query DB tool")
def execute_query_tool(query: str):
    """Executa uma query no banco de dados e retorna os dados."""
    return run_query(query)


# Agente 1: Query Request Agent - Identifica as queries necessárias
query_request_agent = Agent(
    role='SQL Query Requester',
    goal=(
        "Interpretar a pergunta do usuário e identificar as queries SQL\n"
        "necessárias para responder à pergunta. As colunas devem sempre\n"
        "estar entre aspas duplas, e o nome da tabela e da coluna deve\n"
        "ser validado no banco de dados. As queries devem conter somente\n"
        "as colunas necessárias, para otimizar a consulta."
    ),
    backstory=(
        "Você é um especialista em SQL e entende que as colunas em\n"
        "PostgreSQL devem estar entre aspas duplas e que funções\n"
        "específicas do PostgreSQL como EXTRACT(YEAR FROM coluna)\n"
        "devem ser usadas para extrair o ano. Sempre verifique se\n"
        "as tabelas no schema {schema} e as colunas necessárias\n"
        "existem antes de sugerir uma query."
    ),
    tools=[execute_query_tool],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

query_request_task = Task(
    description=(
        'Baseado na pergunta "{question}",\n'
        "identifique as queries SQL necessárias para obter os dados\n"
        "corretos. Verifique se as tabelas no schema {schema} e as\n"
        "colunas necessárias existem no banco de dados, e use as\n"
        "funções PostgreSQL adequadas."
    ),
    expected_output=(
        "Retorne uma descrição detalhada das consultas,\n"
        "incluindo a coluna e tabela corretas,\n"
        "e qualquer consulta adicional que possa fornecer\n"
        "insights mais completos."
    ),
    agent=query_request_agent
)

pam_rex_agent = Agent(
    role='Data Insight Transcriber',
    goal=(
        "Ler os dados fornecidos pelo time técnico e transcrever\n"
        "informações em insights breves e fáceis de interpretar."
    ),
    backstory=(
        "Você é o PAM-REX, um Agente de Dados fofo, generoso e otimista.\n"
        "Seus valores são a verdade e a integridade das informações.\n"
        "Sua função é pegar os dados complexos e transformá-los em\n"
        "insights claros e acessíveis, para que qualquer pessoa\n"
        "possa entender."
    ),
    tools=[],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

pam_rex_task = Task(
    description=(
        "Você deve sempre se apresentar como PAM-REX, o dinossauro\n"
        "especialista em análise de dados. Forneça insights breves e\n"
        "claros em português, sempre de forma fofa e generosa. Além de\n"
        "apresentar os resultados, identifique tendências, comparações\n"
        "com períodos anteriores, percentuais de contribuição para\n"
        "totais, e outros insights relevantes. Mantenha as mensagens\n"
        "curtas e acessíveis, como se estivesse enviando uma mensagem\n"
        "pelo WhatsApp. Garanta a integridade das informações e\n"
        "explique qualquer erro ou questão com os dados."
    ),
    expected_output=(
        "Será uma mensagem enviada no WhatsApp, precisa ser uma mensagem\n"
        "fofa, acessível e breve, com um layout atraente. Retorne os\n"
        "resultados das queries com insights breves e fáceis de\n"
        "interpretar, mantendo a integridade das informações. Indique\n"
        "qualquer erro potencial ou questão com os dados, e ofereça\n"
        "insights adicionais que possam agregar valor."
    ),
    agent=pam_rex_agent,
)

# Configuração da equipe
crew = Crew(
    agents=[query_request_agent, pam_rex_agent],
    tasks=[query_request_task, pam_rex_task],
    process=Process.sequential,
    manager_llm=llm
)


def execute_analysis(question):
    schema = get_schema()  # Obtém o schema do banco de dados
    description = query_request_agent_logic(question)

    # Executa o processo completo usando o Crew
    return crew.kickoff(inputs={
        'question': question,
        'schema': schema,
        'description': description,
    })
