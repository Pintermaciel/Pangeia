from crewai import Agent, Crew, Process, Task
from crewai_tools import tool
from .utils import query_request_agent_logic
from .llm import llm
from .database import get_schema, run_query

@tool("Execute query DB tool")
def execute_query_tool(query: str):
    """Executa uma query no banco de dados e retorna os dados."""
    return run_query(query)

# Agente 1: Query Request Agent - Identifica as queries necessárias
query_request_agent = Agent(
    role='SQL Query Requester',
    goal=(
        "Interpretar a pergunta do usuário e identificar as queries SQL "
        "necessárias para responder à pergunta. As colunas devem sempre estar "
        "entre aspas duplas e o nome da tabela e da coluna deve ser validado no banco de dados."
        "As querys devem conter somente as colunas necessarias, para otimizar a consulta"
    ),
    backstory=(
        "Você é um especialista em SQL e entende que as colunas em PostgreSQL "
        "devem estar entre aspas duplas e que funções específicas do PostgreSQL "
        "como EXTRACT(YEAR FROM coluna) devem ser usadas para extrair o ano. "
        "Sempre Verifique se as tabelas no schema {schema} e as colunas necessárias existem antes de sugerir uma query."
    ),
    tools=[execute_query_tool],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

query_request_task = Task(
    description=(
        'Baseado na pergunta "{question}",'
        "identifique as queries SQL necessárias para obter os dados corretos."
        "Verifique se as tabelas no schema {schema} e as colunas necessárias existem no banco de dados,"
        "e use as funções PostgreSQL adequadas."
    ),
    expected_output=(
        "Retorne uma descrição detalhada das consultas,"
        "incluindo a coluna e tabela corretas,"
        "e qualquer consulta adicional que possa fornecer insights mais completos."
    ),
    agent=query_request_agent
)

pam_rex_agent = Agent(
    role='Data Insight Transcriber',
    goal="Ler os dados fornecidos pelo time técnico e transcrever informações em insights breves e fáceis de interpretar.",
    backstory=(
        "Você é o PAM-REX, um Agente de Dados fofo, generoso e otimista."
        "Seus valores são a verdade e a integridade das informações. "
        "Sua função é pegar os dados complexos e transformá-los em insights claros e acessíveis,"
        "para que qualquer pessoa possa entender."
    ),
    tools=[],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

pam_rex_task = Task(
    description=(
        "Você deve sempre se apresentar como PAM-REX, o dinossauro especialista em análise de dados. "
        "Forneça insights breves e claros em português, sempre de forma fofa e generosa. "
        "Além de apresentar os resultados, identifique tendências, comparações com períodos anteriores, "
        "percentuais de contribuição para totais, e outros insights relevantes. "
        "Mantenha as mensagens curtas e acessíveis, como se estivesse enviando uma mensagem pelo WhatsApp. "
        "Garanta a integridade das informações e explique qualquer erro ou questão com os dados."
    ),
    expected_output=(
        "Sera uma mensagem enviada no whatsapp, precisa ser uma mensagem fofa e acessível. e breve. com um layout atraente"
        "Retorne os resultados das queries com insights breves e fáceis de interpretar, mantendo a integridade das informações. "
        "Indique qualquer erro potencial ou questão com os dados, e ofereça insights adicionais que possam agregar valor."
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
    # Definir os inputs iniciais para o processo
    schema = get_schema()  # Obtém o schema do banco de dados
    description = query_request_agent_logic(question)
    
    generator_inputs = {
        'question': question,
        'schema': schema,
        'description': description,
    }

    # Executar o processo completo usando o Crew
    result = crew.kickoff(inputs=generator_inputs)

    return result
