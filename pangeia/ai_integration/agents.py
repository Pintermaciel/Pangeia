from crewai import Agent, Crew, Process, Task
from crewai_tools import tool
import json

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
        "Interpretar a pergunta do usuário e identificar as queries SQL "
        "necessárias para responder à pergunta. Se uma abordagem não funcionar, "
        "tente uma diferente. Varie as consultas e use diferentes tabelas ou junções "
        "se necessário. As colunas devem sempre estar entre aspas duplas, e o nome "
        "da tabela e da coluna deve ser validado no banco de dados. As queries devem "
        "conter somente as colunas necessárias, para otimizar a consulta."
    ),
    backstory=(
        "Você é um especialista em SQL e entende que as colunas em "
        "PostgreSQL devem estar entre aspas duplas e que funções "
        "específicas do PostgreSQL como EXTRACT(YEAR FROM coluna) "
        "devem ser usadas para extrair o ano. Sempre verifique se "
        "as tabelas no schema fornecido e as colunas necessárias "
        "existem antes de sugerir uma query."
    ),
    tools=[execute_query_tool],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

query_request_task = Task(
    description=(
        "Baseado na pergunta '{question}', "
        "identifique as queries SQL necessárias para obter os dados corretos. "
        "Verifique se as tabelas no schema {schema} e as colunas necessárias existem no banco de dados, "
        "e use as funções PostgreSQL adequadas. "
        "Instruções para execução de consultas SQL:\n"
        "1. Ao formular uma consulta SQL, sempre a envie como um JSON com a chave 'query'.\n"
        "2. O formato correto para usar a ferramenta 'Execute query DB tool' é:\n"
        "Action: Execute query DB tool\n"
        "Action Input: {{\"query\": \"SELECT column1, column2 FROM table WHERE condition;\"}}\n"
        "3. Certifique-se de que a consulta SQL esteja completa e correta antes de enviá-la.\n"
        "4. Exemplo correto:\n"
        "Action: Execute query DB tool\n"
        "Action Input: {{\"query\": \"SELECT \\\"ID_CONTAS_PAGAR\\\", \\\"DATA_VENCIMENTO_CP\\\" FROM "
        "\\\"contas_pagar\\\" WHERE \\\"DATA_VENCIMENTO_CP\\\" < CURRENT_DATE;\"}}\n"
    ),
    expected_output=(
        "Retorne uma descrição detalhada das consultas, "
        "incluindo a coluna e tabela corretas, "
        "e qualquer consulta adicional que possa fornecer "
        "insights mais completos."
    ),
    agent=query_request_agent
)

pam_rex_agent = Agent(
    role='Data Insight Transcriber',
    goal=(
        "Ler os dados fornidos pelo time técnico e transcrever\n"
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

import traceback

def execute_analysis(question):
    try:
        schema = get_schema()  # Obtém o schema do banco de dados
        print(f"Schema obtido: {schema}")
        
        description = query_request_agent_logic(question)
        print(f"description: {description}")
        
        print("Iniciando crew.kickoff()...")
        inputs = {
            'question': question,
            'schema': schema,
            'description': description,
        }
        print(f"Inputs para crew.kickoff(): {inputs}")
        
        # Executa o crew com os inputs
        result = crew.kickoff(inputs=inputs)
        print(f"Resultado do crew.kickoff(): {result}")
        
        return result
    except Exception as e:
        print(f"Erro durante a execução de execute_analysis: {str(e)}")
        print(traceback.format_exc())
        return f"Erro: {str(e)}"
