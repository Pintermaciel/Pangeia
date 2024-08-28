from crewai import Agent, Crew, Process, Task
from crewai_tools import tool
from fuzzywuzzy import process
from langchain_community.utilities import SQLDatabase
from langchain_groq import ChatGroq

from pangeia.cfg import Config

# Configuração do banco de dados
db_url = Config()
db = SQLDatabase.from_uri(db_url.VECTOR_DATABASE_URL)


def get_schema():
    schema = db.get_table_info()
    return schema


# Configuração do LLM
llm = ChatGroq(
    temperature=0,
    groq_api_key="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",
    model_name="llama3-70b-8192"
)

# Mapeamento de palavras-chave para colunas
column_mapping = {
    "despesa": "VALOR_TOTAL_DESPESA",
    "custo": "VALOR_TOTAL_CUSTO",
}


def get_column_from_question(question):
    keywords = column_mapping.keys()
    best_match, match_score = process.extractOne(question.lower(), keywords)

    if match_score > 80:  # Limite ajustável
        return column_mapping[best_match]
    return None


def query_request_agent_logic(question):
    column = get_column_from_question(question)
    if column is None:
        return "Desculpe, não consegui identificar a coluna correta para sua consulta."

    description = f"A consulta deve usar a coluna '{column}' para obter os dados."
    return description


@tool("Execute query DB tool")
def run_query(query: str):
    """Execute a query in the database and return the data."""
    print(query)
    try:
        result = db.run(query, fetch="all", include_columns=True)
        return result
    except Exception as e:
        return f"Error executing query: {str(e)}"


# Agente 1: Query Request Agent - Identifica as queries necessárias
query_request_agent = Agent(
    role='SQL Query Requester',
    goal=(
        "Interpretar a pergunta do usuário e identificar as queries SQL "
        "necessárias para responder à pergunta. As colunas devem sempre estar "
        "entre aspas duplas e o nome da tabela e da coluna deve ser validado no banco de dados."
    ),
    backstory=(
        "Você é um especialista em SQL e entende que as colunas em PostgreSQL "
        "devem estar entre aspas duplas e que funções específicas do PostgreSQL "
        "como EXTRACT(YEAR FROM coluna) devem ser usadas para extrair o ano. "
        "Sempre verifique a existência das tabelas e colunas antes de sugerir uma query."
    ),
    tools=[],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

query_request_task = Task(
    description=(
        'Baseado na pergunta "{question}",'
        "identifique as queries SQL necessárias para obter os dados corretos."
        "Além de responder diretamente à pergunta,"
        "considere outras consultas SQL que possam ser relevantes, "
        "como desvios de padrão, medidas de tendência,"
        "analise vertical e horizontal,"
        "como comparações com períodos anteriores, percentuais de contribuição para totais,"
        "ou tendências ao longo do tempo. "
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


# Agente 2: Query Generator Agent - Gera as queries SQL
query_generator_agent = Agent(
    role='SQL Query Generator',
    goal=(
        "Gerar as queries SQL necessárias com base na descrição fornecida, "
        "garantindo que as colunas estejam entre aspas duplas e que as funções específicas do PostgreSQL "
        "sejam utilizadas corretamente."
    ),
    backstory=(
        "Você é responsável por gerar queries SQL precisas e eficientes com base "
        "nas instruções fornecidas pelo Query Request Agent. Utilize funções como "
        "EXTRACT(YEAR FROM coluna) para extrair informações de data no PostgreSQL."
    ),
    tools=[],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

query_generator_task = Task(
    description=(
        'Gere as queries SQL baseadas na descrição "{description}".'
        "Certifique-se de que as colunas estejam entre aspas duplas"
        "e que as funções do PostgreSQL sejam usadas corretamente."
        "Inclua consultas adicionais que possam fornecer insights"
        "comparativos ou análises complementares, como tendências,"
        "comparações com o ano anterior, e percentuais de contribuição para totais."
    ),
    expected_output=(
        "Retorne as queries SQL geradas,"
        "incluindo consultas adicionais "
        "que possam ajudar na geração de insights mais completos."
    ),
    agent=query_generator_agent
)

# Agente 3: Data Analyst Agent - Executa as queries e fornece insights
data_analyst_agent = Agent(
    role='Data Analyst',
    goal="Executar queries SQL, coletar dados e fornecer insights.",
    backstory=(
        "Você é um analista de dados que executa queries SQL, analisa os "
        "resultados e gera insights significativos baseados nos dados "
        "coletados. Garanta que os resultados estejam corretos e forneça insights "
        "em português de forma clara e útil."
    ),
    tools=[run_query],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

data_analyst_task = Task(
    description=(
        "Execute as queries SQL no banco de dados,"
        "analise os resultados e forneça insights em português,"
        "levando em consideração a precisão e relevância dos dados."
        "Além dos resultados diretos, identifique tendências, "
        "comparações com períodos anteriores, percentuais de contribuição para totais,"
        "e outros insights que possam ser relevantes para a pergunta original. "
        "Explique claramente os resultados e ofereça recomendações baseadas nos dados analisados."
    ),
    expected_output=(
        "Retorne os resultados das queries,"
        "forneça insights significativos em português,"
        "e ofereça análises adicionais que possam agregar valor à interpretação dos dados. "
        "Indique claramente qualquer possível erro ou problema com os dados e como ele pode afetar os insights."
    ),
    agent=data_analyst_agent,
    tools=[run_query]
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
    agents=[query_request_agent, query_generator_agent, data_analyst_agent, pam_rex_agent],
    tasks=[query_request_task, query_generator_task, data_analyst_task, pam_rex_task],
    process=Process.sequential,
    manager_llm=llm
)


def execute_analysis(question):
    # Passo 1: Identificar as queries necessárias e gerar inputs iniciais
    schema = get_schema()  # Obtém o schema do banco de dados
    description = query_request_agent_logic(question)

    if "Desculpe" in description:
        return description

    # Inputs iniciais para o kickoff
    generator_inputs = {
        'question': question,
        'schema': schema,
        'column_mapping': column_mapping,
        'description': description,
    }

    # Executa o processo completo usando o Crew
    result = crew.kickoff(inputs=generator_inputs)

    return result


# Execução do processo para calcular despesas
# question_despesa = 'quanto eu paguei de despesa nesse ano?'
# result_despesa = execute_analysis(question_despesa)

# Execução do processo para calcular custos
# question_custo = 'quanto eu paguei de custo nesse ano?'
# result_custo = execute_analysis(question_custo)

# #print(result_despesa)
# print(result_custo)
