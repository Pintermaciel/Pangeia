from cfg import Config
from crewai import Agent, Crew, Process, Task
from crewai_tools import tool
from langchain_community.utilities import SQLDatabase
from langchain_groq import ChatGroq

# Configuração do banco de dados
db_url = Config()
db = SQLDatabase.from_uri(db_url.VECTOR_DATABASE_URL)


def get_schema(_):
    schema = db.get_table_info()
    return schema


# Configuração do LLM
llm = ChatGroq(
    temperature=0,
    groq_api_key="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",
    model_name="llama3-70b-8192"
)


@tool("Execute query DB tool")
def run_query(query: str):
    """Execute a query in the database and return the data."""
    print(query)
    try:
        result = db.run(query, fetch="all", include_columns=True)
        return f"This is the return data from the database: {result}"
    except Exception as e:
        return f"Error executing query: {str(e)}"


# Definição do agente
sql_developer_agent = Agent(
    role='Senior SQL developer',
    goal="Return data from the database by running the Execute query DB tool.",
    backstory="""You are familiar with Postgres syntax. You know the following table schema. Use the Execute query DB tool to execute the query in the database. You must respond to what is returned from the database.""",
    tools=[run_query],
    allow_delegation=False,
    verbose=True,
    llm=llm
)

# Definição da tarefa
sql_developer_task = Task(
    description="""Build a SQL query to answer the question: {question}. Follow the following schema: {schema}. This query is executed on the database. Your final answer MUST be exactly text in portuguese(pt-BR) based on the data returned.""",
    expected_output="""Return data from the database.""",
    agent=sql_developer_agent,
    tools=[run_query]
)

# Configuração da equipe
crew = Crew(
    agents=[sql_developer_agent],
    tasks=[sql_developer_task],
    process=Process.sequential,
    manager_llm=llm
)

# Execução do processo
result = crew.kickoff(inputs={'question': 'quanto que eu paguei no mês de junho desse ano? e quanto eu recebi?', 'schema': get_schema(None)})

print(result)
