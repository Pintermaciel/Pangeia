from fuzzywuzzy import process

# Mapeamento de palavras-chave para colunas
column_mapping = {
    "despesa": "VALOR_TOTAL_DESPESA",
    "custo": "VALOR_TOTAL_CUSTO",
    "plano de conta": "DESCRICAO_CONTA",
    "conta": "DESCRICAO_CONTA"
}


def get_column_from_question(question):
    MINIMUM_MATCH_SCORE = 80
    keywords = column_mapping.keys()
    best_match, match_score = process.extractOne(question.lower(), keywords)

    if match_score > MINIMUM_MATCH_SCORE:
        return column_mapping[best_match]
    return None


def query_request_agent_logic(question):
    column = get_column_from_question(question)
    if column is None:
        description = (
            "Desculpe, não consegui identificar"
            "a coluna correta para sua consulta."
        )
    else:
        description = (
            f"A consulta deve usar a coluna '{column}' para obter os dados."
        )
    return description
