FROM python:3.11-slim

# Defina a variável de ambiente para não criar ambientes virtuais
ENV POETRY_VIRTUALENVS_CREATE=false

# Instalar dependências do sistema operacional
RUN apt-get update && apt-get install -y \
    libpq-dev \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Defina o diretório de trabalho
WORKDIR /app

# Copie os arquivos do projeto para o contêiner
COPY . .

# Instale o Poetry
RUN pip install poetry

# Configure o Poetry para usar 10 workers durante a instalação
RUN poetry config installer.max-workers 10

# Instale as dependências Python usando o Poetry
RUN poetry install --no-interaction --no-ansi

# Exponha a porta que o serviço usará
EXPOSE 8000

# Comando para iniciar o serviço
CMD ["poetry", "run", "uvicorn", "--host", "0.0.0.0", "app:app"]
