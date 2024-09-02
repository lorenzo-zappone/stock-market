# Use a imagem base do Airflow
FROM apache/airflow:slim-2.9.3-python3.12

# Mudar para o usuário root para instalar dependências do sistema
USER root

# Instalar dependências necessárias para construir pacotes Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Voltar para o usuário padrão do Airflow
USER airflow

# Copiar os arquivos necessários
COPY requirements.txt /opt/airflow/requirements.txt
COPY script/entrypoint.sh /opt/airflow/script/entrypoint.sh

# Atualizar pip e setuptools
RUN pip install --upgrade pip setuptools
# Instalar as dependências Python do requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

USER root
# Dar permissão de execução ao script de entrypoint
RUN chmod +x /opt/airflow/script/entrypoint.sh

USER airflow
# Definir o entrypoint para o script customizado
ENTRYPOINT ["/opt/airflow/script/entrypoint.sh"]

# Comando padrão para o webserver do Airflow
CMD ["airflow", "webserver"]
