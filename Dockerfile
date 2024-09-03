# Use a imagem base do Airflow
FROM apache/airflow:slim-2.9.3-python3.12

# Mudar para o usuário root para instalar dependências do sistema
USER root

# Instalar dependências necessárias para construir pacotes Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    python3-dev \
    build-essential \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Configurar as variáveis de ambiente do Java no Dockerfile
ENV JAVA_HOME="/usr/lib/jvm/default-java"
ENV PATH="$JAVA_HOME/bin:$PATH"

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
RUN mkdir -p /opt/airflow/logs && chown -R airflow:root /opt/airflow/logs

USER airflow
# Definir o entrypoint para o script customizado
ENTRYPOINT ["/opt/airflow/script/entrypoint.sh"]

# O CMD é definido no entrypoint com base no serviço (webserver ou scheduler)