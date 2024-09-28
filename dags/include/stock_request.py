import aiohttp
import asyncio
import pandas as pd
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
import logging

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Adicionar logging para a saída padrão (console)
    ]
)

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Obtenha a chave da API a partir das variáveis de ambiente
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

# Verifique se a chave da API foi carregada corretamente
if not API_KEY:
    raise ValueError("API Key não encontrada. Por favor, verifique o arquivo .env.")

# Lista das 10 maiores ações da NASDAQ (ajuste conforme necessário)
nasdaq_top_10 = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'SPOT', 'NFLX', 'NU']

# URL base da API da Alpha Vantage para a função TIME_SERIES_DAILY
BASE_URL = 'https://www.alphavantage.co/query'

async def fetch_data(session, symbol, max_retries=3):
    """Faz uma requisição resiliente e assíncrona à API da Alpha Vantage para um símbolo de ação especificado."""
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': API_KEY,
        'outputsize': 'full'  # 'full' para dados históricos completos, 'compact' para os últimos 100 dados
    }

    for attempt in range(max_retries):
        try:
            async with session.get(BASE_URL, params=params) as response:
                response.raise_for_status()
                logging.info(f"Sucesso na requisição para {symbol}")
                return await response.json()
        except aiohttp.ClientError as e:
            logging.error(f"Falha na requisição para {symbol}: {e}")

        logging.warning(f"Tentativa {attempt + 1} de {max_retries} falhou para {symbol}. Tentando novamente...")
        await asyncio.sleep(10)  # Aguardar 10 segundos antes de tentar novamente

    logging.error(f"Falha ao buscar dados para {symbol} após {max_retries} tentativas.")
    return None

async def fetch_all_data(symbols):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for symbol in symbols:
            tasks.append(fetch_data(session, symbol))
        # Executa todas as requisições assíncronas de forma concorrente
        return await asyncio.gather(*tasks)

def nasdaq_data():
    # Inicializar a sessão Spark
    spark = SparkSession.builder \
        .appName("AlphaVantageDataIngestion") \
        .config("spark.master", "local[*]") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()

    logging.info("Sessão Spark inicializada.")

    # Obter dados das ações de forma assíncrona
    all_data = asyncio.run(fetch_all_data(nasdaq_top_10))

    # Cria um DataFrame para armazenar os dados de todas as ações
    processed_data = []

    for data, symbol in zip(all_data, nasdaq_top_10):
        if data:
            # Extraia os dados de preço diário
            time_series = data.get('Time Series (Daily)', {})
            # Transforme em DataFrame do Pandas
            df = pd.DataFrame.from_dict(time_series, orient='index')
            df = df.rename(columns={
                '1. open': 'Open',
                '2. high': 'High',
                '3. low': 'Low',
                '4. close': 'Close',
                '5. volume': 'Volume'
            })
            df['Symbol'] = symbol
            processed_data.append(df)
            logging.info(f"Dados processados para {symbol}.")

    # Combine todos os DataFrames individuais em um só
    if processed_data:
        combined_data = pd.concat(processed_data)
        combined_data.index.name = 'Date'
        combined_data = combined_data.reset_index()

        # Converter o DataFrame do Pandas para DataFrame do Spark
        spark_df = spark.createDataFrame(combined_data)

        # Salva os dados em um formato Parquet
        output_path = '/opt/airflow/data/nasdaq_top_10_daily.parquet'
        spark_df.write.mode('overwrite').parquet(output_path)
        logging.info(f"Dados salvos em '{output_path}'.")

    # Finalizar a sessão Spark
    spark.stop()
    logging.info("Sessão Spark finalizada.")

if __name__ == "__main__":
    nasdaq_data()
