import requests
import pandas as pd
import time
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

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

def fetch_data(symbol, max_retries=3):
    """Faz uma requisição resiliente à API da Alpha Vantage para um símbolo de ação especificado."""
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': API_KEY,
        'outputsize': 'full'  # 'full' para dados históricos completos, 'compact' para os últimos 100 dados
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP para {symbol}: {http_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"Erro de requisição para {symbol}: {req_err}")
        
        print(f"Tentativa {attempt + 1} de {max_retries} falhou. Tentando novamente...")
        time.sleep(10)  # Aguardar 10 segundos antes de tentar novamente
    
    print(f"Falha ao buscar dados para {symbol} após {max_retries} tentativas.")
    return None

def nasdaq_data():
    # Inicializar a sessão Spark
    # Configurar o Spark com o mestre 'local' e ajustar as opções necessárias
    spark = SparkSession.builder \
        .appName("AlphaVantageDataIngestion") \
        .config("spark.master", "local[*]") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()

    # Cria um DataFrame para armazenar os dados de todas as ações
    all_data = []

    for symbol in nasdaq_top_10:
        print(f"Buscando dados para {symbol}...")
        data = fetch_data(symbol)
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
            # Adicione ao DataFrame principal
            all_data.append(df)
        # Para evitar exceder o limite de chamadas de API da Alpha Vantage, adicione um atraso
        time.sleep(12)

    # Combine todos os DataFrames individuais em um só
    if all_data:
        combined_data = pd.concat(all_data)
        combined_data.index.name = 'Date'
        combined_data = combined_data.reset_index()
        
        # Converter o DataFrame do Pandas para DataFrame do Spark
        spark_df = spark.createDataFrame(combined_data)

        # Salva os dados em um formato Parquet
        spark_df.write.mode('overwrite').parquet('/opt/airflow/data/nasdaq_top_10_daily.parquet')
        print("Dados salvos em '/opt/airflow/data/nasdaq_top_10_daily.parquet'.")

    # Finalizar a sessão Spark
    spark.stop()
