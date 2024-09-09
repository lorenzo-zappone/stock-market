from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, lag, abs, lit, greatest
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

def dave_landry_analysis():
    # Inicializar a sessão Spark
    spark = SparkSession.builder.appName("DaveLandryAnalysis").getOrCreate()

    # Ler os dados do arquivo Parquet
    input_path = '/opt/airflow/data/nasdaq_top_10_daily.parquet'
    df = spark.read.parquet(input_path)

    # Ordena os dados por símbolo e data
    df = df.orderBy(col('Symbol'), col('Date'))

    # Definir o tamanho das janelas para médias móveis
    window_size_short = 21
    window_size_long = 50

    # Calcula médias móveis
    window_spec = Window.partitionBy('Symbol').orderBy('Date')
    df = df.withColumn('SMA_21', avg(col('Close')).over(window_spec.rowsBetween(-window_size_short + 1, 0)))
    df = df.withColumn('SMA_50', avg(col('Close')).over(window_spec.rowsBetween(-window_size_long + 1, 0)))

    # Calcula o retorno diário
    df = df.withColumn('Daily_Return', (col('Close') / lag('Close').over(window_spec) - 1).cast(DoubleType()))

    # Calcula o ATR (Average True Range)
    df = df.withColumn('High_Low', col('High') - col('Low'))
    df = df.withColumn('High_Close', abs(col('High') - lag('Close').over(window_spec)))
    df = df.withColumn('Low_Close', abs(col('Low') - lag('Close').over(window_spec)))
    df = df.withColumn('TR', greatest('High_Low', 'High_Close', 'Low_Close'))
    df = df.withColumn('ATR', avg('TR').over(window_spec.rowsBetween(-13, 0)))

    # Calcula o RSI
    df = df.withColumn('Price_Change', col('Close') - lag('Close').over(window_spec))
    df = df.withColumn('Gain', when(col('Price_Change') > 0, col('Price_Change')).otherwise(0))
    df = df.withColumn('Loss', when(col('Price_Change') < 0, -col('Price_Change')).otherwise(0))
    df = df.withColumn('Avg_Gain', avg('Gain').over(window_spec.rowsBetween(-13, 0)))
    df = df.withColumn('Avg_Loss', avg('Loss').over(window_spec.rowsBetween(-13, 0)))
    df = df.withColumn('RS', col('Avg_Gain') / col('Avg_Loss'))
    df = df.withColumn('RSI', (100 - (100 / (1 + col('RS')))).cast(DoubleType()))

    # Identifica sinais de compra e venda com filtros adicionais
    df = df.withColumn('Signal',
        when(
            (col('SMA_21') > col('SMA_50')) &  # Condição original
            (col('Close') > col('SMA_21')) &  # Preço acima da SMA_21
            (col('Volume') > avg(col('Volume')).over(window_spec.rowsBetween(-19, 0))) &  # Volume acima da média de 20 dias
            (col('Close') > lag('Close').over(window_spec)) &  # Preço em tendência de alta
            (col('RSI') < 70),  # RSI não sobrecomprado
            'Buy'
        )
        .when(col('SMA_21') < col('SMA_50'), 'Sell')
        .otherwise('Hold')
    )

    # Calcula o Target e Stop Loss baseado no ATR
    df = df.withColumn('Target', col('Close') + 2 * col('ATR'))
    df = df.withColumn('Stop_Loss', col('Close') - 1.5 * col('ATR'))

    # Calcula o retorno do setup (compra até venda)
    df = df.withColumn("Return", lit(None).cast(DoubleType()))

    # Lag para identificar o preço de compra no próximo sinal de venda
    df = df.withColumn("Buy_Price", lag("Close").over(window_spec))

    df = df.withColumn("Return", when(
        (col("Signal") == "Sell") & (lag("Signal").over(window_spec) == "Buy"),
        (col("Close") / col("Buy_Price") - 1)
    ).otherwise(col("Return")))

    # Seleciona as colunas relevantes para o Streamlit
    df_final = df.select('Date', 'Symbol', 'Low', 'High', 'Open', 'Close', 'SMA_21', 'SMA_50', 'Signal', 'Daily_Return', 'ATR', 'RSI', 'Target', 'Stop_Loss', 'Return')

    # Salva os resultados em um novo arquivo Parquet
    output_path = '/opt/airflow/data/dave_landry_analysis.parquet'
    df_final.write.mode('overwrite').parquet(output_path)

    # Finaliza a sessão Spark
    spark.stop()
