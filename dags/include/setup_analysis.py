from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from pyspark.sql.window import Window

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
    df = df.withColumn('SMA_21', avg(col('Close')).over(Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-window_size_short + 1, 0)))
    df = df.withColumn('SMA_50', avg(col('Close')).over(Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-window_size_long + 1, 0)))

    # Identifica sinais de compra e venda
    df = df.withColumn('Signal', 
                       when(col('SMA_21') > col('SMA_50'), 'Buy')
                       .when(col('SMA_21') < col('SMA_50'), 'Sell')
                       .otherwise('Hold'))

    # Exibir os sinais para verificação (opcional)
    df.select('Date', 'Symbol', 'Close', 'SMA_21', 'SMA_50', 'Signal').show(100, truncate=False)

    # Salva os resultados em um novo arquivo Parquet
    output_path = '/opt/airflow/data/dave_landry_analysis.parquet'
    df.write.mode('overwrite').parquet(output_path)

    # Finaliza a sessão Spark
    spark.stop()