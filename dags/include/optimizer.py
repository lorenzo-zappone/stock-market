import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lag, abs, lit, greatest, sum, count, first, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import time

# Configurar o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def optimize_atr_multipliers():
    start_time = time.time()
    logger.info("Iniciando o processo de otimização de multiplicadores ATR")

    # Inicializar a sessão Spark
    spark = SparkSession.builder.appName("ATRMultiplierOptimization").getOrCreate()
    logger.info("Sessão Spark inicializada")

    # Ler os dados do arquivo Parquet
    input_path = '/opt/airflow/data/dave_landry_analysis.parquet'
    logger.info(f"Lendo dados do arquivo: {input_path}")
    df = spark.read.parquet(input_path)
    logger.info(f"Dados lidos. Número de linhas: {df.count()}")

    # Definir uma lista de multiplicadores para testar
    target_multipliers = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5, 10.0]
    stop_loss_multipliers = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    logger.info(f"Testando combinações de multiplicadores: Targets {target_multipliers}, Stop Loss {stop_loss_multipliers}")

    # Função para calcular o retorno para uma combinação de multiplicadores
    def calculate_return(df, target_mult, stop_loss_mult):
        window_spec = Window.partitionBy('Symbol').orderBy('Date')
        
        # Calcular Target e Stop Loss
        df = df.withColumn('Target', col('Close') * (1 + target_mult * col('ATR') / col('Close')))
        df = df.withColumn('Stop_Loss', col('Close') * (1 - stop_loss_mult * col('ATR') / col('Close')))
        
        # Identificar sinais de compra
        df = df.withColumn('Entry_Price', when(col('Signal') == 'Buy', col('Close')))
        df = df.withColumn('Entry_Price', first('Entry_Price', ignorenulls=True).over(window_spec))

        # Adicionar uma coluna para o retorno inicializada com 0.0
        df = df.withColumn('Return', lit(0.0))
        
        # Usar uma janela que vai da linha atual para todas as futuras linhas
        forward_window = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(0, Window.unboundedFollowing)
        
        # Iterar sobre os preços de entrada para calcular o retorno baseado no target e stop loss
        df = df.withColumn('Return', 
                           when((col('High') >= col('Target')), (col('Target') / col('Entry_Price') - 1))
                           .when((col('Low') <= col('Stop_Loss')), (col('Stop_Loss') / col('Entry_Price') - 1))
                           .otherwise((col('Close') / col('Entry_Price') - 1))
                          )
        
        # Ajustar retornos apenas para trades fechados (target ou stop_loss atingidos)
        df = df.withColumn('Return', 
                           when((col('High') >= col('Target')) | (col('Low') <= col('Stop_Loss')), col('Return'))
                           .otherwise(0.0))
        
        # Agregar os resultados
        result = df.groupBy('Symbol').agg(
            sum('Return').alias('Total_Return'),
            count(when(col('Return') != 0, True)).alias('Trade_Count')
        )
        
        return result

    # Testar todas as combinações de multiplicadores
    results = []
    total_combinations = len(target_multipliers) * len(stop_loss_multipliers)
    current_combination = 0

    for target_mult in target_multipliers:
        for stop_loss_mult in stop_loss_multipliers:
            current_combination += 1
            logger.info(f"Processando combinação {current_combination} de {total_combinations}")
            
            returns = calculate_return(df, target_mult, stop_loss_mult)
            
            total_return = returns.withColumn('Target_Mult', lit(target_mult)) \
                                  .withColumn('Stop_Loss_Mult', lit(stop_loss_mult))
            
            results.append(total_return)

    logger.info("Combinando todos os resultados")
    # Combinar todos os resultados
    final_results = results[0]
    for df in results[1:]:
        final_results = final_results.union(df)

    logger.info("Encontrando os melhores multiplicadores para cada símbolo")
    # Encontrar os melhores multiplicadores para cada símbolo
    window_spec = Window.partitionBy('Symbol').orderBy(col('Total_Return').desc())
    best_multipliers = final_results.withColumn('rank', row_number().over(window_spec)) \
                                    .filter(col('rank') == 1) \
                                    .select('Symbol', 'Target_Mult', 'Stop_Loss_Mult', 'Total_Return', 'Trade_Count')

    # Salvar os resultados em um novo arquivo Parquet
    output_path = '/opt/airflow/data/best_atr_multipliers.parquet'
    logger.info(f"Salvando resultados em: {output_path}")
    best_multipliers.write.mode('overwrite').parquet(output_path)

    # Finaliza a sessão Spark
    spark.stop()
    logger.info("Sessão Spark finalizada")

    end_time = time.time()
    logger.info(f"Processo concluído em {end_time - start_time:.2f} segundos")

if __name__ == "__main__":
    optimize_atr_multipliers()
