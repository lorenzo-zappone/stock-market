import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lag, abs, lit, greatest, sum, count, last, row_number, first
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
    def calculate_return(target_mult, stop_loss_mult):
        logger.info(f"Calculando retornos para Target Mult: {target_mult}, Stop Loss Mult: {stop_loss_mult}")
        window_spec = Window.partitionBy('Symbol').orderBy('Date')
        
        df_calc = df.withColumn('Target', col('Close') + target_mult * col('ATR'))
        df_calc = df_calc.withColumn('Stop_Loss', col('Close') - stop_loss_mult * col('ATR'))

        # Identificar blocos contínuos de sinais de compra e venda
        df_calc = df_calc.withColumn('Signal_Change', when(col('Signal') != lag(col('Signal'), 1).over(window_spec), col('Signal')))
        df_calc = df_calc.withColumn('Signal_Block', first(col('Signal_Change'), ignorenulls=True).over(Window.partitionBy('Symbol').orderBy('Date').rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        
        # Marcar o início e o fim de blocos de compra e venda
        df_calc = df_calc.withColumn('Block_Type', when(col('Signal_Block') == 'Buy', 'Start').when(col('Signal_Block') == 'Sell', 'End').otherwise(None))
        df_calc = df_calc.withColumn('Block_Num', sum(when(col('Block_Type') == 'Start', 1).otherwise(0)).over(Window.partitionBy('Symbol').orderBy('Date')))
        
        # Manter o primeiro sinal de compra e o último sinal de venda em cada bloco
        df_calc = df_calc.withColumn('Buy_Price', when(col('Signal') == 'Buy', col('Close')).otherwise(None))
        df_calc = df_calc.withColumn('Buy_Price', first(col('Buy_Price'), ignorenulls=True).over(Window.partitionBy('Symbol', 'Block_Num').orderBy('Date')))
        
        df_calc = df_calc.withColumn('Sell_Price', when(col('Signal') == 'Sell', col('Close')).otherwise(None))
        df_calc = df_calc.withColumn('Sell_Price', last(col('Sell_Price'), ignorenulls=True).over(Window.partitionBy('Symbol', 'Block_Num').orderBy('Date')))
        
        df_calc = df_calc.withColumn('Return', 
            when((col('High') >= col('Target')) & (col('Buy_Price').isNotNull()) & (col('Sell_Price').isNotNull()), 
                 (col('Sell_Price') / col('Buy_Price') - 1))
            .when((col('Low') <= col('Stop_Loss')) & (col('Buy_Price').isNotNull()), 
                  (col('Stop_Loss') / col('Buy_Price') - 1))
            .when((col('Signal') == 'Sell') & (col('Buy_Price').isNotNull()) & (col('Sell_Price').isNull()), 
                  (col('Close') / col('Buy_Price') - 1))
            .otherwise(0))
        
        return df_calc.select('Symbol', 'Return')

    # Testar todas as combinações de multiplicadores
    results = []
    total_combinations = len(target_multipliers) * len(stop_loss_multipliers)
    current_combination = 0
    
    for target_mult in target_multipliers:
        for stop_loss_mult in stop_loss_multipliers:
            current_combination += 1
            logger.info(f"Processando combinação {current_combination} de {total_combinations}")
            
            returns = calculate_return(target_mult, stop_loss_mult)
            
            total_return = returns.groupBy('Symbol').agg(
                sum('Return').alias('Total_Return'),
                count(when(col('Return') != 0, True)).alias('Trade_Count')
            )
            
            results.append(total_return.withColumn('Target_Mult', lit(target_mult))
                                       .withColumn('Stop_Loss_Mult', lit(stop_loss_mult)))

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
