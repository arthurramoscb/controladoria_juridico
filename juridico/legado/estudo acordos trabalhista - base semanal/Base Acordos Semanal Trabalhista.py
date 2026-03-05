# Databricks notebook source
# MAGIC %md
# MAGIC #Base de Acordos Semanal Trabalhista

# COMMAND ----------

#importação das bibliotecas para a geração da base semanal de acordos trabalhista
from pyspark.sql.functions import col, when, regexp_replace, translate, trim
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType, DecimalType
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ###1- Importação da Base Semanal de Acordos Trabalhista

# COMMAND ----------

arquivo_semana = dbutils.widgets.get("arquivo_semana")

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re
spark.conf.set("spark.sql.caseSensitive","true")

# Importação da Base com negociação semanal de acordos do Trabalhista

base_acordos = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'MATRIZ'!A1:CD900000") \
   .load(f"/Volumes/databox/juridico_comum/arquivos/trabalhista/base_semanal_acordos/{arquivo_semana} - REPORTE SEMANAL.xlsx")

# Get the column names
columns = base_acordos.columns

# Count the occurrences of each column name
column_counts = Counter(columns)

# Create a list to store unique column names
unique_columns = []

# Iterate over column names and their counts
for column, count in column_counts.items():
    if count > 1:
        # If the column name appears more than once, rename the duplicates
        for i in range(count):
            # Replace spaces and special characters with underscores
            cleaned_column = re.sub(r'[\s\W]', '_', column)
            old_name = concat_ws("_", cleaned_column, str(i))
            new_name = concat_ws("_", cleaned_column, str(i+1)) if i > 0 else cleaned_column
            base_acordos = base_acordos.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)


# COMMAND ----------

# MAGIC %md
# MAGIC ###2-Tratamento Dados Geração da Base de Acordos

# COMMAND ----------

# Substituir vírgulas por pontos
base_acordos_1 = base_acordos.withColumn('VALOR DO ACORDO FECHADO', regexp_replace('VALOR DO ACORDO FECHADO', ',', '.'))

# Substituir pontos por vírgulas, exceto o último ponto (para evitar confusão com o separador decimal)
base_acordos_1 = base_acordos_1.withColumn('VALOR DO ACORDO FECHADO', regexp_replace('VALOR DO ACORDO FECHADO', r'(?<=\d)\.(?=\d{3}(?:,|$))', ','))
# Substituir o último ponto por vírgula
base_acordos_3 = base_acordos_1.withColumn('VALOR DO ACORDO FECHADO', regexp_replace('VALOR DO ACORDO FECHADO', r'\.(?=\d+$)', ','))

# Substituir '-' por 0
base_acordos_4 = base_acordos_3.withColumn('VALOR DO ACORDO FECHADO', regexp_replace('VALOR DO ACORDO FECHADO', '-', '0'))

base_acordos_5 = base_acordos_4.withColumn("VALOR DO ACORDO FECHADO", regexp_replace(col("VALOR DO ACORDO FECHADO"), "R\$", ""))
base_acordos_5 = base_acordos_5.withColumn("VALOR DO ACORDO FECHADO", trim(col("VALOR DO ACORDO FECHADO")))

base_acordos_6 = base_acordos_5.withColumn("VALOR DO ACORDO FECHADO", regexp_replace(col("VALOR DO ACORDO FECHADO"), "\\.", ""))
base_acordos_6 = base_acordos_6.withColumn("VALOR DO ACORDO FECHADO", regexp_replace(col("VALOR DO ACORDO FECHADO"), ",", "."))
base_acordos_6 = base_acordos_6.withColumn("VALOR DO ACORDO FECHADO", base_acordos_6["VALOR DO ACORDO FECHADO"].cast(DoubleType()))
base_acordos_6 = base_acordos_6.withColumnRenamed("ID.", "ID_PROCESSO")

# COMMAND ----------

from pyspark.sql.functions import col, format_number

base_acordos_7 = (
  base_acordos_6
  .withColumn('DATA ACORDO', col('DATA ACORDO').cast('date'))

  #.withColumn('Valor de Risco', format_number(col('CONSOLIDADO Valor de Risco'), 2).cast('string'))
  #.withColumn('Valor de Risco', col('Valor de Risco').cast('double'))
  #.withColumn('Alçada 1', regexp_replace('Alçada 1', ',', '.').cast('double'))
  #.withColumn('Alçada 2', regexp_replace('Alçada 2', ',', '.').cast('double'))
  #.withColumn('Alçada 3', regexp_replace('Alçada 3', ',', '.').cast('double'))
  #.withColumn('VALOR DO ACORDO FECHADO', regexp_replace('VALOR DO ACORDO FECHADO', ',', '.').cast('double'))
  .withColumn('QUANTIDADE DE PARCELAS', col('QUANTIDADE DE PARCELAS').cast('int'))
  .select(
    'ID_PROCESSO',
    'Alçada 1',
    'Alçada 2',
    'Alçada 3',
    'CONSOLIDADO Valor de Risco',
    'CONSOLIDADO PROVISÃO/CALCULISTA',
    'VALOR DO ACORDO FECHADO',
    'STATUS',
    'SUBSTATUS',
    'A VISTA/PARCELADO',
    'QUANTIDADE DE PARCELAS',
    'DATA ACORDO',
    'STATUS ELAW',
    'SEMANA (BOLETIM)',
    'ANO FECHAMENTO',
    'A VISTA/PARCELADO'
  )
)


# COMMAND ----------

from pyspark.sql.functions import concat_ws, lpad, year, month, to_date, regexp_extract

base_acordos_8 = base_acordos_7.withColumn('MES_DATA_ACORDO', concat_ws('/', lpad(month('DATA ACORDO'), 2, '0'), year('DATA ACORDO')))
base_acordos_8 = base_acordos_8.withColumn('MES_DATA_ACORDO', to_date('MES_DATA_ACORDO', 'MM/yyyy'))


base_acordos_8 = base_acordos_8.withColumn('QTD_ACORDO', when(base_acordos_8['A VISTA/PARCELADO'] != "", 1).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC ###3- Inclusão do campo com a faixa de valor que o acordo se enquadra

# COMMAND ----------

# Criando faixas para o valor do acordo
base_acordos_final = base_acordos_8.withColumn(
    'FAIXA',
    when((col('VALOR DO ACORDO FECHADO') > 1) & (col('VALOR DO ACORDO FECHADO') <= 50000), 'Até R$ 50 mil')
    .when((col('VALOR DO ACORDO FECHADO') > 50000) & (col('VALOR DO ACORDO FECHADO') <= 100000), 'R$ 50 - 100 mil')
    .when((col('VALOR DO ACORDO FECHADO') > 100000) & (col('VALOR DO ACORDO FECHADO') <= 200000), 'R$ 100 - 200 mil')
    .when((col('VALOR DO ACORDO FECHADO') > 200000) & (col('VALOR DO ACORDO FECHADO') <= 500000), 'R$ 200 - 500 mil')
    .when(col('VALOR DO ACORDO FECHADO') > 500000, 'Acima R$ 500 mil')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###4- Cria os campos adicionais para controle do histórico de acordos

# COMMAND ----------

#colunas adicionais para o indicador semanal

from pyspark.sql.functions import lit, col

base_acordos_final = base_acordos_final.withColumn('SEMANA_NUMERO', lit('')).select(
    *[col(c) for c in base_acordos_final.columns[:base_acordos_final.columns.index('DATA ACORDO')+1]] +
    [lit('').alias('SEMANA_NUMERO')] +
    [col(c) for c in base_acordos_final.columns[base_acordos_final.columns.index('DATA ACORDO')+1:]]

)
base_acordos_final = base_acordos_final.withColumn('TOTAL_APTO_MES', lit('')).select( *[col(c) for c in base_acordos_final.columns[:base_acordos_final.columns.index('SEMANA (BOLETIM)')+1]] + [lit('').alias('TOTAL_APTO_MES')] + [col(c) for c in base_acordos_final.columns[base_acordos_final.columns.index('SEMANA (BOLETIM)')+1:]] )

# COMMAND ----------

# MAGIC %md
# MAGIC ###5- Cria a Base Final delimitada com os campos do Report

# COMMAND ----------

base_acordos_final_f= (
  base_acordos_final
  
  .select(
    'ID_PROCESSO',
    'STATUS ELAW',
    'CONSOLIDADO Valor de Risco',
    'CONSOLIDADO PROVISÃO/CALCULISTA',
    'Alçada 1',
    'Alçada 2',
    'Alçada 3',
    'STATUS',
    'SUBSTATUS',
    'VALOR DO ACORDO FECHADO',
    'FAIXA',
    'A VISTA/PARCELADO',
    'QUANTIDADE DE PARCELAS',
    'DATA ACORDO',
    'SEMANA_NUMERO',
    'MES_DATA_ACORDO',
    'SEMANA (BOLETIM)',
    'TOTAL_APTO_MES',
    'QTD_ACORDO',

  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Exportação da Base Final com Indicadores de Acordos Trabalhista

# COMMAND ----------

#Salvar Dataframe em formato xlsx no Databox


import pandas as pd
from shutil import copyfile

# Converter o Dataframe Spark para Pandas 
base_acordos_final_ff = base_acordos_final_f.toPandas()

# Define a saída do arquivo xlsx
output_filename = "/local_disk0/tmp/base_acordos_final.xlsx"

#Tratamento Pandas para salvar o arquivo
base_acordos_final_ff.to_excel(output_filename, index=False, engine="xlsxwriter")


# Obter o valor do widget 'arquivo_data'
arquivo_data = dbutils.widgets.get("arquivo_data")

# Gravar o arquivo XLSX no Databox
destination_filename = f"/Volumes/databox/juridico_comum/arquivos/trabalhista/base_semanal_acordos/base_acordos_{arquivo_data}.xlsx"
copyfile(output_filename, destination_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tópico para validação da base final

# COMMAND ----------

display(base_acordos_final_ff)
