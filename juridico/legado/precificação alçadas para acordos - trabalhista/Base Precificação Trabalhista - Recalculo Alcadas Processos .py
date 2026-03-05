# Databricks notebook source
# MAGIC %md
# MAGIC ###Processo para recalculo alçadas de precificação

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re
spark.conf.set("spark.sql.caseSensitive","true")

# Importação da Base do Calculista de Pedidos
df_base_elegivel = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'BASE_ELEGIVEL'!A1:GB30000") \
    .load("/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_precificacao/base_precificacao_setembro_2024/BASE_APTA_SETEMBRO_2024_V2.xlsx")

# Get the column names
columns = df_base_elegivel.columns

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
            df_base_elegivel = df_base_elegivel.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)

# COMMAND ----------

#Exclui as colunas com as alçadas para efetuar o recalculo
df_base_elegivel_1 = df_base_elegivel.drop("ALCADA_1", "ALCADA_2", "ALCADA_3")


# COMMAND ----------


from pyspark.sql.functions import when, col

# Step 3: Apply the third transformation
trabalhista_gerencial_risco5 = df_base_elegivel_1.withColumn("ALCADA_1",
                                             when((col("FASE_AJUSTADA") == "CONHECIMENTO"), col("VALOR_DE_RISCO1") * 0.65)
                                             .otherwise(when((col("FASE") == "RECURSAL TRT") & (col("VALOR_DE_RISCO1") < 100000), col("VALOR_DE_RISCO1") * 0.55)
                                             .otherwise(when((col("FASE") == "RECURSAL TST") & (col("VALOR_DE_RISCO1") < 100000), col("VALOR_DE_RISCO1") * 0.70)
                                             .otherwise(when((col("FASE") == "RECURSAL TRT") & (col("VALOR_DE_RISCO1") > 100000), col("VALOR_DE_RISCO1") * 0.50)
                                             .otherwise(when((col("FASE") == "RECURSAL TST") & (col("VALOR_DE_RISCO1") > 100000), col("VALOR_DE_RISCO1") * 0.60)
                                             .otherwise(when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("CALCULO_UTILIZADO") == "EXECUÇÃO HOMOLOGADA"), col("VALOR_DE_RISCO1") * 0.85)
                                             .otherwise(col("VALOR_DE_RISCO1") * 0.80)))))))

# Step 4: Apply the fourth transformation
trabalhista_gerencial_risco6 = trabalhista_gerencial_risco5.withColumn("ALCADA_2",
                                             when((col("FASE_AJUSTADA") == "CONHECIMENTO"), col("VALOR_DE_RISCO1") * 0.75)
                                             .otherwise(when((col("FASE") == "RECURSAL TRT") & (col("VALOR_DE_RISCO1") < 100000), col("VALOR_DE_RISCO1") * 0.65)
                                             .otherwise(when((col("FASE") == "RECURSAL TST") & (col("VALOR_DE_RISCO1") < 100000), col("VALOR_DE_RISCO1") * 0.80)
                                             .otherwise(when((col("FASE") == "RECURSAL TRT") & (col("VALOR_DE_RISCO1") > 100000), col("VALOR_DE_RISCO1") * 0.60)
                                             .otherwise(when((col("FASE") == "RECURSAL TST") & (col("VALOR_DE_RISCO1") > 100000), col("VALOR_DE_RISCO1") * 0.70)
                                             .otherwise(when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("CALCULO_UTILIZADO") == "EXECUÇÃO HOMOLOGADA"), col("VALOR_DE_RISCO1") * 0.90)
                                             .otherwise(col("VALOR_DE_RISCO1") * 0.85)))))))

# Step 5: Apply the fifth transformation
df_base_elegivel_final= trabalhista_gerencial_risco6.withColumn("ALCADA_3",
                                             when((col("FASE_AJUSTADA") == "CONHECIMENTO"), col("VALOR_DE_RISCO1") * 0.85)
                                             .otherwise(when((col("FASE") == "RECURSAL TRT") & (col("VALOR_DE_RISCO1") < 100000), col("VALOR_DE_RISCO1") * 0.75)
                                             .otherwise(when((col("FASE") == "RECURSAL TST") & (col("VALOR_DE_RISCO1") < 100000), col("VALOR_DE_RISCO1") * 0.90)
                                             .otherwise(when((col("FASE") == "RECURSAL TRT") & (col("VALOR_DE_RISCO1") > 100000), col("VALOR_DE_RISCO1") * 0.70)
                                             .otherwise(when((col("FASE") == "RECURSAL TST") & (col("VALOR_DE_RISCO1") > 100000), col("VALOR_DE_RISCO1") * 0.80)
                                             .otherwise(when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("CALCULO_UTILIZADO") == "EXECUÇÃO HOMOLOGADA"), col("VALOR_DE_RISCO1") * 0.95)
                                             .otherwise(col("VALOR_DE_RISCO1") * 0.90)))))))

# COMMAND ----------

display(df_base_elegivel_final)

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# Assuming 'your_date_column' is the name of the column causing the issue
# Convert the datetime columns to string
df_base_elegivel_final = df_base_elegivel_final.withColumn(
    "DT_ACORDAO_TRT", 
    col("DT_ACORDAO_TRT").cast("string")
)



# COMMAND ----------

#Salvar arquivo final na pasra correspondente ao mês da base de precificação gerada
#Disponibilizar arquivo na pasta do mês para importação

import pandas as pd
from shutil import copyfile

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df_base_elegivel_final .toPandas()

# Save the Pandas DataFrame to an Excel file
local_path = f'/local_disk0/tmp/BASE-ELEGIVEL_RECALCULO.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='BASE_ELEGIVEL', engine='xlsxwriter')

# Copy the file from the local disk to the desired volume
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/BASE_ELEGIVEL_RECALCULO_ALCADAS.xlsx'

copyfile(local_path, volume_path)
