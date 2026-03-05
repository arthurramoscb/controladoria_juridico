# Databricks notebook source
pip install openpyxl

# COMMAND ----------

# Padronizar o código, caso o SparkSession seja ativado
#dbutils.library.restartPython()

# COMMAND ----------

import pyspark.pandas as ps

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re

spark.conf.set("spark.sql.caseSensitive","true")

# Load Excel file
civel_fechamento_202403 = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'Base'!A2:CW200000") \
    .load('/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/civel_base_financeiro/21.03.2024 Fechamento Cível.xlsx')

# Get the column names
columns = civel_fechamento_202403.columns

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
            civel_fechamento_202403 = civel_fechamento_202403.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)

# Display the new column names
print("New column names:", civel_fechamento_202403.columns)


# COMMAND ----------

#tratamento das variáveis data timestamp da base gerencial Trabalhista

#função para tratamento simultâneo de todas as variáveis data em formato timestamp
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import TimestampType

def convert_timestamp_to_datetime(df):
    for column in df.columns:
        column_type = df.schema[column].dataType
        if isinstance(column_type, TimestampType):
            df = df.withColumn(column, to_date(col(column)))
    return df

# Exemplo de uso:
civel_fechamento_202403 = convert_timestamp_to_datetime(civel_fechamento_202403)

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import DateType

# Adicionar uma coluna MES_FECH com o valor 2024-03-01 em formato DateType
civel_fechamento_202403 = civel_fechamento_202403.withColumn("MES_FECH", lit("2024-03-01").cast(DateType()))
civel_fechamento_202403 = civel_fechamento_202403.withColumn("ID PROCESSO", col("ID PROCESSO").cast("integer"))

# Exibir o DataFrame resultante


# COMMAND ----------

civel_fechamento_202403.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Agregação Dados Base Fechamento Financeiro com a Base de Pagamentos e Garantias

# COMMAND ----------

hist_pagamentos_garantia_civel = spark.table("databox.juridico_comum.hist_pagamentos_garantias_f")

# COMMAND ----------


from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType

# Corrija os campos com padrão de data e valor da causa
tb_fechamento_civel_202403_2 = civel_fechamento_202403 \
    .where(col("ID PROCESSO").isNotNull()) \
    

# COMMAND ----------

tb_fechamento_civel_202403_2.count()

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Realizando a junção
tb_fechamento_civel_202403_3 = tb_fechamento_civel_202403_2.join(hist_pagamentos_garantia_civel,
                                                                 tb_fechamento_civel_202403_2["ID_PROCESSO"] == hist_pagamentos_garantia_civel["PROCESSO_ID"], "left")

# Filtrando e selecionando os dados
tb_fechamento_civel_202403_3 = tb_fechamento_civel_202403_3 \
    .select(
        col("ID_PROCESSO"),
        col("MES_FECH"),
        col("ACORDO").alias("ACORDO"),
        col("CONDENACAO").alias("CONDENACAO"),
        col("PENHORA").alias("PENHORA"),
        col("GARANTIAS").alias("GARANTIAS"),
        col("IMPOSTO").alias("IMPOSTO"),
        col("OUTROS_PAGAMENTOS").alias("OUTROS_PAGAMENTOS"),
        col("ENCERRADOS"),
        (col("ACORDO") + col("CONDENACAO") + col("PENHORA") + col("GARANTIAS") + col("IMPOSTO") + col("OUTROS_PAGAMENTOS")).alias("TOTAL_PAGAMENTOS"),
        col("DATA_EFETIVA_PAGAMENTO").alias("DT_ULT_PGTO"),
        col("DATA_EFETIVA_PAGAMENTO").alias("MES_CONTABIL")
    ) \
    .filter(col("ID_PROCESSO").isNotNull() & (col("ENCERRADOS") == '1'))



# COMMAND ----------

tb_fechamento_civel_202403_3.count()

# COMMAND ----------

from pyspark.sql import functions as F

tb_fechamento_civel_202403_4 = tb_fechamento_civel_202403_3 \
    .groupBy("ID_PROCESSO", "MES_FECH", "ENCERRADOS") \
    .agg(
        F.max("MES_CONTABIL").alias("DT_ULT_PGTO").cast("date").alias("DT_ULT_PGTO"),
        F.sum("ACORDO").alias("ACORDOS").cast("decimal(18,2)").alias("ACORDOS"),
        F.sum("CONDENACAO").alias("CONDENAÇÃO").cast("decimal(18,2)").alias("CONDENAÇÃO"),
        F.sum("PENHORA").alias("PENHORA").cast("decimal(18,2)").alias("PENHORA"),
        F.sum("GARANTIAS").alias("GARANTIA").cast("decimal(18,2)").alias("GARANTIA"),
        F.sum("IMPOSTO").alias("IMPOSTO").cast("decimal(18,2)").alias("IMPOSTO"),
        F.sum("OUTROS_PAGAMENTOS").alias("OUTROS_PAGAMENTOS").cast("decimal(18,2)").alias("OUTROS_PAGAMENTOS")
    )




# COMMAND ----------

tb_fechamento_civel_202403_2 = tb_fechamento_civel_202403_2.withColumnRenamed("ID PROCESSO", "ID_PROCESSO")

# COMMAND ----------

tb_fechamento_civel_202403_4 = tb_fechamento_civel_202403_4.withColumnRenamed("ID PROCESSO", "ID_PROCESSO")


# COMMAND ----------

tb_fechamento_civel_202403_4.createOrReplaceTempView("FECHAMENTO_CIVEL_4")
tb_fechamento_civel_202403_2.createOrReplaceTempView("FECHAMENTO_CIVEL_2")

tb_fechamento_civel_202403 = spark.sql(f"""
SELECT A.*,
       B.DT_ULT_PGTO,
       B.ACORDOS,
       B.`CONDENAÇÃO`,
       B.PENHORA,
       B.GARANTIA,
       B.IMPOSTO,
       B.OUTROS_PAGAMENTOS
FROM FECHAMENTO_CIVEL_2 A
LEFT JOIN FECHAMENTO_CIVEL_4 B
ON A.ID_PROCESSO = B.ID_PROCESSO
   AND A.MES_FECH = B.MES_FECH
   AND A.ENCERRADOS = B.ENCERRADOS
ORDER BY ID_PROCESSO
""")

# COMMAND ----------

display(tb_fechamento_civel_202403)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformação arquivos de Fechamento Financeiro em Tabela Delta

# COMMAND ----------

import os
import re
import pandas as pd

# Caminho para a pasta que contém os arquivos XLSX
folder_path = "/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/civel_base_financeiro_tratada/"

# Função para obter o nome da tabela sem a extensão
def get_table_name(file_name):
    return os.path.splitext(file_name)[0]

# Função para limpar os nomes das colunas
def clean_column_names(columns):
    cleaned_columns = []
    for col in columns:
        # Substitui caracteres inválidos por underscore
        cleaned_col = re.sub(r'[ ,;{}()\n\t=]', '_', col)
        cleaned_columns.append(cleaned_col)
    return cleaned_columns

# Iterar sobre todos os arquivos na pasta
for file_name in os.listdir(folder_path):
    if file_name.endswith(".xlsx"):
        file_path = os.path.join(folder_path, file_name)
        
        # Carregar o arquivo Excel em um DataFrame do pandas
        pdf = pd.read_excel(file_path)
        
        # Limpar os nomes das colunas
        pdf.columns = clean_column_names(pdf.columns)
        
        # Converter o DataFrame do pandas para um DataFrame do Spark
        sdf = spark.createDataFrame(pdf)
        
        # Nome da Delta Table
        table_name = get_table_name(file_name)
        
        # Nome completo da tabela no formato "database_name.table_name"
        full_table_name = f"databox.juridico_comum.{table_name}"
        
        # Escrever o DataFrame como uma Delta Table
        sdf.write.format("delta").mode("overwrite").saveAsTable(full_table_name)




# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from databox.juridico_comum.tb_fechamento_civel_202204

# COMMAND ----------

import os
import re
import pandas as pd


# Caminho para o arquivo XLSX
file_path = "/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/civel_base_financeiro_tratada/TB_FECHAMENTO_CIVEL_202112.xlsx"

# Função para obter o nome da tabela sem a extensão
def get_table_name(file_name):
    return os.path.splitext(file_name)[0]

# Função para limpar os nomes das colunas
def clean_column_names(columns):
    cleaned_columns = []
    for col in columns:
        # Substitui caracteres inválidos por underscore
        cleaned_col = re.sub(r'[ ,;{}()\n\t=]', '_', col)
        cleaned_columns.append(cleaned_col)
    return cleaned_columns

# Carregar o arquivo Excel em um DataFrame do pandas
pdf = pd.read_excel(file_path)

# Limpar os nomes das colunas
pdf.columns = clean_column_names(pdf.columns)

# Converter o DataFrame do pandas para um DataFrame do Spark
sdf = spark.createDataFrame(pdf)

# Nome da Delta Table
file_name = os.path.basename(file_path)
table_name = get_table_name(file_name)

# Nome completo da tabela no formato "database_name.table_name"
full_table_name = f"databox.juridico_comum.{table_name}"

# Escrever o DataFrame como uma Delta Table
sdf.write.format("delta").mode("overwrite").saveAsTable(full_table_name)



# COMMAND ----------


