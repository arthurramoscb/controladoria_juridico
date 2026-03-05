# Databricks notebook source
pip install openpyxl

# COMMAND ----------

# Padronizar o código, caso o SparkSession seja ativado
dbutils.library.restartPython()

# COMMAND ----------

import pyspark.pandas as ps

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re

spark.conf.set("spark.sql.caseSensitive","true")


# Load Excel file
civel_fechamento_202404 = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'Base'!A2:CW200000") \
    .load("/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/civel_base_financeiro/22.04.2024 Fechamento Cível.xlsx")

# Get the column names
columns = civel_fechamento_202404.columns

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
            civel_fechamento_202404 = civel_fechamento_202404.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)

# Display the new column names
print("New column names:", civel_fechamento_202404.columns)


# COMMAND ----------

display(civel_fechamento_202404)

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
civel_fechamento_202404 = convert_timestamp_to_datetime(civel_fechamento_202404)

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.types import DateType

# Adicionar uma coluna MES_FECH com o valor 2024-04-01 em formato DateType
civel_fechamento_202404 = civel_fechamento_202404.withColumn("MES_FECH", lit("2024-04-01").cast(DateType()))

# Exibir o DataFrame resultante


# COMMAND ----------

import os
import re
import pandas as pd

# Defina o caminho da pasta de origem e destino
diretorio_origem = "/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/"
diretorio_destino = "/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/civel_consolidado/"

# Lista para armazenar os dataframes
civel_fechamento = []

# Percorrer todos os arquivos na pasta
for file_name in os.listdir(diretorio_origem):
    file_path = os.path.join(diretorio_origem, file_name)

    # Substitua o nome do arquivo, se necessário, antes de extrair a data
    if file_name == "02.05.2023 Fechamento Cível.xlsx":
        file_name_adjusted = "01.04.2023 Fechamento Cível.xlsx"
    else:
        file_name_adjusted = file_name

    # Verificar se o arquivo (ajustado) é um Excel (.xlsx ou .xlsm) e contém a data no nome
    if file_name_adjusted.endswith(('.xlsx', '.xlsm')) and re.search(r"\d{2}.\d{2}.\d{4}", file_name_adjusted):
        # Extrair a data do padrão de nomeação
        date = re.search(r"\d{2}.\d{2}.\d{4}", file_name_adjusted).group()
        date = pd.to_datetime(date, format="%d.%m.%Y").replace(day=1).strftime("%Y-%m-%d")
        
        # Ler o arquivo utilizando pandas
        xl = pd.ExcelFile(file_path)
        
        # Percorrer as abas do arquivo
        for sheet_name in xl.sheet_names:
            # Verificar se a aba contém 'Base' ou 'Base Fechamento' no nome
            if 'Base' in sheet_name or 'Base Fechamento' in sheet_name:
                # Determinar a linha do cabeçalho de forma dinâmica
                header_line = None
                for i, row in enumerate(pd.read_excel(file_path, sheet_name, header=None).itertuples(), start=1):
                    if 'LINHA' in row:
                        header_line = i
                        break
                
                if header_line is not None:
                    # Ler a aba como DataFrame, pulando as linhas de cabeçalho incorretas
                    df = xl.parse(sheet_name, skiprows=header_line-1)

                    # Renomear a coluna LINHA para MES_FECH
                    df = df.rename(columns={"LINHA": "MES_FECH"})
                    
                    # Inserir a data no dataframe
                    df["MES_FECH"] = date

                    # Adicionar o dataframe à lista
                    civel_fechamento.append(df)

# Fazer append de todos os dataframes
fechamento_civel_final = pd.concat(civel_fechamento, ignore_index=True)

# Converter a coluna MES_FECH para o formato de data
fechamento_civel_final["MES_FECH"] = pd.to_datetime(fechamento_civel_final["MES_FECH"], format="%Y-%m-%d")

# Criar o diretório de destino se não existir
os.makedirs(diretorio_destino, exist_ok=True)

# Salvar o DataFrame resultante em um arquivo CSV
fechamento_civel_final.to_csv(os.path.join(diretorio_destino, "fechamento_civel_final.csv"), index=False)

print("Bases concatenadas foram salvas em:", os.path.join(diretorio_destino, "fechamento_civel_final.csv"))

# COMMAND ----------

# Define the absolute path for saving the table
output_path = "databox.juridico_comum"

# Read the CSV file as a Spark DataFrame
fechamento_civel_final_spark = spark.read.csv("/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/civel_consolidado/fechamento_civel_final.csv", header=True, inferSchema=True)



# COMMAND ----------

import re

def clean_column_names(df):
    """
    Limpa os caracteres irregulares nas colunas do DataFrame.
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame Spark.
    
    Returns:
        pyspark.sql.DataFrame: DataFrame com nomes de colunas limpos.
    """
    # Define os caracteres irregulares
    invalid_chars = " ,;{}()\n\t="
    
    # Limpa os caracteres irregulares das colunas
    cleaned_columns = []
    for column in df.columns:
        cleaned_column = re.sub(r'[{}]+'.format(re.escape(invalid_chars)), '_', column)
        cleaned_columns.append(cleaned_column)
    
    # Renomeia as colunas no DataFrame
    df_cleaned = df.toDF(*cleaned_columns)
    
    return df_cleaned

# COMMAND ----------

# Chama a função para limpar os nomes das colunas
df_cleaned = clean_column_names(fechamento_civel_final_spark)

# Salva o DataFrame limpo como uma tabela Delta
df_cleaned.write.format("delta").mode("overwrite").saveAsTable("databox.juridico_comum.fechamento_civel_final")

# COMMAND ----------

display(spark.sql("SELECT * FROM databox.juridico_comum.fechamento_civel_final"))

# COMMAND ----------

# Ler a tabela Delta como um DataFrame
civel_fechamento = spark.read.table("databox.juridico_comum.fechamento_civel_final")





# COMMAND ----------

# Ordenar o DataFrame pela coluna "MES_FECH"
civel_fechamento = civel_fechamento.orderBy("MES_FECH")

# Mostrar o DataFrame ordenado
civel_fechamento.show()

# COMMAND ----------

# Agrupar pelo valor da coluna e contar o número de ocorrências para cada valor único
frequencia_df = civel_fechamento.groupBy('MES_FECH').count()

# Ordenar os resultados pela coluna de contagem, se desejado
frequencia_df = frequencia_df.orderBy("count", ascending=False)
frequencia_df= frequencia_df.orderBy("MES_FECH")
# Mostrar o resultado
frequencia_df.show()

# COMMAND ----------


civel_fechamento_financeiro = spark.table("databox.juridico_comum.fechamento_civel_final")

# COMMAND ----------

hist_pagamentos_garantia_civel = spark.table("databox.juridico_comum.hist_pagamentos_garantias_f")

# COMMAND ----------

from pyspark.sql.functions import col

hist_pagamentos_garantia_civel = hist_pagamentos_garantia_civel.withColumn("PROCESSO_ID", col("PROCESSO_ID").cast("double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Agregação Dados Base Fechamento Financeiro com a Base de Pagamentos e Garantias

# COMMAND ----------


from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType

# Corrija os campos com padrão de data e valor da causa
tb_fechamento_civel_202404_2 = civel_fechamento_202404 \
    .where(col("ID PROCESSO").isNotNull()) \
    

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, to_date


# Realizar o left join
joined_df = tb_fechamento_civel_202404_2.join(
    hist_pagamentos_garantia_f,
    tb_fechamento_civel_202404_2["ID PROCESSO"] == hist_pagamentos_garantia_f["PROCESSO - ID"],
    "left"
)

# Aplicar as transformações e calcular o total de pagamentos
formatted_df = joined_df \
    .filter(col("ID PROCESSO").isNotNull()) \
    .filter(col("ENCERRADOS") == 1) \
    .select(
        col("ID PROCESSO"),
        col("MES_FECH"),
        col("ACORDO"),
        col("CONDENACAO"),
        col("PENHORA"),
        col("GARANTIA"),
        col("IMPOSTO"),
        col("OUTROS_PAGAMENTOS"),
        sum(col("ACORDO"), col("CONDENACAO"), col("PENHORA"), col("GARANTIA"), col("IMPOSTO"), col("OUTROS_PAGAMENTOS")).alias("TOTAL_PAGAMENTOS"),
        col("DATA_EFETIVA_PAGAMENTO").alias("DT_ULT_PGTO"),
        to_date(col("DATA_EFETIVA_PAGAMENTO"), "dd/MM/yyyy").alias("MES_CONTABIL")
    )

# COMMAND ----------

formatted_df.count()

# COMMAND ----------


