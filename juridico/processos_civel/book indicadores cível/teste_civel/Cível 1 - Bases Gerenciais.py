# Databricks notebook source
# MAGIC %md
# MAGIC ####Importação Bases Gerenciais Cível

# COMMAND ----------

# MAGIC %md
# MAGIC ####Fechamento Cível

# COMMAND ----------

# MAGIC %md
# MAGIC ####Import da tabela de Ativos Cível

# COMMAND ----------

#dbutils.library.restartPython()


# COMMAND ----------

#Nova importação padrão Indicadores


# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Caminho das pastas e arquivos

nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/'

arquivo_ativos = f'CIVEL_GERENCIAL-(ATIVOS)-{nmtabela}.xlsx'
arquivo_encerrados = f'CIVEL_GERENCIAL-(ENCERRADO)-{nmtabela}.xlsx'
arquivo_encerrados_historico = 'CIVEL_ENCERRADOS_HISTORICO - 20240301.xlsx'

path_ativos = diretorio_origem + arquivo_ativos
path_encerrados = diretorio_origem + arquivo_encerrados
path_encerrados_historico = diretorio_origem + arquivo_encerrados_historico



# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# Carrega as planilhas em Spark Data Frames
gerencial_ativos_civel = read_excel(path_ativos, "'CÍVEL'!A6")
gerencial_encerrados_civel = read_excel(path_encerrados, "'CÍVEL'!A6")
gerencial_encerrados_civel_historico = read_excel(path_encerrados_historico, "'CÍVEL ENCERRADOS HISTÓRICO'!A6")

# COMMAND ----------

dataframes = [gerencial_ativos_civel, gerencial_encerrados_civel, gerencial_encerrados_civel_historico]  # Lista de DataFrames

# COMMAND ----------

from pyspark.sql import DataFrame

def drop_var_data(dataframes: list) -> list:
    for i in range(len(dataframes)):
        if isinstance(dataframes[i], DataFrame) and "Data da Baixa Provisória" in dataframes[i].columns:
            dataframes[i] = dataframes[i].drop("Data da Baixa Provisória")
    return dataframes

# COMMAND ----------


updated_dataframes = drop_var_data(dataframes)

# COMMAND ----------

#Encerrar spark.conf.set

spark.conf.set("spark.sql.caseSensitive","false")


# COMMAND ----------

# Lista as colunas com datas
gerencial_ativos_civel_cols = find_columns_with_word(gerencial_ativos_civel, 'DATA ')
gerencial_ativos_civel_cols.append('DISTRIBUIÇÃO')

gerencial_encerrados_civel_cols = find_columns_with_word(gerencial_encerrados_civel, 'DATA ')
gerencial_encerrados_civel_cols.append('DISTRIBUIÇÃO')

gerencial_encerrados_civel_historico_cols = find_columns_with_word(gerencial_encerrados_civel_historico, 'DATA ')
gerencial_encerrados_civel_historico_cols.append('DISTRIBUIÇÃO')

print("gerencial_ativos_civel_cols")
print(gerencial_ativos_civel_cols)
print("\n")
print("gerencial_encerrados_civel_cols")
print(gerencial_encerrados_civel_cols)
print("\n")
print("gerencial_encerrados_civel_historico_cols")
print(gerencial_encerrados_civel_historico_cols)

# COMMAND ----------

# Converte as datas das colunas listadas
gerencial_ativos_civel = convert_to_date_format(gerencial_ativos_civel, gerencial_ativos_civel_cols)
gerencial_encerrados_civel = convert_to_date_format(gerencial_encerrados_civel, gerencial_encerrados_civel_cols)
gerencial_encerrados_civel_historico = convert_to_date_format(gerencial_encerrados_civel_historico, gerencial_encerrados_civel_historico_cols)

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re

spark.conf.set("spark.sql.caseSensitive","true")


# Load Excel file
df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'CÍVEL'!A6:CW200000") \
    .load("/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/CIVEL_GERENCIAL-(ATIVOS)-20240423.xlsx")

# Get the column names
columns = df.columns

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
            df = df.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)




# COMMAND ----------

display(df)

# COMMAND ----------

# Load Excel file
gerencial_ativos_civel = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'CÍVEL'!A6:CW200000") \
    .load("/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/CIVEL_GERENCIAL-(ATIVOS)-20240408.xlsx")

# COMMAND ----------

from pyspark.sql.functions import expr, to_date

# Converter a string da data para o formato correto ('MM/dd/yy' -> 'dd/MM/yy')

gerencial_ativos_civel = gerencial_ativos_civel.withColumn("DATA_FATO_GERADOR_1", to_date(expr("substring(`DATA DO FATO GERADOR78`, 4, 2) || '/' || substring(`DATA DO FATO GERADOR78`, 1, 2) || '/' || substring(`DATA DO FATO GERADOR78`, 7, 2)"), "dd/MM/yy"))
gerencial_ativos_civel = gerencial_ativos_civel.withColumn("DATA_FATO_GERADOR_4", to_date(expr("substring(`DATA DO FATO GERADOR81`, 4, 2) || '/' || substring(`DATA DO FATO GERADOR81`, 1, 2) || '/' || substring(`DATA DO FATO GERADOR81`, 7, 2)"), "dd/MM/yy"))

# Exibir o dataframe tratado
#display(gerencial_encerrados_civel)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestamp_variables_ativos = []
for column in gerencial_ativos_civel.columns:
    if gerencial_ativos_civel.schema[column].dataType == TimestampType():
        timestamp_variables_ativos.append(column)

        print(timestamp_variables_ativos)

# COMMAND ----------

    print(timestamp_variables_ativos)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestamp_variables_ativos = []
for column in gerencial_ativos_civel.columns:
    if gerencial_ativos_civel.schema[column].dataType == TimestampType():
        timestamp_variables_ativos.append(column)

        print(timestamp_variables_ativos)

# COMMAND ----------

from pyspark.sql.functions import to_date, coalesce, lit
from pyspark.sql.types import DateType

for column in timestamp_variables_ativos:
    if gerencial_ativos_civel.schema[column].dataType == DateType():
        gerencial_ativos_civel = gerencial_ativos_civel.withColumn(column, coalesce(gerencial_ativos_civel[column], lit(' ')).cast(DateType()))

# COMMAND ----------


from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import TimestampType

def convert_timestamp_to_datetime(df):
    for column in df.columns:
        column_type = df.schema[column].dataType
        if isinstance(column_type, TimestampType):
            df = df.withColumn(column, to_date(col(column)))
    return df

# Exemplo de uso:
gerencial_ativos_civel = convert_timestamp_to_datetime(gerencial_ativos_civel)

# COMMAND ----------

#tratamento para renomear e alterar formatos dos campos FATO GERADOR
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DA COMPRA", "DATA_DA_COMPRA")
gerencial_ativos_civel =gerencial_ativos_civel.withColumnRenamed("DATA DO PEDIDO", "DATA_DO_PEDIDO")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA FATO GERADOR", "DATA_FATO_GERADOR_2")
gerencial_ativos_civel =gerencial_ativos_civel.withColumnRenamed("DATA DO FATO GERADOR80", "DATA_FATO_GERADOR_3")


#função para encontrar a maior data e criar a variável DATA_FATO_GERADOR
from pyspark.sql.functions import greatest
gerencial_ativos_civel_1= gerencial_ativos_civel.withColumn("DATA_FATO_GERADOR", greatest("DATA_DO_PEDIDO", "DATA_DA_COMPRA", "DATA_FATO_GERADOR_1", "DATA_FATO_GERADOR_2", "DATA_FATO_GERADOR_3", "DATA_FATO_GERADOR_4"))

gerencial_ativos_civel_2 = gerencial_ativos_civel_1.withColumn("DATA_FATO_GERADOR", to_date("DATA_FATO_GERADOR"))


# COMMAND ----------

colunas_para_apagar_ativos = ["DATA DO PEDIDO", "DATA DA COMPRA", "DATA FATO GERADOR","DATA DO FATO GERADOR80","DATA DO FATO GERADOR78","DATA DO FATO GERADOR81"]
gerencial_ativos_civel_2 = gerencial_ativos_civel_2.drop(*colunas_para_apagar_ativos)

# COMMAND ----------

from functools import reduce
from pyspark.sql.functions import col

def rename_columns(df, replacements):
    new_df = reduce(lambda data, replacement: data.withColumnRenamed(replacement[0], replacement[1]), replacements, df)
    return new_df.toDF(*[col.replace(' ', '_') for col in new_df.columns])

# Exemplo de uso
replacements = [
    (' ', '_'),
    ('(', ''),
    (')', ''),
    ('´', '')
]

gerencial_ativos_civel_2 = rename_columns(gerencial_ativos_civel_2, replacements)


# COMMAND ----------

gerencial_ativos_civel_2 = gerencial_ativos_civel_2.withColumnRenamed("PROCESSO_-_ID", "PROCESSO_ID")
gerencial_ativos_civel_2 = gerencial_ativos_civel_2.withColumnRenamed("PROCESSO_-_Nº._PEDIDO", "PROCESSO_N_PEDIDO")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Supondo que df seja o seu DataFrame PySpark
gerencial_ativos_civel_3 = gerencial_ativos_civel_2.select(*[
    regexp_replace(column, "[^\x00-\x7F´]+", "").alias(column)
    for column in gerencial_ativos_civel_2.columns
])


# COMMAND ----------

import unicodedata
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def remove_special_characters(dataframe):
    for column in dataframe.columns:
        udf_remove_special_chars = udf(lambda text: remove_special_chars(text), StringType())
        dataframe = dataframe.withColumn(column, udf_remove_special_chars(col(column)))
    return dataframe

def remove_special_chars(text):
    normalized_text = unicodedata.normalize('NFD', text)
    return ''.join(c for c in normalized_text if not unicodedata.combining(c))

# COMMAND ----------

gerencial_ativos_civel_3 = remove_special_characters(gerencial_ativos_civel_3)

# COMMAND ----------

selected_columns_civel_ativos = ['PROCESSO_ID', 'PARTE_CONTRÁRIA_CPF', 'DATA_REGISTRADO', 'STATUS',
                    'MOTIVO_DE_ENCERRAMENTO_', 'CENTRO_DE_CUSTO_/_ÁREA_DEMANDANTE_-_CÓDIGO',
                    'EMPRESA', 'VALOR_DA_CAUSA', 'ESFERA', 'AÇÃO', 'ESCRITÓRIO_EXTERNO',
                    'FASE', 'DATA_AUDIENCIA_INICIAL', 'LISTA_LOJISTA_-_NOME',
                    'LISTA_LOJISTA_-_ID', 'FABRICANTE_DO_PRODUTO', 'PRODUTO',
                    'RECLAMAÇÃO_BLACK_FRIDAY?',  'DATA_DO_PEDIDO',
                    'DATA_FATO_GERADOR', 'TRANSPORTADORA', 'ESTEIRA', 'ASSUNTO_(CÍVEL)_-_PRINCIPAL',
                    'ASSUNTO_(CÍVEL)_-_ASSUNTO', 'ESTADO',
                    'COMPRA_DIRETA_OU_MARKETPLACE?_-_NEW_', 'INDIQUE_A_FILIAL', 'DATA_DO_PEDIDO',
                    'DATA_DA_COMPRA', 'DISTRIBUIÇÃO', 'DATA_AUDIENCIA_INICIAL',
                    'DATA_DE_RECEBIMENTO', 'DATA_REGISTRADO','DATA_FATO_GERADOR','PROCESSO_N_PEDIDO','COMARCA']

gerencial_ativos_civel_final = gerencial_ativos_civel_3.select(*selected_columns_civel_ativos)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Import da tabela de Encerrados Cível

# COMMAND ----------

# Load Excel file
gerencial_encerrados_civel = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'CÍVEL'!A6:CW200000") \
    .load("/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/CIVEL_GERENCIAL-(ENCERRADO)-20240408.xlsx")




# COMMAND ----------

from pyspark.sql.functions import expr, to_date

# Converter a string da data para o formato correto ('MM/dd/yy' -> 'dd/MM/yy')
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumn("DATA_DO_PEDIDO", to_date(expr("substring(`DATA DO PEDIDO`, 4, 2) || '/' || substring(`DATA DO PEDIDO`, 1, 2) || '/' || substring(`DATA DO PEDIDO`, 7, 2)"), "dd/MM/yy"))
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumn("DATA_DA_COMPRA", to_date(expr("substring(`DATA DA COMPRA`, 4, 2) || '/' || substring(`DATA DA COMPRA`, 1, 2) || '/' || substring(`DATA DA COMPRA`, 7, 2)"), "dd/MM/yy"))
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumn("DATA_FATO_GERADOR_2", to_date(expr("substring(`DATA FATO GERADOR`, 4, 2) || '/' || substring(`DATA FATO GERADOR`, 1, 2) || '/' || substring(`DATA FATO GERADOR`, 7, 2)"), "dd/MM/yy"))
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumn("DATA_FATO_GERADOR_3", to_date(expr("substring(`DATA DO FATO GERADOR80`, 4, 2) || '/' || substring(`DATA DO FATO GERADOR80`, 1, 2) || '/' || substring(`DATA DO FATO GERADOR80`, 7, 2)"), "dd/MM/yy"))

# Exibir o dataframe tratado
#display(gerencial_encerrados_civel)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestamp_variables_encerrados = []
for column in gerencial_encerrados_civel.columns:
    if gerencial_encerrados_civel.schema[column].dataType == TimestampType():
        timestamp_variables_encerrados .append(column)

        print(timestamp_variables_encerrados )

# COMMAND ----------

    print(timestamp_variables_encerrados)

# COMMAND ----------

from pyspark.sql.functions import to_date, coalesce, lit
from pyspark.sql.types import DateType

for column in timestamp_variables_encerrados :
    if gerencial_encerrados_civel.schema[column].dataType == DateType():
        gerencial_encerrados_civel = gerencial_encerrados_civel.withColumn(column, coalesce(gerencial_encerrados_civel[column], lit(' ')).cast(DateType()))

# COMMAND ----------


from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import TimestampType

def convert_timestamp_to_datetime(df):
    for column in df.columns:
        column_type = df.schema[column].dataType
        if isinstance(column_type, TimestampType):
            df = df.withColumn(column, to_date(col(column)))
    return df

# Exemplo de uso:
gerencial_encerrados_civel = convert_timestamp_to_datetime(gerencial_encerrados_civel)

# COMMAND ----------

#tratamento para renomear e alterar formatos dos campos FATO GERADOR
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO FATO GERADOR78", "DATA_FATO_GERADOR_1")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO FATO GERADOR81", "DATA_FATO_GERADOR_4")


#função para encontrar a maior data e criar a variável DATA_FATO_GERADOR
from pyspark.sql.functions import greatest
gerencial_encerrados_civel_1 = gerencial_encerrados_civel.withColumn("DATA_FATO_GERADOR", greatest("DATA_DO_PEDIDO", "DATA_DA_COMPRA", "DATA_FATO_GERADOR_1", "DATA_FATO_GERADOR_2", "DATA_FATO_GERADOR_3", "DATA_FATO_GERADOR_4"))

gerencial_encerrados_civel_2 = gerencial_encerrados_civel_1.withColumn("DATA_FATO_GERADOR", to_date("DATA_FATO_GERADOR"))


# COMMAND ----------

# Set the spark.sql.legacy.timeParserPolicy configuration
#spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Execute the display function again
display(gerencial_encerrados_civel_2)

# COMMAND ----------

colunas_para_apagar_encerrados = ["DATA DO PEDIDO", "DATA DA COMPRA", "DATA FATO GERADOR","DATA DO FATO GERADOR80",""]
gerencial_encerrados_civel_2 = gerencial_encerrados_civel_2.drop(*colunas_para_apagar_encerrados )

# COMMAND ----------

from functools import reduce
from pyspark.sql.functions import col

def rename_columns(df, replacements):
    new_df = reduce(lambda data, replacement: data.withColumnRenamed(replacement[0], replacement[1]), replacements, df)
    return new_df.toDF(*[col.replace(' ', '_') for col in new_df.columns])

# Exemplo de uso
replacements = [
    (' ', '_'),
    ('(', ''),
    (')', ''),
    ('´', '')
]

gerencial_encerrados_civel_2 = rename_columns(gerencial_encerrados_civel_2, replacements)


# COMMAND ----------

gerencial_encerrados_civel_2 = gerencial_encerrados_civel_2.withColumnRenamed("PROCESSO_-_ID", "PROCESSO_ID")
gerencial_encerrados_civel_2 = gerencial_encerrados_civel_2.withColumnRenamed("PROCESSO_-_Nº._PEDIDO", "PROCESSO_N_PEDIDO")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Supondo que df seja o seu DataFrame PySpark
gerencial_encerrados_civel_3 = gerencial_encerrados_civel_2.select(*[
    regexp_replace(column, "[^\x00-\x7F´]+", "").alias(column)
    for column in gerencial_encerrados_civel_2.columns
])


# COMMAND ----------

import unicodedata
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def remove_special_characters(dataframe):
    for column in dataframe.columns:
        udf_remove_special_chars = udf(lambda text: remove_special_chars(text), StringType())
        dataframe = dataframe.withColumn(column, udf_remove_special_chars(col(column)))
    return dataframe

def remove_special_chars(text):
    normalized_text = unicodedata.normalize('NFD', text)
    return ''.join(c for c in normalized_text if not unicodedata.combining(c))

# COMMAND ----------

gerencial_encerrados_civel_4 = remove_special_characters(gerencial_encerrados_civel_3)

# COMMAND ----------

selected_columns_civel_encerrado = ['PROCESSO_ID', 'PARTE_CONTRÁRIA_CPF', 'DATA_REGISTRADO', 'STATUS',
                    'MOTIVO_DE_ENCERRAMENTO_', 'CENTRO_DE_CUSTO_/_ÁREA_DEMANDANTE_-_CÓDIGO',
                    'EMPRESA', 'VALOR_DA_CAUSA', 'ESFERA', 'AÇÃO', 'ESCRITÓRIO_EXTERNO',
                    'FASE', 'DATA_AUDIENCIA_INICIAL', 'LISTA_LOJISTA_-_NOME',
                    'LISTA_LOJISTA_-_ID', 'FABRICANTE_DO_PRODUTO', 'PRODUTO',
                    'RECLAMAÇÃO_BLACK_FRIDAY?',  'DATA_DO_PEDIDO',
                    'DATA_FATO_GERADOR', 'TRANSPORTADORA', 'ESTEIRA', 'ASSUNTO_(CÍVEL)_-_PRINCIPAL',
                    'ASSUNTO_(CÍVEL)_-_ASSUNTO', 'ESTADO',
                    'COMPRA_DIRETA_OU_MARKETPLACE?_-_NEW_', 'INDIQUE_A_FILIAL', 'DATA_DO_PEDIDO',
                    'DATA_DA_COMPRA', 'DISTRIBUIÇÃO', 'DATA_AUDIENCIA_INICIAL',
                    'DATA_DE_RECEBIMENTO', 'DATA_REGISTRADO','DATA_FATO_GERADOR','PROCESSO_N_PEDIDO','COMARCA']

gerencial_encerrados_civel_final = gerencial_encerrados_civel_4.select(*selected_columns_civel_encerrado)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Importação da base histórica do Cível Encerrados

# COMMAND ----------

from pyspark.sql.functions import col

# Carregar o arquivo Excel Base Gerencial de Encerrados
gerencial_encerrados_civel_historico = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 5000) \
    .option("dataAddress", "'CÍVEL ENCERRADOS HISTÓRICO'!A6:CW200000") \
    .load("/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/CIVEL_ENCERRADOS_HISTORICO - 20240301.xlsx")

# COMMAND ----------

from pyspark.sql.functions import expr, to_date

# Converter a string da data para o formato correto ('MM/dd/yy' -> 'dd/MM/yy')
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumn("DATA_DO_PEDIDO", to_date(expr("substring(`DATA DO PEDIDO`, 4, 2) || '/' || substring(`DATA DO PEDIDO`, 1, 2) || '/' || substring(`DATA DO PEDIDO`, 7, 2)"), "dd/MM/yy"))
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumn("DATA_DA_COMPRA", to_date(expr("substring(`DATA DA COMPRA`, 4, 2) || '/' || substring(`DATA DA COMPRA`, 1, 2) || '/' || substring(`DATA DA COMPRA`, 7, 2)"), "dd/MM/yy"))
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumn("DATA_FATO_GERADOR_1", to_date(expr("substring(`DATA DO FATO GERADOR78`, 4, 2) || '/' || substring(`DATA DO FATO GERADOR78`, 1, 2) || '/' || substring(`DATA DO FATO GERADOR78`, 7, 2)"), "dd/MM/yy"))
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumn("DATA_FATO_GERADOR_2", to_date(expr("substring(`DATA FATO GERADOR`, 4, 2) || '/' || substring(`DATA FATO GERADOR`, 1, 2) || '/' || substring(`DATA FATO GERADOR`, 7, 2)"), "dd/MM/yy"))
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumn("DATA_FATO_GERADOR_3", to_date(expr("substring(`DATA DO FATO GERADOR80`, 4, 2) || '/' || substring(`DATA DO FATO GERADOR80`, 1, 2) || '/' || substring(`DATA DO FATO GERADOR80`, 7, 2)"), "dd/MM/yy"))
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumn("DATA_FATO_GERADOR_4", to_date(expr("substring(`DATA DO FATO GERADOR81`, 4, 2) || '/' || substring(`DATA DO FATO GERADOR81`, 1, 2) || '/' || substring(`DATA DO FATO GERADOR81`, 7, 2)"), "dd/MM/yy"))


# Exibir o dataframe tratado
#display(gerencial_encerrados_civel)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestamp_variables_civel_historico = []
for column in gerencial_encerrados_civel_historico .columns:
    if gerencial_encerrados_civel_historico.schema[column].dataType == TimestampType():
        timestamp_variables_civel_historico.append(column)

        print(timestamp_variables_civel_historico)

# COMMAND ----------

from pyspark.sql.functions import to_date, coalesce, lit
from pyspark.sql.types import DateType

for column in timestamp_variables_civel_historico:
    if gerencial_encerrados_civel_historico.schema[column].dataType == DateType():
        gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumn(column, coalesce(gerencial_encerrados_civel_historico[column], lit(' ')).cast(DateType()))

# COMMAND ----------


from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import TimestampType

def convert_timestamp_to_datetime(df):
    for column in df.columns:
        column_type = df.schema[column].dataType
        if isinstance(column_type, TimestampType):
            df = df.withColumn(column, to_date(col(column)))
    return df

# Exemplo de uso:
gerencial_encerrados_civel_historico = convert_timestamp_to_datetime(gerencial_encerrados_civel_historico)

# COMMAND ----------

#tratamento para renomear e alterar formatos dos campos FATO GERADOR
#gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO FATO GERADOR78", "DATA_FATO_GERADOR_1")
#gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO FATO GERADOR81", "DATA_FATO_GERADOR_4")


#função para encontrar a maior data e criar a variável DATA_FATO_GERADOR
from pyspark.sql.functions import greatest
gerencial_encerrados_civel_historico_1 = gerencial_encerrados_civel_historico.withColumn("DATA_FATO_GERADOR", greatest("DATA_DO_PEDIDO", "DATA_DA_COMPRA", "DATA_FATO_GERADOR_1", "DATA_FATO_GERADOR_2", "DATA_FATO_GERADOR_3", "DATA_FATO_GERADOR_4"))

gerencial_encerrados_civel_historico_2 = gerencial_encerrados_civel_historico_1.withColumn("DATA_FATO_GERADOR", to_date("DATA_FATO_GERADOR"))


# COMMAND ----------

colunas_para_apagar_civel_historico = ["DATA DO FATO GERADOR78", "DATA FATO GERADOR", "DATA DO FATO GERADOR80","DATA DO FATO GERADOR81","DATA DA COMPRA","DATA DO PEDIDO"]
gerencial_encerrados_civel_historico_2 = gerencial_encerrados_civel_historico_2.drop(*colunas_para_apagar_civel_historico)

# COMMAND ----------

from functools import reduce
from pyspark.sql.functions import col

def rename_columns(df, replacements):
    new_df = reduce(lambda data, replacement: data.withColumnRenamed(replacement[0], replacement[1]), replacements, df)
    return new_df.toDF(*[col.replace(' ', '_') for col in new_df.columns])

# Exemplo de uso
replacements = [
    (' ', '_'),
    ('(', ''),
    (')', ''),
    ('´', '')
]

gerencial_encerrados_civel_historico_2 = rename_columns(gerencial_encerrados_civel_historico_2, replacements)


# COMMAND ----------

gerencial_encerrados_civel_historico_2 = gerencial_encerrados_civel_historico_2.withColumnRenamed("PROCESSO_-_ID", "PROCESSO_ID")
gerencial_encerrados_civel_historico_2 = gerencial_encerrados_civel_historico_2.withColumnRenamed("PROCESSO_-_Nº._PEDIDO", "PROCESSO_N_PEDIDO")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Supondo que df seja o seu DataFrame PySpark
gerencial_encerrados_civel_historico_3 = gerencial_encerrados_civel_historico_2.select(*[
    regexp_replace(column, "[^\x00-\x7F´]+", "").alias(column)
    for column in gerencial_encerrados_civel_historico_2.columns
])


# COMMAND ----------

import unicodedata
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def remove_special_characters(dataframe):
    for column in dataframe.columns:
        udf_remove_special_chars = udf(lambda text: remove_special_chars(text), StringType())
        dataframe = dataframe.withColumn(column, udf_remove_special_chars(col(column)))
    return dataframe

def remove_special_chars(text):
    normalized_text = unicodedata.normalize('NFD', text)
    return ''.join(c for c in normalized_text if not unicodedata.combining(c))

# COMMAND ----------

gerencial_encerrados_civel_historico_3 = remove_special_characters(gerencial_encerrados_civel_historico_3)

# COMMAND ----------

selected_columns_civel_historico = ['PROCESSO_ID', 'PARTE_CONTRÁRIA_CPF', 'DATA_REGISTRADO', 'STATUS',
                    'MOTIVO_DE_ENCERRAMENTO_', 'CENTRO_DE_CUSTO_/_ÁREA_DEMANDANTE_-_CÓDIGO',
                    'EMPRESA', 'VALOR_DA_CAUSA', 'ESFERA', 'AÇÃO', 'ESCRITÓRIO_EXTERNO',
                    'FASE', 'DATA_AUDIENCIA_INICIAL', 'LISTA_LOJISTA_-_NOME',
                    'LISTA_LOJISTA_-_ID', 'FABRICANTE_DO_PRODUTO', 'PRODUTO',
                    'RECLAMAÇÃO_BLACK_FRIDAY?',  'DATA_DO_PEDIDO',
                    'DATA_FATO_GERADOR', 'TRANSPORTADORA', 'ESTEIRA', 'ASSUNTO_(CÍVEL)_-_PRINCIPAL',
                    'ASSUNTO_(CÍVEL)_-_ASSUNTO', 'ESTADO',
                    'COMPRA_DIRETA_OU_MARKETPLACE?_-_NEW_', 'INDIQUE_A_FILIAL', 'DATA_DO_PEDIDO',
                    'DATA_DA_COMPRA', 'DISTRIBUIÇÃO', 'DATA_AUDIENCIA_INICIAL',
                    'DATA_DE_RECEBIMENTO', 'DATA_REGISTRADO','DATA_FATO_GERADOR','PROCESSO_N_PEDIDO','COMARCA']

gerencial_encerrados_civel_historico_final = gerencial_encerrados_civel_historico_3.select(*selected_columns_civel_historico)

# COMMAND ----------

#tb_ger_civel = gerencial_encerrados_civel_final.unionByName(gerencial_encerrados_civel_historico_final,allowMissingColumns=True)
   
#unionByName(gerencial_encerrados_civel_final,allowMissingColumns=True)

# COMMAND ----------

# Por exemplo, faça um union dos DataFrames
tb_ger_civel = gerencial_ativos_civel_final.union(gerencial_encerrados_civel_final)\
    .union(gerencial_encerrados_civel_final)


# COMMAND ----------

from pyspark.sql import functions as F

import unicodedata

def remove_special_chars(col):
    if col is None:
        return None
    return unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('ASCII')

tb_ger_civel = tb_ger_civel.withColumn("MOTIVO_DE_ENCERRAMENTO_", F.udf(remove_special_chars)(F.col("MOTIVO_DE_ENCERRAMENTO_")))

# COMMAND ----------

gerencial_ativos_civel_final.printSchema()

# COMMAND ----------

valid = tb_ger_civel.count()

# COMMAND ----------

# Filter rows where 'PROCESSO - ID' is not null
tb_ger_civel_1 = tb_ger_civel.filter(col('PROCESSO_ID').isNotNull())

# COMMAND ----------

# Sort the data by 'PROCESSO - ID' and 'Data FATO GERADOR' in descending order
tb_ger_civel_1 = tb_ger_civel_1.orderBy(col('PROCESSO_ID').desc(), col('DATA_FATO_GERADOR').desc())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Criação da Tabela Cível Responsabilidade

# COMMAND ----------

# Seleciona as colunas necessárias do DataFrame 'gerencial_ativos_civel'
ativos_civel = gerencial_ativos_civel.select('PROCESSO - ID', 'CÍVEL MASSA - RESPONSABILIDADE')

# Seleciona as colunas necessárias do DataFrame 'gerencial_encerrados_civel'
encerrados_civel = gerencial_encerrados_civel.select('PROCESSO - ID', 'CÍVEL MASSA - RESPONSABILIDADE')

# Une os DataFrames 'ativos_civel' e 'encerrados_civel'
tb_responsabilidade = ativos_civel.union(encerrados_civel)

# Remove registros duplicados com base na coluna 'PROCESSO - ID'
tb_responsabilidade = tb_responsabilidade.dropDuplicates(['PROCESSO - ID'])

# COMMAND ----------

from pyspark.sql.functions import col, count

# Usando groupBy e count para obter a contagem de ocorrências de cada valor na coluna 'CÍVEL MASSA - RESPONSABILIDADE'
freq_table = tb_responsabilidade.groupBy('CÍVEL MASSA - RESPONSABILIDADE').agg(count('*').alias('FREQUENCY'))

# Ordenando a tabela pela coluna 'FREQUENCY' em ordem decrescente
freq_table = freq_table.orderBy(col('FREQUENCY').desc())

# Exibindo a tabela de frequências
freq_table.show()

# COMMAND ----------


