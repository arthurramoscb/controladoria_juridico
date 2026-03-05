# Databricks notebook source
# MAGIC %md
# MAGIC ###Importação e Tratamento Bases de Dados Precificação

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def format_float(number):
    formatted_number = "{:,.2f}".format(number).replace(",", "@").replace(".", ",").replace("@", ".")
    return formatted_number

# Register the function as a UDF
format_float_udf = udf(format_float, StringType())

# Example usage on a Spark DataFrame
df = spark.createDataFrame([
    (1234.56787,),
    (9876.54321,),
    (0.12345,)
], ["number"])

# Apply the UDF to a column
formatted_df = df.withColumn("formatted_number", format_float_udf("number"))

display(formatted_df)

# COMMAND ----------

# Padronizar o código, caso o SparkSession seja ativado
#dbutils.library.restartPython()

# COMMAND ----------

# Importação da Base Gerencial Trabalhista para Base de Precificação
from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re
spark.conf.set("spark.sql.caseSensitive","true")

# Importação da Base do Calculista de Pedidos
gerencial_consolidado_trabalhista_precificacao = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'TRABALHISTA'!A6:DC200000") \
    .load("/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_precificacao/base_precificacao_abril_24/TRABALHISTA_GERENCIAL_(CONSOLIDADO)-20240402.xlsx")

# Get the column names
columns = gerencial_consolidado_trabalhista_precificacao.columns

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
            gerencial_consolidado_trabalhista_precificacao = gerencial_consolidado_trabalhista_precificacao.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)

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
gerencial_consolidado_trabalhista_precificacao = convert_timestamp_to_datetime(gerencial_consolidado_trabalhista_precificacao)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace



# Criar a coluna 'RESPONS_SOCIO_PERCENTUAL' com a fórmula adaptada
gerencial_consolidado_trabalhista_precificacao_1 = gerencial_consolidado_trabalhista_precificacao.withColumn('RESPONS_SOCIO_PERCENTUAL',
                                                                           (regexp_replace(col('RESPONSÁBILIDADE SÓCIO PERCENTUAL'), '[^0-9.]', '') / 100).cast('double'))

# COMMAND ----------

from pyspark.sql.functions import col

# Filtrar os dados com base na coluna 'STATUS'
gerencial_consolidado_trabalhista_precificacao_2 = gerencial_consolidado_trabalhista_precificacao_1.filter(col('STATUS').isin('ATIVO', 'REATIVADO', 'PRÉ CADASTRO'))



# COMMAND ----------

import re

def remover_caracteres_irregulares(df):
    # Obtém a lista de nomes das colunas
    colunas = df.columns
    
    # Define a expressão regular para encontrar caracteres indesejados
    padrao = re.compile(r"[^\w\s.]")
    
    # Substitui os caracteres indesejados por uma string vazia em cada nome de coluna
    novas_colunas = [re.sub(padrao, "", coluna) for coluna in colunas]
    
    # Renomeia as colunas do dataframe
    gerencial_consolidado_trabalhista_precificacao_2=gerencial_consolidado_trabalhista_precificacao_2.toDF(*novas_colunas)
    
    return df

# COMMAND ----------

from pyspark.sql.functions import col

def remove_dots_in_column_names(df):
    for column_name in df.columns:
        new_column_name = column_name.replace(".", "")
        df = df.withColumnRenamed(column_name, new_column_name)
    return df

# Apply the function to your DataFrame
gerencial_consolidado_trabalhista_precificacao_3= remove_dots_in_column_names(gerencial_consolidado_trabalhista_precificacao_2)

# COMMAND ----------

gerencial_consolidado_trabalhista_precificacao_4 = gerencial_consolidado_trabalhista_precificacao_3.withColumnRenamed("ASSISTENTE JURÍDICO", "ASSISTENTE JURIDICO")

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re
spark.conf.set("spark.sql.caseSensitive","true")

# Importação da Base do Calculista de Pedidos
base_calculista_processos = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'CALCULISTA_PROCESSO'!A6:BK200000") \
    .load("/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_precificacao/base_precificacao_abril_24/CALCULISTA_PROCESSO_01.04.24.xlsx")

# Get the column names
columns = base_calculista_processos.columns

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
            base_calculista_processos = base_calculista_processos.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)


# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType

# Lista de colunas para transformar de string para numérico (neste caso, para Integer)
colunas_string = ["VALOR DA SENTENÇA", "VALOR SENTENÇA ATUALIZADA", "VALOR ACÓRDÃO TRT","VALOR ACÓRDÃO TRT ATUALIZADO","VALOR ACÓRDÃO TST","VALOR ACÓRDÃO TST ATUALIZADO","EXECUÇÃO RECLAMADA - VALOR","EXECUÇÃO RECLAMADA - VALOR ATUALIZADO","EXECUÇÃO CALCULO PERITO - VALOR","EXECUÇÃO CALCULO PERITO - VALOR ATUALIZADO","EXECUÇÃO CALCULO HOMOLOGADO - VALOR","EXECUÇÃO CALCULO HOMOLOGADO - VALOR ATUALIZADO","EXECUÇÃO PROVISIONADA - VALOR","EXECUÇÃO PROVISIONADA  - VALOR ATUALIZADO"]

for coluna in colunas_string:
    base_calculista_processos = base_calculista_processos.withColumn(coluna, 
                                                                     regexp_replace(col(coluna), '[^0-9.]', '').cast(DoubleType()))

# COMMAND ----------

display(base_calculista_processos)

# COMMAND ----------

#tratamento das variáveis data timestamp da base Calculista Processos Trabalhista

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
base_calculista_processos = convert_timestamp_to_datetime(base_calculista_processos)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, unix_timestamp

# Define a list of variables to be transformed
variables_list = ["DATA SENTENÇA", "DATA ACÓRDÃO TRT", "DATA ACÓRDÃO TST","EXECUÇÃO RECLAMADA - DATA","EXECUÇÃO RECLAMADA - DATA","EXECUÇÃO CALCULO PERITO - DATA","EXECUÇÃO CALCULO HOMOLOGADO - DATA","EXECUÇÃO PROVISIONADA  - DATA"]

# Loop through the list of variables
for variable in variables_list:
    base_calculista_processos = base_calculista_processos.withColumn(
        variable, 
        to_date(unix_timestamp(col(variable), "dd/MM/yyyy").cast("timestamp"))
    )

# COMMAND ----------

# Importação da Base Estoque Financeiro Mensal 
from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re
spark.conf.set("spark.sql.caseSensitive","true")

# Importação da Base do Calculista de Pedidos
estoque_financeiro_trabalhista = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'TB_ESTQ_FINANC_TRAB_202403_F'!A1:CI27000") \
    .load("/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_precificacao/base_precificacao_abril_24/TB_ESTQ_FINANC_TRAB_202403_F.xlsx")

# Get the column names
columns = estoque_financeiro_trabalhista.columns

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
            estoque_financeiro_trabalhista = estoque_financeiro_trabalhista.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)



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
estoque_financeiro_trabalhista = convert_timestamp_to_datetime(estoque_financeiro_trabalhista)

# COMMAND ----------

# Importação da Base Automação Trabalhista 
from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re
spark.conf.set("spark.sql.caseSensitive","true")

# Importação da Base do Calculista de Pedidos
base_automacao_trabalhista = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'Consolidado'!A1:DP35000") \
    .load("/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_precificacao/base_precificacao_abril_24/AutomacaoMarço25032024 - Atualizada.xlsx")

# Get the column names
columns = base_automacao_trabalhista .columns

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
            base_automacao_trabalhista  = base_automacao_trabalhista .withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)



# COMMAND ----------

#tratamento das variáveis data timestamp da base de automação

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
base_automacao_trabalhista = convert_timestamp_to_datetime(base_automacao_trabalhista)

# COMMAND ----------

display(base_automacao_trabalhista)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Construção da Base Apta de Acordos do Trabalhista
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when, round

# Inclusão do campo cálculo execução (Base de Automação)
consolidado_modelagem_1 = base_automacao_trabalhista.withColumn(
    "CALCULO_EXECUCAO",
    when(col("Valor Final") == col("VALOR DE CALCULO"), col("TIPO DE CALCULO")).otherwise("Sem Cálculo")
)

# Round the CALCULO_EXECUCAO column to two decimal places
consolidado_modelagem_1 = consolidado_modelagem_1.withColumn("Valor Final", round(col("Valor Final"), 2))

# COMMAND ----------

import pandas as pd


# COMMAND ----------

pip install openpyxl

# COMMAND ----------

#Importação base de Pagamentos 
pagamentos_1 = pd.read_excel("/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_20240401.xlsx", sheet_name = "PAGAMENTOS", header=5)

# COMMAND ----------

import pandas as pd

# Convertendo para datetime e definindo erros como 'coerce' para tratar datas inválidas
pagamentos_1['DATA LIMITE DE PAGAMENTO'] = pd.to_datetime(pagamentos_1['DATA LIMITE DE PAGAMENTO'], errors='coerce')
pagamentos_1['DATA DE VENCIMENTO'] = pd.to_datetime(pagamentos_1['DATA DE VENCIMENTO'], errors='coerce')
pagamentos_1['DATA EFETIVA DO PAGAMENTO'] = pd.to_datetime(pagamentos_1['DATA EFETIVA DO PAGAMENTO'], errors='coerce')

# Print dos tipos de dados para verificar se os dados foram convertidos corretamente


# COMMAND ----------

#Schema manual para a conversão do Dataframe Pyspark


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# Definir o schema manualmente
schema = StructType([
    StructField("PROCESSO - ID", IntegerType(), True),
    StructField("ID DO PAGAMENTO", IntegerType(), True),
    StructField("STATUS DO PROCESSO", StringType(), True),
    StructField("ÁREA DO DIREITO", StringType(), True),
    StructField("DATA REGISTRADO", DateType(), True),
    StructField("EMPRESA", StringType(), True),
    StructField("DATA SOLICITAÇÃO (ESCRITÓRIO)", DateType(), True),
    StructField("DATA LIMITE DE PAGAMENTO", DateType(), True),
    StructField("DATA DE VENCIMENTO", DateType(), True),
    StructField("DATA EFETIVA DO PAGAMENTO", DateType(), True),
    StructField("STATUS DO PAGAMENTO", StringType(), True),
    StructField("TIPO", StringType(), True),
    StructField("SUB TIPO", StringType(), True),
    StructField("VALOR", FloatType(), True),
    StructField("RESPONSÁBILIDADE COMPRADOR PERCENTUAL", FloatType(), True),
    StructField("RESPONSÁBILIDADE VENDEDOR PERCENTUAL", FloatType(), True),
    StructField("VALOR DA MULTA POR DESCUMPRIMENTO DA OBRIGAÇÃO", FloatType(), True),
    StructField("PAGAMENTO ATRAVÉS DE", FloatType(), True),
    StructField("NÚMERO DO DOCUMENTO SAP", FloatType(), True),
    StructField("INDICAR O MOMENTO DA REALIZAÇÃO DO ACORDO", FloatType(), True),
    StructField("PARCELAMENTO ACORDO", StringType(), True),
    StructField("PARCELAMENTO CONDENAÇÃO", StringType(), True),
    StructField("PARTE CONTRÁRIA CPF", StringType(), True),
    StructField("PARTE CONTRÁRIA NOME", StringType(), True),
    StructField("FAVORECIDO (NOME)", StringType(), True),
    StructField("PARTE CONTRÁRIA - CARGO - CARGO (GRUPO)", StringType(), True),
    StructField("CÍVEL MASSA - RESPONSABILIDADE",StringType(), True),
    StructField("ESCRITÓRIO", StringType(), True),
    StructField("CARTEIRA", StringType(), True),
    StructField("ADVOGADO RESPONSÁVEL", StringType(), True),
    StructField("NOVO TERCEIRO", StringType(), True)
])

# Converter DataFrame Pandas para DataFrame PySpark com o esquema definido
hist_pag = spark.createDataFrame(pagamentos_1, schema=schema)

# Exibir o esquema do DataFrame Spark resultante
hist_pag.printSchema()

# COMMAND ----------

# Renomear e tratar campos da Base de Pagamentos para formato Pyspark
historico_de_pagamentos_1 = hist_pag.toDF(*[col.replace(' ', '_') for col in hist_pag.columns])
historico_de_pagamentos_1.toDF(*[col.replace('(', '') for col in hist_pag.columns])
historico_de_pagamentos_1.toDF(*[col.replace(')', '') for col in hist_pag.columns])

# COMMAND ----------

from pyspark.sql.functions import when

# Defina o DataFrame original BASE_PAGAMENTOS
# Suponha que você já tenha lido ou criado este DataFrame

# Aplicar a lógica de transformação
base_pagamentos_2 =historico_de_pagamentos_1.withColumn(
    "ACORDO_PAGAMENTO",
    when(historico_de_pagamentos_1["SUB_TIPO"].isin("ACORDO - TRABALHISTA", "RNO - ACORDO - TRABALHISTA"), 1).otherwise(0)
).withColumn(
    "CONDENACAO_PAGAMENTO",
    when(historico_de_pagamentos_1["SUB_TIPO"].isin("CONDENAÇÃO - TRABALHISTA", "RNO - CONDENAÇÃO - TRABALHISTA"), 1).otherwise(0)
).withColumn(
    "INCONTROVERSO_PAGAMENTO",
    when(historico_de_pagamentos_1["SUB_TIPO"].isin("CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)", "RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)"), 1).otherwise(0)
).distinct()




# COMMAND ----------

# Importar funções do PySpark
from pyspark.sql.functions import col

# Utilizar groupBy e count para obter a frequência
frequencia = base_pagamentos_2.groupBy("INCONTROVERSO_PAGAMENTO").count()



# COMMAND ----------

from pyspark.sql.functions import col, format_number

# Junte os DataFrames com alias
trabalhista_gerencial_base_1 = gerencial_consolidado_trabalhista_precificacao_4.alias("A").join(
    base_calculista_processos.alias("B"),
    col("A.`(PROCESSO) ID`") == col("B.(PROCESSO) ID"),
    "left"
).join(
    estoque_financeiro_trabalhista.alias("C"),
    col("A.`(PROCESSO) ID`") == col("C.ID_PROCESSO"),
    "left"
).join(
    consolidado_modelagem_1.alias("D"),
    col("A.`(PROCESSO) ID`") == col("D.`ID PROCESSO`"),
    "left"
)

# Selecione as colunas e aplique o arredondamento
trabalhista_gerencial_base_1 = trabalhista_gerencial_base_1.select(
    "A.*",
    col("B.VALOR ACÓRDÃO TRT").alias("VALOR_ACORDAO_TRT_1"),
    col("B.`VALOR ACÓRDÃO TRT ATUALIZADO`").alias("VALOR_ACORDAO_TRT_ATUALIZADO_1"),
    col("B.`DATA ACÓRDÃO TRT`").alias("DT_ACORDAO_TRT"),
    col("B.`VALOR ACÓRDÃO TST`").alias("VALOR_ACORDAO_TST_1"),
    col("B.`VALOR ACÓRDÃO TST ATUALIZADO`").alias("VALOR_ACORDAO_TST_ATUALIZADO_1"),
    col("B.`DATA ACÓRDÃO TST`").alias("DT_ACORDAO_TST"),
    col("B.`EXECUÇÃO RECLAMADA - VALOR`").alias("EXECUCAO_RECLAMADA_VALOR_1"),
    col("B.`EXECUÇÃO RECLAMADA - VALOR ATUALIZADO`").alias("EXEC_RECLAMADA_VALOR_ATUALIZADO_1"),
    col("B.`EXECUÇÃO RECLAMADA - DATA`").alias("DT_EXECUCAO_RECLAMADA"),
    col("B.`EXECUÇÃO CALCULO PERITO - VALOR`").alias("EXECUCAO_PER_VALOR_1"),
    col("B.`EXECUÇÃO CALCULO PERITO - VALOR ATUALIZADO`").alias("EXEC_PER_VALOR_ATUALIZADO_1"),
    col("B.`EXECUÇÃO CALCULO PERITO - DATA`").alias("DT_EXECUCAO_PERITO"),
    col("B.`EXECUÇÃO CALCULO HOMOLOGADO - VALOR`").alias("EXECUCAO_HOM_VALOR_1"),
    col("B.`EXECUÇÃO CALCULO HOMOLOGADO - VALOR ATUALIZADO`").alias("EXEC_HOM_VALOR_ATUALIZADO_1"),
    col("B.`EXECUÇÃO CALCULO HOMOLOGADO - DATA`").alias("DT_EXECUCAO_HOMOLOGADO"),
    col("B.`VALOR DA SENTENÇA`").alias("VALOR_SENTENCA"),
    col("B.`VALOR SENTENÇA ATUALIZADA`").alias("VALOR_SENTENCA_ATUALIZADO_1"),
    col("B.`DATA SENTENÇA`").alias("DT_SENTENCA"),
    col("B.`EXECUÇÃO PROVISIONADA - VALOR`").alias("EXECUCAO_PROV_1"),
    col("B.`EXECUÇÃO PROVISIONADA  - VALOR ATUALIZADO`").alias("EXECUCAO_PROV_ATUALIZADO_1"),
    col("B.`EXECUÇÃO PROVISIONADA  - DATA`").alias("DT_EXECUCAO_PROV"),
    col("C.`PROVISAO_TOTAL_PASSIVO_M`").alias("PROVISAO_TOTAL_PASSIVO_M"),
    col("D.`TIPO DE CALCULO`")
).distinct()


# COMMAND ----------

from pyspark.sql.functions import col

trab_alias = trabalhista_gerencial_base_1.alias("TRAB")
pag_alias = base_pagamentos_2.alias("PAG")

trabalhista_gerencial_base_pag = trab_alias.join(
    pag_alias,
    col("TRAB.`(PROCESSO) ID`") == col("PAG.`PROCESSO_-_ID`"),
    "left"
)
trabalhista_gerencial_base_pag = trabalhista_gerencial_base_pag.select(
    "TRAB.*",
    "PAG.`ACORDO_PAGAMENTO`",
    "PAG.`CONDENACAO_PAGAMENTO`",
    "PAG.`INCONTROVERSO_PAGAMENTO`"
)

trabalhista_gerencial_base_pag = trabalhista_gerencial_base_pag.dropDuplicates(["(PROCESSO) ID"])


# COMMAND ----------

trabalhista_gerencial_base_pag.count()

# COMMAND ----------

from pyspark.sql.functions import when

# Aplicar a lógica de condição
trabalhista_gerencial_base_final = trabalhista_gerencial_base_pag.withColumn(
    "MARCACAO_PGTO",
    when(
        (col("ACORDO_PAGAMENTO").isin(list(range(1, 16)))) |
        (col("CONDENACAO_PAGAMENTO").isin(list(range(1, 16)))) |
        (col("INCONTROVERSO_PAGAMENTO").isin(list(range(1, 16)))),
        1
    ).otherwise(0)
).distinct()

# COMMAND ----------

from pyspark.sql.functions import greatest, col, when
from pyspark.sql.types import FloatType

# Definir a coluna VALOR_CALCULO_INTERNO
trabalhista_gerencial_calculista = trabalhista_gerencial_base_final.withColumn(
    "VALOR_CALCULO_INTERNO", 
    when((col("DT_EXECUCAO_PROV") >= "2021-01-01") & (col("DT_EXECUCAO_PROV") == greatest(col("DT_EXECUCAO_PROV"), col("DT_EXECUCAO_HOMOLOGADO"), col("DT_EXECUCAO_PERITO"), col("DT_EXECUCAO_RECLAMADA"), col("DT_ACORDAO_TST"), col("DT_ACORDAO_TRT"), col("DT_SENTENCA"))) & (col("EXECUCAO_PROV_ATUALIZADO_1") != 0), col("EXECUCAO_PROV_ATUALIZADO_1"))
    .when((col("DT_EXECUCAO_HOMOLOGADO") >= "2021-01-01") & (col("DT_EXECUCAO_HOMOLOGADO") == greatest(col("DT_EXECUCAO_PROV"), col("DT_EXECUCAO_HOMOLOGADO"), col("DT_EXECUCAO_PERITO"), col("DT_EXECUCAO_RECLAMADA"), col("DT_ACORDAO_TST"), col("DT_ACORDAO_TRT"), col("DT_SENTENCA"))) & (col("EXEC_HOM_VALOR_ATUALIZADO_1") != 0), col("EXEC_HOM_VALOR_ATUALIZADO_1"))
    .when((col("DT_EXECUCAO_PERITO") >= "2021-01-01") & (col("DT_EXECUCAO_PERITO") == greatest(col("DT_EXECUCAO_PROV"), col("DT_EXECUCAO_HOMOLOGADO"), col("DT_EXECUCAO_PERITO"), col("DT_EXECUCAO_RECLAMADA"), col("DT_ACORDAO_TST"), col("DT_ACORDAO_TRT"), col("DT_SENTENCA"))) & (col("EXEC_PER_VALOR_ATUALIZADO_1") != 0), col("EXEC_PER_VALOR_ATUALIZADO_1"))
    .when((col("DT_EXECUCAO_RECLAMADA") >= "2021-01-01") & (col("DT_EXECUCAO_RECLAMADA") == greatest(col("DT_EXECUCAO_PROV"), col("DT_EXECUCAO_HOMOLOGADO"), col("DT_EXECUCAO_PERITO"), col("DT_EXECUCAO_RECLAMADA"), col("DT_ACORDAO_TST"), col("DT_ACORDAO_TRT"), col("DT_SENTENCA"))) & (col("EXEC_RECLAMADA_VALOR_ATUALIZADO_1") != 0), col("EXEC_RECLAMADA_VALOR_ATUALIZADO_1"))
    .when((col("DT_ACORDAO_TST") >= "2021-01-01") & (col("DT_ACORDAO_TST") == greatest(col("DT_EXECUCAO_PROV"), col("DT_EXECUCAO_HOMOLOGADO"), col("DT_EXECUCAO_PERITO"), col("DT_EXECUCAO_RECLAMADA"), col("DT_ACORDAO_TST"), col("DT_ACORDAO_TRT"), col("DT_SENTENCA"))) & (col("VALOR_ACORDAO_TST_ATUALIZADO_1") != 0), col("VALOR_ACORDAO_TST_ATUALIZADO_1"))
    .when((col("DT_ACORDAO_TRT") >= "2021-01-01") & (col("DT_ACORDAO_TRT") == greatest(col("DT_EXECUCAO_PROV"), col("DT_EXECUCAO_HOMOLOGADO"), col("DT_EXECUCAO_PERITO"), col("DT_EXECUCAO_RECLAMADA"), col("DT_ACORDAO_TST"), col("DT_ACORDAO_TRT"), col("DT_SENTENCA"))) & (col("VALOR_ACORDAO_TRT_ATUALIZADO_1") != 0), col("VALOR_ACORDAO_TRT_ATUALIZADO_1"))
    .when((col("DT_SENTENCA") >= "2021-01-01") & (col("DT_SENTENCA") == greatest(col("DT_EXECUCAO_PROV"), col("DT_EXECUCAO_HOMOLOGADO"), col("DT_EXECUCAO_PERITO"), col("DT_EXECUCAO_RECLAMADA"), col("DT_ACORDAO_TST"), col("DT_ACORDAO_TRT"), col("DT_SENTENCA"))) & (col("VALOR_SENTENCA_ATUALIZADO_1") != 0), col("VALOR_SENTENCA_ATUALIZADO_1"))
    .otherwise(0)  # Se nenhum dos casos anteriores for satisfeito, definimos como 0
    .cast(FloatType())  # Convertendo para tipo float
)


# COMMAND ----------

#Marcar o Tipo de cálculo Utilizado

from pyspark.sql.functions import col, expr, greatest, when

trab_gerencial_calculista1 = trabalhista_gerencial_calculista.withColumn(
    "DATA_MAX",
    greatest(col("DT_EXECUCAO_PROV"), col("DT_EXECUCAO_HOMOLOGADO"), col("DT_EXECUCAO_PERITO"), col("DT_EXECUCAO_RECLAMADA"), col("DT_ACORDAO_TST"), col("DT_ACORDAO_TRT"), col("DT_SENTENCA"))
).withColumn(
    "CALCULO_UTILIZADO",
    when(
        (col("DT_EXECUCAO_PROV") >= "2021-01-01") & (col("DT_EXECUCAO_PROV") == col("DATA_MAX")) &
        (col("EXECUCAO_PROV_ATUALIZADO_1") != 0),
        expr("'EXECUÇÃO HOMOLOGADA'")
    )
    .when(
        (col("DT_EXECUCAO_HOMOLOGADO") >= "2021-01-01") & (col("DT_EXECUCAO_HOMOLOGADO") == col("DATA_MAX")) &
        (col("EXEC_HOM_VALOR_ATUALIZADO_1") != 0),
        expr("'EXECUÇÃO HOMOLOGADA'")
    )
    .when(
        (col("DT_EXECUCAO_PERITO") >= "2021-01-01") & (col("DT_EXECUCAO_PERITO") == col("DATA_MAX")) &
        (col("EXEC_PER_VALOR_ATUALIZADO_1") != 0),
        expr("'EXECUÇÃO PERITO'")
    )
    .when(
        (col("DT_EXECUCAO_RECLAMADA") >= "2021-01-01") & (col("DT_EXECUCAO_RECLAMADA") == col("DATA_MAX")) &
        (col("EXECUCAO_RECLAMADA_VALOR_1") != 0),
        expr("'EXECUÇÃO RECLAMADA'")
    )
    .when(
        (col("DT_ACORDAO_TST") >= "2021-01-01") & (col("DT_ACORDAO_TST") == col("DATA_MAX")) &
        (col("VALOR_ACORDAO_TST_ATUALIZADO_1") != 0),
        expr("'TST'")
    )
    .when(
        (col("DT_ACORDAO_TRT") >= "2021-01-01") & (col("DT_ACORDAO_TRT") == col("DATA_MAX")) &
        (col("VALOR_ACORDAO_TRT_ATUALIZADO_1") != 0),
        expr("'TRT'")
    )
    .when(
        (col("DT_SENTENCA") >= "2021-01-01") & (col("DT_SENTENCA") == col("DATA_MAX")) &
        (col("VALOR_SENTENCA_ATUALIZADO_1") != 0),
        expr("'SENTENÇA'")
    )
    .otherwise("")
).drop("DATA_MAX")



# COMMAND ----------

from pyspark.sql.functions import col, when
#Criação do Campo com a marcação de Fase Consolidada
trabalhista_gerencial_fase = trab_gerencial_calculista1.withColumn("FASE_AJUSTADA",
    when(col("FASE") == "CONHECIMENTO", "CONHECIMENTO")
    .when(col("FASE").isin("RECURSAL", "RECURSAL TRT", "RECURSAL TST"), "RECURSAL")
    .when(
        col("FASE").isin(
            "EXECUÇÃO", "EXECUÇÃO - TRT", "EXECUÇÃO - TST", "EXECUÇÃO DEFINITIVA",
            "EXECUÇÃO PROVISORIA (TRT)", "EXECUÇÃO PROVISORIA (TST)",
            "EXECUÇÃO DEFINITIVA (TRT)", "EXECUÇÃO DEFINITIVA (TST)",
            "EXECUÇÃO DEFINITIVA PROSSEGUIMENTO", "EXECUÇÃO PROVISÓRIA",
            "EXECUÇÃO PROVISÓRIA PROSSEGUIMENTO"
        ),
        "EXECUÇÃO"
    )
    .otherwise("OUTROS")
)

# COMMAND ----------

from pyspark.sql.functions import col, when, sum

# MARCAÇÃO DE CAMPO PARA COMPOSIÇÃO DO VALOR DE RISCO
trabalhista_gerencial_risco = (trabalhista_gerencial_fase
    .withColumn("VALOR_DO_RISCO",
        when(col("FASE_AJUSTADA") == "CONHECIMENTO", col("PROVISAO_TOTAL_PASSIVO_M"))
        .when((col("FASE_AJUSTADA") == "RECURSAL") & (col("VALOR_CALCULO_INTERNO") != 0), col("VALOR_CALCULO_INTERNO"))
        .when((col("FASE_AJUSTADA") == "RECURSAL") & (col("VALOR_CALCULO_INTERNO") == 0), col("PROVISAO_TOTAL_PASSIVO_M"))
        .when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("TIPO DE CALCULO").isin("Cálculo Homologado","Cálculo Reclamada","Perito")) & (col("VALOR_CALCULO_INTERNO") != 0), col("VALOR_CALCULO_INTERNO"))
        .when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("TIPO DE CALCULO").isin("Cálculo Homologado","Cálculo Reclamada","Perito")) & (col("VALOR_CALCULO_INTERNO") == 0), col("PROVISAO_TOTAL_PASSIVO_M"))
        .when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("VALOR_CALCULO_INTERNO") != 0), col("VALOR_CALCULO_INTERNO"))
        .otherwise(col("PROVISAO_TOTAL_PASSIVO_M"))
    )
)

# MARCAÇÃO DA FLAG DE RISCO NULO
base_apta_filtros_valid = (trabalhista_gerencial_risco
    .withColumn("VALIDADOR",
        when(col("VALOR_DO_RISCO") == col("VALOR_CALCULO_INTERNO"), "OK")
        .otherwise("ERRADO")
    )
)

# DEFINIÇÃO ENTRE VALOR CALCULISTA OU PROVISÃO COM NAS FEATURES FASE_AJUSTADA, VALOR_CALCULO_INTERNO, PROVISAO_CALCULISTA
trabalhista_gerencial_risco1 = (trabalhista_gerencial_risco
    .withColumn("PROVISAO_CALCULISTA",
        when(col("FASE_AJUSTADA") == "CONHECIMENTO", "PROVISÃO")
        .when((col("FASE_AJUSTADA") == "RECURSAL") & (col("VALOR_CALCULO_INTERNO") != 0), "CALCULISTA")
        .when((col("FASE_AJUSTADA") == "RECURSAL") & (col("VALOR_CALCULO_INTERNO") == 0), "PROVISÃO")
        .when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("TIPO DE CALCULO").isin("Cálculo Homologado","Reclamada","Perito")), "CALCULISTA")
        .when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("VALOR_CALCULO_INTERNO") != 0), "CALCULISTA")
        .when((col("FASE_AJUSTADA") == "EXECUÇÃO") & ~col("TIPO DE CALCULO").isin("Cálculo Homologado","Reclamada","Perito") & (col("VALOR_CALCULO_INTERNO") != 0), "PROVISÃO")
        .otherwise("PROVISÃO")
    )
)

# MARCAÇÃO DE PROCESSOS QUE UTILIZAM VALOR DO CALCULISTA OU VALOR DE PROVISÃO
trabalhista_gerencial_risco2 = (trabalhista_gerencial_risco1
    .withColumn("PROVISAO_CALCULISTA",
        when(col("VALOR_CALCULO_INTERNO") != 0, "CALCULISTA")
        .otherwise("PROVISÃO")
    )
)



# COMMAND ----------

trabalhista_gerencial_risco2.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Inserir informações estratégicas sobre a negociação dos processos e cálculo das alçadas time Trabalhista

# COMMAND ----------



# Campo para informar a necessidade de avaliação do Time Trabalhista Acordos
trabalhista_gerencial_risco3 = trabalhista_gerencial_risco2.withColumn("NECESSITA_AVALIACAO", 
                                when(col("VALOR_CALCULO_INTERNO") > col("PROVISAO_TOTAL_PASSIVO_M"), "SIM")
                                .otherwise(when(col("VALOR_CALCULO_INTERNO") < col("PROVISAO_TOTAL_PASSIVO_M"), "NÃO")))

# Step 2: Apply the second transformation
trabalhista_gerencial_risco4 = trabalhista_gerencial_risco3.withColumn("VALOR_DE_RISCO1",
                                             when((col("VALOR DA CAUSA") < col("VALOR_DO_RISCO")) & (col("FASE_AJUSTADA") == "CONHECIMENTO"), col("VALOR DA CAUSA"))
                                             .otherwise(when(col("VALOR DA CAUSA") == 0, col("VALOR DA CAUSA"))
                                             .otherwise(col("VALOR_DO_RISCO"))))

# Step 3: Apply the third transformation
trabalhista_gerencial_risco5 = trabalhista_gerencial_risco4.withColumn("ALCADA_1",
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
trabalhista_gerencial_risco7 = trabalhista_gerencial_risco6.withColumn("ALCADA_3",
                                             when((col("FASE_AJUSTADA") == "CONHECIMENTO"), col("VALOR_DE_RISCO1") * 0.85)
                                             .otherwise(when((col("FASE") == "RECURSAL TRT") & (col("VALOR_DE_RISCO1") < 100000), col("VALOR_DE_RISCO1") * 0.75)
                                             .otherwise(when((col("FASE") == "RECURSAL TST") & (col("VALOR_DE_RISCO1") < 100000), col("VALOR_DE_RISCO1") * 0.90)
                                             .otherwise(when((col("FASE") == "RECURSAL TRT") & (col("VALOR_DE_RISCO1") > 100000), col("VALOR_DE_RISCO1") * 0.70)
                                             .otherwise(when((col("FASE") == "RECURSAL TST") & (col("VALOR_DE_RISCO1") > 100000), col("VALOR_DE_RISCO1") * 0.80)
                                             .otherwise(when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("CALCULO_UTILIZADO") == "EXECUÇÃO HOMOLOGADA"), col("VALOR_DE_RISCO1") * 0.95)
                                             .otherwise(col("VALOR_DE_RISCO1") * 0.90)))))))

# Step 6: Check the risk values
df_valid_risco = trabalhista_gerencial_risco7.groupBy().agg(sum("VALOR_DE_RISCO1").alias("RISCO"),
                                               sum("ALCADA_1").alias("ALCADA1"),
                                               sum("ALCADA_2").alias("ALCADA2"),
                                               sum("ALCADA_3").alias("ALCADA3"),
                                               sum("PROVISAO_TOTAL_PASSIVO_M").alias("PROVISAO"))



# COMMAND ----------

# Seleção de nomes advogados contumazes
from pyspark.sql.functions import col

advogado_trat_0 = trabalhista_gerencial_risco7.filter(
    (col('ADVOGADO DA PARTE CONTRÁRIA').like('%MARCOS ROBERTO DIAS%')) |
    (col('ADVOGADO DA PARTE CONTRÁRIA').like('%SANDRA CRISTINA DIAS%')) |
    (col('ADVOGADO DA PARTE CONTRÁRIA').like('%DANIELLE CRISTINA VIEIRA DE SOUZA DIAS%')) |
    (col('ADVOGADO DA PARTE CONTRÁRIA').like('%THIAGO MARTINS RABELO%'))
)

# Remoção dos nomes duplicados da base
advogado_trat_0 = advogado_trat_0.dropDuplicates(['ADVOGADO DA PARTE CONTRÁRIA'])

# Seleção de nomes dos advogados contumazes sem a duplicação - lista a ser inserida na base para tratamento
advogado_trat_01 = advogado_trat_0.select('ADVOGADO DA PARTE CONTRÁRIA')


# COMMAND ----------


# Marcação processo MRD
trabalhista_gerencial_valores = trabalhista_gerencial_risco7.withColumn(
    'CLASSIFICADOR_MRD',
    when(
        col('ADVOGADO DA PARTE CONTRÁRIA').isin(
            'MARCOS ROBERTO DIAS',
            'ADVOGADO MARCOS ROBERTO DIAS',
            ' MARCOS ROBERTO DIAS',
            'MARCOS ROBERTO DIAS  - 87.946 MG',
            'MARCOS ROBERTO DIAS - 0087946 MG',
            'MARCOS ROBERTO DIAS - 159843',
            'MARCOS ROBERTO DIAS - 87946 MG',
            'ALESSANDRA CRISTINA DIAS',
            'ALESSANDRA CRISTINA DIAS - 144802N/MG',
            'ALESSANDRA CRISTINA DIAS - VV',
            'DANIELLE CRISTINA VIEIRA DE SOUZA DIAS',
            'DANIELLE CRISTINA VIEIRA DE SOUZA DIAS - 116893N/MG',
            'THIAGO MARTINS RABELO',
            'THIAGO MARTINS RABELO - 154211N/MG'
        ),
        'MRD'
    ).otherwise('NÃO')
)





# COMMAND ----------

from pyspark.sql.functions import col, when

terceiros_inelegiveis = [
    'ACHEI MONTADOR RIGONI INTERMEDIACOES DE NEGOCIOS LTDA',
    'CHAO BRASIL LOGISTICA E DISTRIBUICAO EIRELI',
    'ESQUADRA - TRANSPORTE DE VALORES & SEGURANÇA LTDA',
    'EXPRESSO RIO TRANSPORTE E MONTAGENS',
    'MHB LOGISTICA DE TRANSPOTE LTDA',
    'MSN LOGISTICA LTDA',
    'PRIME MONTAGENS EIRELI ',
    'PRIME MONTAGENS EIRELI',
    'PRIME SP MONTAGENS EIRELI',
    'POWER - SEGURANCA E VIGILANCIA LTDA',
    'POWER - SEGURANÇA E VIGILANCIA LTDA',
    'ROYALLE SERVICOS DE TRANSPORTE E MONTAGEM DE MOVEIS LTDA',
    'TRM TRANSPORTES E SERVICOS'
]

terceiros_elegiveis_apos_recursal = [
    'DMF SINALIZACAO VIARIA LTDA ME',
    'HL TRANSPORTE E MONTAGENS',
    'MARCO ANTÔNIO SILVA SOBRINHO ME',
    'MASS TRANSPORTES E LOGISTICA LTDA',
    'MBS CARGAS E DESCARGAS EIRELI',
    'MM TRANSPORTADORA LTDA',
    'RGL CARGAS E DESCARGAS',
    'R G LEITE CARGA E DESCARGAS',
    'ROBSON E PAULA BERUTH TRANSPORTADORA LTDA',
    'RUSSELL MONTAGEM DE MOVEIS LTDA',
    'SAME LOGISTICA E TRANSPORTES LTDA',
    'TRANS RUSSELL LOCAÇÃO E SERVIÇOS'
]

trabalhista_gerencial_valores_1 = trabalhista_gerencial_valores.withColumn(
    "TERCEIRO_INELEGIVEL",
    when(col("NOVO TERCEIRO").isin(terceiros_inelegiveis), "Terceiro totalmente inelegível")
    .when(col("NOVO TERCEIRO").isin(terceiros_elegiveis_apos_recursal), "Terceiro elegível apenas somente após a fase recursal (com sentença condenatória)")
    .otherwise("Sem Restrição")
)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Filtrar os dados com base na coluna 'STATUS'
gerencial_consolidado_trabalhista_precificacao_1 = gerencial_consolidado_trabalhista_precificacao.filter(col('STATUS').isin('ATIVO', 'REATIVADO', 'PRÉ CADASTRO'))

# Criar a coluna 'RESPONS_SOCIO_PERCENTUAL' com a fórmula adaptada
gerencial_consolidado_trabalhista_precificacao_1 = gerencial_consolidado_trabalhista_precificacao_1.withColumn('RESPONS_SOCIO_PERCENTUAL',
                                                                           (regexp_replace(col('RESPONSÁBILIDADE SÓCIO PERCENTUAL'), '[^0-9.]', '') / 1).cast('double'))

# COMMAND ----------

from pyspark.sql.functions import col, when

# Identificação se o processo é compartilhado com a FK
trabalhista_gerencial_valores_2 = trabalhista_gerencial_valores_1.withColumn(
    "COMPARTILHADO_FK",
    when(
        (col("EMPRESA").isin("NOVA CASA BAHIA S/A (ATUAL VIA VAREJO)", "INDÚSTRIA DE MÓVEIS BARTIRA LTDA")) &
        (col("RESPONS_SOCIO_PERCENTUAL") > 0),
        "SIM"
    )
    .when(
        (col("EMPRESA") == "CASAS BAHIA ONLINE") &
        (col("RESPONS_SOCIO_PERCENTUAL") >= 78),
        "SIM"
    )
    .otherwise("NAO")
)

# Funcionário ativo e inativo - Feature Data de Dispensa
trabalhista_gerencial_trat = trabalhista_gerencial_valores_2.withColumn(
    "FUNCIONARIO_ATIVO",
    when(col("PARTE CONTRÁRIA DATA DISPENSA ").isNull(), "ATIVO")
    .otherwise("INATIVO")
)

# Classificador MRD
trabalhista_gerencial_trat_1 = trabalhista_gerencial_trat.withColumn(
    "MRD_EXECUCAO_RECURSAL",
    when(
        (col("FASE_AJUSTADA").isin("EXECUÇÃO", "RECURSAL")) &
        (col("CLASSIFICADOR_MRD") == "MRD"),
        "SIM"
    )
    .otherwise("NÃO")
)

# Marcação da flag de risco nulo
trabalhista_gerencial_trat_2 = trabalhista_gerencial_trat_1.withColumn(
    "RISCO_NULO",
    when(col("VALOR_DE_RISCO1") <= 0, "NULO")
    .otherwise("")
)



# COMMAND ----------

# Frequência de classificador MRD
trabalhista_gerencial_trat_2.groupBy('COMPARTILHADO_FK').count().show()

# COMMAND ----------

import re

# Define a string com o nome da variável
column_name = "`DEFINIÇÃO DE ESTRATÉGIA - AUDIÊNCIA - ESTRATÉGIA (ACORDO/DEFESA)`"

# Remove os caracteres especiais utilizando expressões regulares
cleaned_column_name = re.sub(r'\W+', '_', column_name)

print(cleaned_column_name)


# COMMAND ----------

# Importar módulos necessários do PySpark
from pyspark.sql import functions as F

# Renomear a coluna no DataFrame
trabalhista_gerencial_trat_2= trabalhista_gerencial_trat_2.withColumnRenamed('DEFINIÇÃO DE ESTRATÉGIA - AUDIÊNCIA - ESTRATÉGIA (ACORDO/DEFESA)', '_DEFINIÇÃO_DE_ESTRATÉGIA_AUDIÊNCIA_ESTRATÉGIA_ACORDO_DEFESA_')




# COMMAND ----------



# Selecionar as colunas e renomeá-las conforme a organização desejada
trabalhista_gerencial_org = trabalhista_gerencial_trat_2.select(
    "(PROCESSO) ID",
    "PASTA",
    "ÁREA DO DIREITO",
    "SUB-ÁREA DO DIREITO",
    "AÇÃO",
    "GRUPO",
    "ESFERA",
    "DATA REGISTRADO",
    "STATUS",
    "CARTEIRA",
    "EMPRESA",
    "ADVOGADO RESPONSÁVEL",
    "VALOR DA CAUSA",
    "(PROCESSO) ESTADO",
    "(PROCESSO) COMARCA",
    "(PROCESSO) FORO/TRIBUNAL/ÓRGÃO",
    "(PROCESSO) VARA/ÓRGÃO",
    "DISTRIBUIÇÃO",
    "NÚMERO DO PROCESSO",
    "PARTE CONTRÁRIA NOME",
    "ESCRITÓRIO",
    "CLASSIFICAÇÃO",
    "PARTE CONTRÁRIA DATA ADMISSÃO",
    "PARTE CONTRÁRIA DATA DISPENSA ",
    "PARTE CONTRÁRIA - CARGO - CARGO (GRUPO)",
    "PARTE CONTRÁRIA - CARGO - CARGO (SUB-GRUPO)",
    "PARTE CONTRÁRIA CPF",
    "DATA DE ENCERRAMENTO NO TRIBUNAL",
    "DATA DE REGISTRO DO ENCERRAMENTO",
    "DATA DE SOLICITAÇÃO DE ENCERRAMENTO",
    "MOTIVO DE ENCERRAMENTO ",
    "DATA DA BAIXA PROVISÓRIA",
    "MOTIVO DA BAIXA PROVISÓRIA",
    "PARTE CONTRÁRIA - MOTIVO DO DESLIGAMENTO",
    "MOTIVO DA CONDENAÇÃO",
    "FASE",
    "CADASTRANTE",
    "DATA DA REATIVAÇÃO",
    "ADVOGADO DA PARTE CONTRÁRIA",
    "MATRICULA",
    "DATA AUDIENCIA INICIAL",
    "RESPONSÁBILIDADE EMPRESA PERCENTUAL",
    "RESPONSÁBILIDADE SÓCIO PERCENTUAL",
    "TERCEIRO PRINCIPAL",
    "OUTRAS PARTES / NÃO-CLIENTES",
    "PARTE CONTRÁRIA CARGO",
    "CENTRO DE CUSTO / ÁREA DEMANDANTE - UNIDADE",
    "CENTRO DE CUSTO / ÁREA DEMANDANTE - CÓDIGO",
    "FILIAL48",
    "BANDEIRA",
    "RESPONSÁVEL DIRETORIA",
    "DIRETORIA",
    "REGIONAL",
    "CENTRO DE CUSTO / ÁREA DEMANDANTE - NOME",
    "CC DO SÓCIO",
    "DEMITIDOS POR ESTRUTURAÇÃO",
    "DATA DE REABERTURA",
    "DATA DE RECEBIMENTO",
    "TIPO DE CONTINGÊNCIA",
    "ASSISTENTE JURIDICO",
    "RESPONSÁBILIDADE FIXA",
    "PAGEREPORTOUTROSPARTESAUTORCPFCNPJ",
    "PAGEREPORTOUTROSPARTESREUCPFCNPJ",
    "PEDIDO - VALOR ATUALIZADO",
    "NATUREZA OPERACIONAL",
    "OBSERVAÇÕES DA TRATATIVA",
    "NOVO TERCEIRO",
    "CNPJ TERCEIRO PRINCIPAL",
    "VALOR PROVISIONADO",
    "VALOR PROVISIONADO ATUALIZADO",
    "ASSUNTO / SUB ASSUNTO70",
    "ASSUNTO / SUB ASSUNTO71",
    "OBSERVAÇÃO DE ENCERRAMENTO",
    "RNO 2020",
    "STATUS EM FOLHA",
    "POSSUI OUTRA AÇÃO?",
    "ADVOGADO OFENSOR",
    "LISTA DE ADVOGADO OFENSOR",
    "PEDIDO",
    "SALDO REMOTO",
    "ADVOGADO ESCRITORIO",
    "GRUPO DE TERCEIROS",
    "POSSÍVEIS TESTEMUNHAS (MATRÍCULA - CPF) - GRUPO",
    "POSSÍVEIS TESTEMUNHAS 2 (MATRÍCULA - CPF)",
    "DEFINIÇÃO DE ESTRATÉGIA - CADASTRO - ESTRATÉGIA (ACORDO/DEFESA)",
    "DEFINIÇÃO DE ESTRATÉGIA - CADASTRO - JUSTIFICATIVA (ACORDO/DEFESA)",
    "DEFINIÇÃO DE ESTRATÉGIA - DOCUMENTOS - ESTRATÉGIA (ACORDO/DEFESA)",
    "DEFINIÇÃO DE ESTRATÉGIA - DOCUMENTOS - JUSTIFICATIVA (ACORDO/DEFESA)",
    "DEFINIÇÃO DE ESTRATÉGIA - DOCUMENTOS - PERCENTUAL DE ACORDO (%)",
    "DEFINIÇÃO DE ESTRATÉGIA - PRÉ AUDIÊNCIA - ESTRATÉGIA (ACORDO/DEFESA)",
    "DEFINIÇÃO DE ESTRATÉGIA - PRÉ AUDIÊNCIA - JUSTIFICATIVA (ACORDO/DEFESA)",
    "DEFINIÇÃO DE ESTRATÉGIA - PRÉ AUDIÊNCIA - PERCENTUAL DE ACORDO (%)",
    "DEFINIÇÃO DE ESTRATÉGIA  - AUDIÊNCIA - ESTRATÉGIA (ACORDO/DEFESA)",
    "DEFINIÇÃO DE ESTRATÉGIA - AUDIÊNCIA - JUSTIFICATIVA (ACORDO/DEFESA)",
    "DEFINIÇÃO DE ESTRATÉGIA - AUDIÊNCIA - PERCENTUAL DE ACORDO (%)",
    "DEFINIÇÃO DE ESTRATÉGIA - DECISÃO - ESTRATÉGIA (ACORDO/DEFESA)",
    "DEFINIÇÃO DE ESTRATÉGIA - DECISÃO - JUSTIFICATIVA (ACORDO/DEFESA)",
    "REGRA DE PROVISÃO - DEFINIR ESTRATÉGIA DE ATUAÇÃO (ACORDO/DEFESA)",
    "ÚLTIMA POSIÇÃO | ESTRATÉGIA - JUSTIFICATIVA (ACORDO/DEFESA)",
    "JUSTIFICATIVA DO AJUSTE DE ESTRATÉGIA DE ATUAÇÃO",
    "FILIAL100",
    "SEGREDO DE JUSTIÇA",
    "ÁREA INTERNA",
    "ÁREA INTERNA - ESTRATÉGICO",
    "REGRAS MACRO TRABALHISTA",
    "PARA QUAL STATUS DESEJA ALTERAR?",
    "MOTIVO DA ALTERAÇÃO DE STATUS",
    "RESPONS_SOCIO_PERCENTUAL",
    "VALOR_ACORDAO_TRT_1",
    "VALOR_ACORDAO_TRT_ATUALIZADO_1",
    "DT_ACORDAO_TRT",
    "VALOR_ACORDAO_TST_1",
    "VALOR_ACORDAO_TST_ATUALIZADO_1",
    "DT_ACORDAO_TST",
    "EXECUCAO_RECLAMADA_VALOR_1",
    "EXEC_RECLAMADA_VALOR_ATUALIZADO_1",
    "DT_EXECUCAO_RECLAMADA",
    "EXECUCAO_PER_VALOR_1",
    "EXEC_PER_VALOR_ATUALIZADO_1",
    "DT_EXECUCAO_PERITO",
    "EXECUCAO_HOM_VALOR_1",
    "EXEC_HOM_VALOR_ATUALIZADO_1",
    "DT_EXECUCAO_HOMOLOGADO",
    "VALOR_SENTENCA",
    "VALOR_SENTENCA_ATUALIZADO_1",
    "DT_SENTENCA",
    "EXECUCAO_PROV_1",
    "EXECUCAO_PROV_ATUALIZADO_1",
    "DT_EXECUCAO_PROV",
    "PROVISAO_TOTAL_PASSIVO_M",
    "TIPO DE CALCULO",
    "ACORDO_PAGAMENTO",
    "CONDENACAO_PAGAMENTO",
    "INCONTROVERSO_PAGAMENTO",
    "MARCACAO_PGTO",
    "VALOR_CALCULO_INTERNO",
    "CALCULO_UTILIZADO",
    "FASE_AJUSTADA",
    "VALOR_DO_RISCO",
    "PROVISAO_CALCULISTA",
    "NECESSITA_AVALIACAO",
    "VALOR_DE_RISCO1",
    "ALCADA_1",
    "ALCADA_2",
    "ALCADA_3",
    "CLASSIFICADOR_MRD",
    "TERCEIRO_INELEGIVEL",
    "COMPARTILHADO_FK",
    "FUNCIONARIO_ATIVO",
    "MRD_EXECUCAO_RECURSAL",
    "RISCO_NULO"
)






# COMMAND ----------

from pyspark.sql.functions import col

# SELEÇÃO DE NOMES ADVOGADOS CONTUMAZES
advo_estrategia =trabalhista_gerencial_org.filter(
    (col("ADVOGADO DA PARTE CONTRÁRIA").like("%ALAN HONJOYA%")) |
    (col("ADVOGADO DA PARTE CONTRÁRIA").like("%ANDRESSA RAYSSA DE SOUZA%")) |
    (col("ADVOGADO DA PARTE CONTRÁRIA").like("%CARLOS EDUARDO JORGE BERNARDINI%")) |
    (col("ADVOGADO DA PARTE CONTRÁRIA").like("%DENER MANGOLIN%")) |
    (col("ADVOGADO DA PARTE CONTRÁRIA").like("%EDUARDO ZIPPIN KNIJNIK%")) |
    (col("ADVOGADO DA PARTE CONTRÁRIA").like("%FELIPPE AUGUSTO%")) |
    (col("ADVOGADO DA PARTE CONTRÁRIA").like("%LARISSA CANTO AZEVEDO%")) |
    (col("ADVOGADO DA PARTE CONTRÁRIA").like("%RICARDO MIRICO%")) |
    (col("ADVOGADO DA PARTE CONTRÁRIA").like("%HENRIQUE LUIZ DOS SANTOS%"))
)

# REMOÇÃO DOS NOMES DUPLICADOS DA BASE
advo_estrategia_dedup = advo_estrategia.dropDuplicates(["ADVOGADO DA PARTE CONTRÁRIA"])




# COMMAND ----------

advo_trat_2 = advo_estrategia_dedup.select("ADVOGADO DA PARTE CONTRÁRIA").orderBy("ADVOGADO DA PARTE CONTRÁRIA")

# COMMAND ----------

display(advo_trat_2)

# COMMAND ----------


from pyspark.sql.functions import col, when


# Defina a lógica de mapeamento para a coluna ADVOGADO_CONHECIMENTO
ADVOGADO_CONHECIMENTO_expr = (
    when(
        (col("ADVOGADO DA PARTE CONTRÁRIA").isin(
            'ANDRESSA RAYSSA DE SOUZA - 172788 MG',
            'ANDRESSA RAYSSA DE SOUZA - 58741 SC',
            'ANDRESSA RAYSSA DE SOUZA - 58741B SC',
            'ANDRESSA RAYSSA DE SOUZA - 58741-B SC',
            'ANDRESSA RAYSSA DE SOUZA - 58741B SC',
            'FELIPPE AUGUSTO SOUZA SANTOS - 310160 SP',
            'HENRIQUE LUIZ DOS SANTOS NETO - 40247 GO',
            'HENRIQUE LUIZ DOS SANTOS NETO - GO40247',
            'LARISSA CANTO AZEVEDO - 50765 GO')
        ) & (col("FASE_AJUSTADA") == 'CONHECIMENTO'),
        "SIM"
    )
    .when(
        col("ADVOGADO DA PARTE CONTRÁRIA").isin(
            'ALAN HONJOYA - 280907 SP',
            'CARLOS EDUARDO JORGE BERNARDINI',
            'CARLOS EDUARDO JORGE BERNARDINI  - 242.289 SP',
            'CARLOS EDUARDO JORGE BERNARDINI - 242289 SP',
            'DENER MANGOLIN',
            'DENER MANGOLIN  -  222.137 SP',
            'DENER MANGOLIN - 222137 SP',
            'BRUNO DAL-BÓ PAMPLONA',
            'EDUARDO ZIPPIN KNIJNIK',
            'EDUARDO ZIPPIN KNIJNIK - 342.490 SP',
            'EDUARDO ZIPPIN KNIJNIK - 342490 SP',
            'RICARDO MIRICO ARONIS',
            ' RICARDO MIRICO ARONIS - 64079 RS'
        ),
        "SIM"
    )
    .otherwise("NÃO")
)

# Aplique a expressão definida à coluna ADVOGADO_CONHECIMENTO
trabalhista_gerencial_org_1 = trabalhista_gerencial_org\
    .withColumn("ADVOGADO_CONHECIMENTO", ADVOGADO_CONHECIMENTO_expr)






# COMMAND ----------

# MAGIC %md
# MAGIC ####Inclusão de Filtros e Definição de Processos Elegíveis 

# COMMAND ----------

display(trabalhista_gerencial_org_1)

# COMMAND ----------

from pyspark.sql.functions import when, col

# Defina as condições para marcação dos processos
trabalhista_gerencial_valor_1 = trabalhista_gerencial_org_1.withColumn("MOTIVO_APTO", when(trabalhista_gerencial_org_1["REGRA DE PROVISÃO - DEFINIR ESTRATÉGIA DE ATUAÇÃO (ACORDO/DEFESA)"] != "DEFESA", "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_1  = trabalhista_gerencial_org_1.withColumn("MOTIVO_NAO_APTO1", when(trabalhista_gerencial_org_1["REGRA DE PROVISÃO - DEFINIR ESTRATÉGIA DE ATUAÇÃO (ACORDO/DEFESA)"] == "DEFESA", "Processo com Estratégia Defesa + ").otherwise(None))


# Defina as condições para marcação dos processos no dataframe trabalhista_gerencial_valor_2
trabalhista_gerencial_valor_2 = trabalhista_gerencial_valor_1.withColumn("MOTIVO_APTO", 
    when((col("ESCRITÓRIO") != "FAC (TRABALHISTA)") & (col("ESCRITÓRIO") != "FARIA ADVOGADOS E CONSULTORES DE EMPRESAS"), 
    "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_2 = trabalhista_gerencial_valor_2.withColumn("MOTIVO_NAO_APTO2", 
    when((col("ESCRITÓRIO") == "FAC (TRABALHISTA)") | (col("ESCRITÓRIO") == "FARIA ADVOGADOS E CONSULTORES DE EMPRESAS"), 
    "Escritório FAC Faria + ").otherwise(None))


# Defina as condições para marcação dos processos no dataframe trabalhista_gerencial_valor_3
trabalhista_gerencial_valor_3 = trabalhista_gerencial_valor_2.withColumn("MOTIVO_APTO", 
    when(col("PARTE CONTRÁRIA DATA DISPENSA ").isNotNull(), "Processo apto para acordo").otherwise(None))

trabalhista_gerencial_valor_3 = trabalhista_gerencial_valor_3.withColumn("MOTIVO_NAO_APTO3", 
    when(col("PARTE CONTRÁRIA DATA DISPENSA ").isNull(), "Processo sem data de desligamento").otherwise(None))

# Defina as condições para marcação dos processos no dataframe trabalhista_gerencial_valor_4
trabalhista_gerencial_valor_4 = trabalhista_gerencial_valor_3.withColumn("MOTIVO_APTO", 
    when((col("PARTE CONTRÁRIA - MOTIVO DO DESLIGAMENTO") != "DESPEDIDA POR JUSTA CAUSA, PELO EMPREGADOR") & 
    (col("PARTE CONTRÁRIA - MOTIVO DO DESLIGAMENTO") != "JUSTA CAUSA") & 
    (col("PARTE CONTRÁRIA - MOTIVO DO DESLIGAMENTO") != "DISPENSA COM JUSTA CAUSA"), 
    "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_4 = trabalhista_gerencial_valor_4.withColumn("MOTIVO_NAO_APTO4", 
    when(col("FASE_AJUSTADA") == "CONHECIMENTO", "Processo com demissão por justa causa + ").otherwise(None))

# Defina as condições para marcação dos processos no dataframe trabalhista_gerencial_valor_5
trabalhista_gerencial_valor_5 = trabalhista_gerencial_valor_4.withColumn("MOTIVO_APTO", 
    when((~col("SUB-ÁREA DO DIREITO").isin("CONTENCIOSO COLETIVO", "Preventivo Auto de Infração", "PREVENTIVO AUTO DE INFRAÇÃO")) & 
    (~col("NATUREZA OPERACIONAL").isin("SINDICATO / MINISTERIO PUBLICO", "ADMINISTRATIVO")), 
    "Processo apto para acordo").otherwise(None))

trabalhista_gerencial_valor_5 = trabalhista_gerencial_valor_5.withColumn("MOTIVO_NAO_APTO5", 
    when((col("SUB-ÁREA DO DIREITO") == "CONTENCIOSO COLETIVO") | 
    (col("NATUREZA OPERACIONAL") == "SINDICATO / MINISTERIO PUBLICO") | 
    (col("NATUREZA OPERACIONAL") == "ADMINISTRATIVO"), 
    "Processo Contencioso Coletivo, Auto de Infração, Ministerio Publico, Adminsitrativo + ").otherwise(None))


# Remoção de terceiro solvente
trabalhista_gerencial_valor_6 = trabalhista_gerencial_valor_5.withColumn("MOTIVO_APTO", 
    when(col("NATUREZA OPERACIONAL") != "TERCEIRO SOLVENTE", "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_6 = trabalhista_gerencial_valor_6.withColumn("MOTIVO_NAO_APTO6", 
    when(col("NATUREZA OPERACIONAL") == "TERCEIRO SOLVENTE", "Processo de terceiro solvente + ").otherwise(None))


# Remoção de processos com risco nulo
trabalhista_gerencial_valor_7 = trabalhista_gerencial_valor_6.withColumn("MOTIVO_APTO", 
    when(col("VALOR_DE_RISCO1") > 0, "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_7 = trabalhista_gerencial_valor_7.withColumn("MOTIVO_NAO_APTO7", 
    when(col("VALOR_DE_RISCO1") <= 0, "Processo com risco nulo + ").otherwise(None))

# Avaliação de ação trabalhista
trabalhista_gerencial_valor_8 = trabalhista_gerencial_valor_7.withColumn("MOTIVO_APTO", 
    when((col("AÇÃO") == " ") | (col("AÇÃO") == "RECLAMAÇÃO TRABALHISTA"), "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_8 = trabalhista_gerencial_valor_8.withColumn("MOTIVO_NAO_APTO8", 
    when((col("AÇÃO") != " ") & (col("AÇÃO") != "RECLAMAÇÃO TRABALHISTA"), "Processo com ação não negociável + ").otherwise(None))

# Casos em conhecimento com risco superior a R$ 250.000,00
trabalhista_gerencial_valor_9 = trabalhista_gerencial_valor_8.withColumn("MOTIVO_APTO", 
    when((col("FASE_AJUSTADA") == "CONHECIMENTO") & (col("VALOR_DE_RISCO1") <= 250000), "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_9 = trabalhista_gerencial_valor_9.withColumn("MOTIVO_NAO_APTO9", 
    when((col("FASE_AJUSTADA") == "CONHECIMENTO") & (col("VALOR_DE_RISCO1") > 250000), "Processo em conhecimento com risco superior a 250.000 + ").otherwise(None))

# Processo MRD em conhecimento
trabalhista_gerencial_valor_10 = trabalhista_gerencial_valor_9.withColumn("MOTIVO_APTO", 
    when((col("FASE_AJUSTADA") != "CONHECIMENTO") & (col("CLASSIFICADOR_MRD") == "NÃO"), "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_10= trabalhista_gerencial_valor_10.withColumn("MOTIVO_NAO_APTO10", 
    when((col("FASE_AJUSTADA") == "CONHECIMENTO") & (col("CLASSIFICADOR_MRD") == "MRD"), "Processo MRD em Conhecimento + ").otherwise(None))

# Marcação de processos com pagamento
trabalhista_gerencial_valor_11 = trabalhista_gerencial_valor_10.withColumn("MOTIVO_APTO", 
    when(col("MARCACAO_PGTO") == 0, "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_11= trabalhista_gerencial_valor_11.withColumn("MOTIVO_NAO_APTO11", 
    when(col("MARCACAO_PGTO") != 0, "Processo possui pagamento + ").otherwise(None))

    # Processo MRD em conhecimento
trabalhista_gerencial_valor_12 = trabalhista_gerencial_valor_11.withColumn("MOTIVO_APTO", 
    when(col("COMPARTILHADO_FK") == "NAO", "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_12 = trabalhista_gerencial_valor_12.withColumn("MOTIVO_NAO_APTO12", 
    when(col("COMPARTILHADO_FK") == "SIM", "Processo compartilhado FK + ").otherwise(None))

# Processo advogado contumaz
trabalhista_gerencial_valor_13 = trabalhista_gerencial_valor_12.withColumn("MOTIVO_APTO", 
    when(col("ADVOGADO_CONHECIMENTO") == "NÃO", "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_13 = trabalhista_gerencial_valor_13.withColumn("MOTIVO_NAO_APTO13", 
    when(col("ADVOGADO_CONHECIMENTO") != "NÃO", "Processo não apto, advogado contumaz + ").otherwise(None))

# Processo terceiros totalmente inelegíveis
trabalhista_gerencial_valor_14 = trabalhista_gerencial_valor_13.withColumn("MOTIVO_APTO", 
    when(col("TERCEIRO_INELEGIVEL") == "Sem Restrição", "Processo apto para acordo").otherwise(None))
trabalhista_gerencial_valor_14 = trabalhista_gerencial_valor_14.withColumn("MOTIVO_NAO_APTO14", 
    when(col("TERCEIRO_INELEGIVEL") == "Terceiro totalmente inelegível", "Processo Terceiro Totalmente Inelegível").otherwise(None))

trabalhista_gerencial_valor_15 = trabalhista_gerencial_valor_14.withColumn('MOTIVO_NAO_APTO15', 
                   when((col('TERCEIRO_INELEGIVEL') == 'Terceiro elegível apenas somente após a fase recursal (com sentença condenatória)') &
                        (col('FASE_AJUSTADA') == 'CONHECIMENTO') &
                        (col('NOVO TERCEIRO').isin('DMF SINALIZACAO VIARIA LTDA ME',
                                                    'HL TRANSPORTE E MONTAGENS',
                                                    'MARCO ANTÔNIO SILVA SOBRINHO ME',
                                                    'MASS TRANSPORTES E LOGISTICA LTDA',
                                                    'MBS CARGAS E DESCARGAS EIRELI',
                                                    'MM TRANSPORTADORA LTDA',
                                                    'RGL CARGAS E DESCARGAS',
                                                    'R G LEITE CARGA E DESCARGAS',
                                                    'ROBSON E PAULA BERUTH TRANSPORTADORA LTDA',
                                                    'RUSSELL MONTAGEM DE MOVEIS LTDA',
                                                    'SAME LOGISTICA E TRANSPORTES LTDA',
                                                    'TRANS RUSSELL LOCAÇÃO E SERVIÇOS')),
                       'Terceiro elegível apenas somente após a fase recursal (com sentença condenatória)')
                   .otherwise(None))









# COMMAND ----------

from pyspark.sql import functions as F

trabalhista_gerencial_valor_16 = trabalhista_gerencial_valor_15.withColumn("MOTIVO_NAO_APTO", 
                            F.trim(F.concat_ws("", 
                                           F.col("MOTIVO_NAO_APTO1"), 
                                           F.col("MOTIVO_NAO_APTO2"), 
                                           F.col("MOTIVO_NAO_APTO3"), 
                                           F.col("MOTIVO_NAO_APTO4"), 
                                           F.col("MOTIVO_NAO_APTO5"), 
                                           F.col("MOTIVO_NAO_APTO6"), 
                                           F.col("MOTIVO_NAO_APTO7"), 
                                           F.col("MOTIVO_NAO_APTO8"), 
                                           F.col("MOTIVO_NAO_APTO9"), 
                                           F.col("MOTIVO_NAO_APTO10"), 
                                           F.col("MOTIVO_NAO_APTO11"), 
                                           F.col("MOTIVO_NAO_APTO12"), 
                                           F.col("MOTIVO_NAO_APTO13"), 
                                           F.col("MOTIVO_NAO_APTO14"), 
                                           F.col("MOTIVO_NAO_APTO15"))))

# Correctly calculating the substring by subtracting 1 from the length of `MOTIVO_NAO_APTO`
trabalhista_gerencial_valor_16 = trabalhista_gerencial_valor_16.withColumn("MOTIVO_NAO_APTO_", 
                               F.expr("substring(MOTIVO_NAO_APTO, 1, length(MOTIVO_NAO_APTO) - 1)"))

# Using the corrected `MOTIVO_NAO_APTO_` for further logic
trabalhista_gerencial_valor_16 = trabalhista_gerencial_valor_16.withColumn("MOTIVO_NAO_APTO_2", 
                               F.when(
                                   (F.col("MOTIVO_NAO_APTO_") == "") &
                                   F.col("MOTIVO_NAO_APTO1").isNull() &
                                   F.col("MOTIVO_NAO_APTO2").isNull() &
                                   F.col("MOTIVO_NAO_APTO3").isNull() &
                                   F.col("MOTIVO_NAO_APTO4").isNull() &
                                   F.col("MOTIVO_NAO_APTO5").isNull() &
                                   F.col("MOTIVO_NAO_APTO6").isNull() &
                                   F.col("MOTIVO_NAO_APTO7").isNull() &
                                   F.col("MOTIVO_NAO_APTO8").isNull() &
                                   F.col("MOTIVO_NAO_APTO9").isNull() &
                                   F.col("MOTIVO_NAO_APTO10").isNull() &
                                   F.col("MOTIVO_NAO_APTO11").isNull() &
                                   F.col("MOTIVO_NAO_APTO12").isNull() &
                                   F.col("MOTIVO_NAO_APTO13").isNull() &
                                   F.col("MOTIVO_NAO_APTO14").isNull() &
                                   F.col("MOTIVO_NAO_APTO15").isNull(),
                                   "Processo apto para acordo"
                               ).otherwise(F.col("MOTIVO_NAO_APTO_")))

# Dropping the intermediate columns
trabalhista_gerencial_valor_16 = trabalhista_gerencial_valor_16.drop("MOTIVO_APTO", "MOTIVO_NAO_APTO", "MOTIVO_NAO_APTO_")

# Renaming the final column to the desired name
trabalhista_gerencial_valor_16 = trabalhista_gerencial_valor_16.withColumnRenamed("MOTIVO_NAO_APTO_2", "MOTIVO_NAO_APTO")

# COMMAND ----------

display(trabalhista_gerencial_valor_16)

# COMMAND ----------


from pyspark.sql.functions import col, when

# Defina a coluna ATIVO_EXECUCAO no DataFrame df_20
trabalhista_gerencial_valor_17 = trabalhista_gerencial_valor_16.withColumn("ATIVO_EXECUCAO", 
                         when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("PARTE CONTRÁRIA DATA DISPENSA ").isNull()), "OK")
                         .otherwise("NAO"))


# Defina a coluna PGTO_CONDENACAO no DataFrame df_21
trabalhista_gerencial_valor_18 = trabalhista_gerencial_valor_17.withColumn("PGTO_CONDENACAO", 
                         when((col("FASE_AJUSTADA").isin("EXECUÇÃO", "RECURSAL")) & (col("MARCACAO_PGTO") == 1), "OK")
                         .otherwise("NAO"))



# COMMAND ----------

display(trabalhista_gerencial_valor_18)

# COMMAND ----------

base_precificacao = trabalhista_gerencial_valor_18.toPandas()

# Path to save the CSV file in Databox
caminho_arquivo = "/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_precificacao/base_precificacao_abril_24.csv"

# Save the DataFrame as a CSV file
base_precificacao.to_csv(caminho_arquivo, index=False)

print("Arquivo CSV salvo com sucesso no Databox!")

# COMMAND ----------

from pyspark.sql.functions import col

# Contar quantidade de valores nulos na coluna "coluna_nula"
count_nulls = trabalhista_gerencial_valor_18.filter(col("MOTIVO_NAO_APTO").isNull()).count()

# Exibir o resultado
print("Quantidade de valores nulos na coluna 'coluna_nula':", count_nulls)

# COMMAND ----------


