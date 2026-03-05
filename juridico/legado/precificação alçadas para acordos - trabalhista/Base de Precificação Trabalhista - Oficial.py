# Databricks notebook source
# MAGIC %md
# MAGIC #Base de Precificação Processos Trabalhistas

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1- Importação e Tratamento Bases de Dados 

# COMMAND ----------

# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo. Ex: 20240423
dbutils.widgets.text("nmtabela", "")
dbutils.widgets.text("tb_estoque_trab", "")
#dbutils.widgets.text("calculista_processo", "")
dbutils.widgets.text("calculista_processo", "")
#dbutils.widgets.text("base_automacao", "")

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

nmtabela = dbutils.widgets.get("nmtabela")
#calculista_processo = dbutils.widgets.get("calculista_processo")
nmtabela_pgto = dbutils.widgets.get("nmtabela_pgto")
#base_automacao = dbutils.widgets.get("base_automacao")
tb_estoque_trab = dbutils.widgets.get("tb_estoque_trab")

diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_precificacao/base_precificacao_setembro_2024/'
diretorio_estoque = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/'



arquivo_gerencial = f'TRABALHISTA_GERENCIAL_(CONSOLIDADO)-{nmtabela}.xlsx'
#calculista_processos = f'CALCULISTA_PROCESSO_{calculista_processo}.xlsx'
base_pagamentos = f'HISTORICO_DE-PAGAMENTOS_{nmtabela_pgto}.xlsx'
#arquivo_automacao = f'AutomacaoJunho{base_automacao} - Atualizada.xlsx'
base_estoque = f'tb_estq_financ_trab_{tb_estoque_trab}_f.xlsx'


path_gerencial = diretorio_origem + arquivo_gerencial
#path_calculista_processos = diretorio_origem + calculista_processos 
path_base_pagamentos = diretorio_origem + base_pagamentos
#path_base_automacao = diretorio_origem + arquivo_automacao
path_base_estoque = diretorio_estoque + base_estoque



# COMMAND ----------

# Carrega as planilhas em Spark Data Frames
df_gerencial = read_excel(path_gerencial, "'TRABALHISTA'!A6")
#df_calculista_processos = read_excel(path_calculista_processos, "A6")
df_base_pagamentos = read_excel(path_base_pagamentos, "A6")
#df_base_automacao = read_excel(path_base_automacao, "'Consolidado'!A3")
df_base_estoque = read_excel(path_base_estoque, "A1")

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.1 Importação Individual Base do Calculista 

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re
spark.conf.set("spark.sql.caseSensitive","true")

# Importação da Base do Calculista de Pedidos
df_calculista_processos = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'CALCULISTA_PROCESSO'!A6:BK200000") \
    .load("/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_precificacao/base_precificacao_setembro_2024/CALCULISTA_PROCESSO_24.09.02.xlsx")

# Get the column names
columns = df_calculista_processos.columns

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
            df_calculista_processos = df_calculista_processos.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)

# COMMAND ----------

#listagem variáveis númericas base do calculista processos
variavel_numero = ['VALOR DA SENTENÇA', 'VALOR SENTENÇA ATUALIZADA', 'VALOR ACÓRDÃO TRT', 'VALOR ACÓRDÃO TRT ATUALIZADO', 'VALOR ACÓRDÃO TST', 'VALOR ACÓRDÃO TST ATUALIZADO', 'EXECUÇÃO RECLAMADA - VALOR', 'EXECUÇÃO RECLAMADA - VALOR ATUALIZADO', 'EXECUÇÃO RECLAMANTE - VALOR', 'EXECUÇÃO RECLAMANTE - VALOR ATUALIZADO', 'EXECUÇÃO CALCULO PERITO - VALOR', 'EXECUÇÃO CALCULO PERITO - VALOR ATUALIZADO', 'EXECUÇÃO CALCULO HOMOLOGADO - VALOR', 'EXECUÇÃO CALCULO HOMOLOGADO - VALOR ATUALIZADO', 'EXECUÇÃO PROVISIONADA - VALOR', 'EXECUÇÃO PROVISIONADA  - VALOR ATUALIZADO']

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

for column_name in variavel_numero:
    df_calculista_processos = df_calculista_processos.withColumn(
        column_name, 
        regexp_replace(col(column_name), '[^0-9.]', '').cast('double')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.2 Importação individual Base de Automação

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws
from collections import Counter
import re
spark.conf.set("spark.sql.caseSensitive","true")

# Importação da Base do Calculista de Pedidos
df_base_automacao = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("MaxRowsInMemory", 1000) \
    .option("dataAddress", "'Consolidado'!A3:DN200000") \
    .load("/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_precificacao/base_precificacao_setembro_2024/Automacaoagosto22082024.xlsx")

# Get the column names
columns = df_base_automacao.columns

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
            df_base_automacao = df_base_automacao.withColumnRenamed(old_name, new_name)
    else:
        # If the column name is unique, add it to unique_columns
        unique_columns.append(column)

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.3 Tratamento dados e padronização

# COMMAND ----------

#lista todas as variáveis que devem ser convertidas em data nos respectivos dataframes
df_gerencial_cols = find_columns_with_word(df_gerencial, 'DATA ')
df_calculista_processos_cols = find_columns_with_word(df_calculista_processos, 'DATA ')
df_calculista_processos_cols.extend(['EXECUÇÃO RECLAMADA - DATA', 'EXECUÇÃO RECLAMANTE - DATA','EXECUÇÃO CALCULO PERITO - DATA','EXECUÇÃO CALCULO PERITO - DATA','EXECUÇÃO CALCULO HOMOLOGADO - DATA','EXECUÇÃO PROVISIONADA  - DATA'])
df_base_pagamentos_cols = find_columns_with_word(df_base_pagamentos, 'DATA ')

print("df_gerencial_cols")
print(df_gerencial_cols)
print("\n")
print("df_calculista_processos_cols")
print(df_calculista_processos_cols)
print("\n")
print("df_base_pagamentos_cols")
print(df_base_pagamentos_cols)
print("\n")

# COMMAND ----------

#realiza a conversão das variáveis para formato data
df_gerencial = convert_to_date_format(df_gerencial, df_gerencial_cols)
df_calculista_processos = convert_to_date_format(df_calculista_processos, df_calculista_processos_cols)
df_base_pagamentos = convert_to_date_format(df_base_pagamentos, df_base_pagamentos_cols)

# COMMAND ----------

df_gerencial = remove_acentos(df_gerencial)
df_calculista_processos = remove_acentos(df_calculista_processos)
df_base_pagamentos  = remove_acentos(df_base_pagamentos )


# COMMAND ----------

df_gerencial = df_gerencial.withColumnRenamed("CLASSIFICACAO.", "CLASSIFICACAO")
df_gerencial = df_gerencial.withColumnRenamed("ASSISTENTE JURIDICO.", "ASSISTENTE JURIDICO")


# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
# Criar a coluna 'RESPONS_SOCIO_PERCENTUAL' com a fórmula adaptada
df_gerencial = df_gerencial.withColumn('RESPONS_SOCIO_PERCENTUAL',
                                                                           (regexp_replace(col('RESPONSABILIDADE SOCIO PERCENTUAL'), '[^0-9.]', '') / 1).cast('double'))

# COMMAND ----------

from pyspark.sql.functions import col

# Filtrar os dados com base na coluna 'STATUS'
gerencial_1 = df_gerencial.filter(col('STATUS').isin('ATIVO', 'REATIVADO', 'PRÉ CADASTRO'))



# COMMAND ----------

gerencial_1.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2 - Construção da Base Apta de Acordos do Trabalhista
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when, round

# Inclusão do campo cálculo execução (Base de Automação)
consolidado_modelagem_1 = df_base_automacao.withColumn(
    "CALCULO_EXECUCAO",
    when(col("Valor Final") == col("VALOR DE CALCULO"), col("TIPO DE CALCULO")).otherwise("SEM CALCULO")
)

# Round the CALCULO_EXECUCAO column to two decimal places
consolidado_modelagem_1 = consolidado_modelagem_1.withColumn("Valor Final", round(col("Valor Final"), 2))

# COMMAND ----------

from pyspark.sql.functions import col

# Perform the join operations
trabalhista_gerencial_base_1 = gerencial_1.alias("A").join(
    df_calculista_processos.alias("B"),
    col("A.`(PROCESSO) ID`") == col("B.`(PROCESSO) ID`"),
    "left"
).join(
    df_base_estoque.alias("C"),
    col("A.`(PROCESSO) ID`") == col("C.ID_PROCESSO"),
    "left"
).join(
    consolidado_modelagem_1.alias("D"),
    col("A.`(PROCESSO) ID`") == col("D.`ID PROCESSO`"),
    "left"
)

# Select columns and apply aliasing directly from the joined DataFrame
trabalhista_gerencial_base_1 = trabalhista_gerencial_base_1.select(
    "A.*",
    col("B.`VALOR ACORDAO TRT`").alias("VALOR_ACORDAO_TRT_1"),
    col("B.`VALOR ACORDAO TRT ATUALIZADO`").alias("VALOR_ACORDAO_TRT_ATUALIZADO_1"),
    col("B.`DATA ACORDAO TRT`").alias("DT_ACORDAO_TRT"),
    col("B.`VALOR ACORDAO TST`").alias("VALOR_ACORDAO_TST_1"),
    col("B.`VALOR ACORDAO TST ATUALIZADO`").alias("VALOR_ACORDAO_TST_ATUALIZADO_1"),
    col("B.`DATA ACORDAO TST`").alias("DT_ACORDAO_TST"),
    col("B.`EXECUCAO RECLAMADA - VALOR`").alias("EXECUCAO_RECLAMADA_VALOR_1"),
    col("B.`EXECUCAO RECLAMADA - VALOR ATUALIZADO`").alias("EXEC_RECLAMADA_VALOR_ATUALIZADO_1"),
    col("B.`EXECUCAO RECLAMADA - DATA`").alias("DT_EXECUCAO_RECLAMADA"),
    col("B.`EXECUCAO CALCULO PERITO - VALOR`").alias("EXECUCAO_PER_VALOR_1"),
    col("B.`EXECUCAO CALCULO PERITO - VALOR ATUALIZADO`").alias("EXEC_PER_VALOR_ATUALIZADO_1"),
    col("B.`EXECUCAO CALCULO PERITO - DATA`").alias("DT_EXECUCAO_PERITO"),
    col("B.`EXECUCAO CALCULO HOMOLOGADO - VALOR`").alias("EXECUCAO_HOM_VALOR_1"),
    col("B.`EXECUCAO CALCULO HOMOLOGADO - VALOR ATUALIZADO`").alias("EXEC_HOM_VALOR_ATUALIZADO_1"),
    col("B.`EXECUCAO CALCULO HOMOLOGADO - DATA`").alias("DT_EXECUCAO_HOMOLOGADO"),
    col("B.`VALOR DA SENTENCA`").alias("VALOR_SENTENCA"),
    col("B.`VALOR SENTENCA ATUALIZADA`").alias("VALOR_SENTENCA_ATUALIZADO_1"),
    col("B.`DATA SENTENCA`").alias("DT_SENTENCA"),
    col("B.`EXECUCAO PROVISIONADA - VALOR`").alias("EXECUCAO_PROV_1"),
    col("B.`EXECUCAO PROVISIONADA  - VALOR ATUALIZADO`").alias("EXECUCAO_PROV_ATUALIZADO_1"),
    col("B.`EXECUCAO PROVISIONADA  - DATA`").alias("DT_EXECUCAO_PROV"),
    col("C.`PROVISAO_TOTAL_PASSIVO_M`").alias("PROVISAO_TOTAL_PASSIVO_M"),
    col("D.`TIPO DE CALCULO`")
).distinct()

# COMMAND ----------

trabalhista_gerencial_base_1.count()

# COMMAND ----------

from pyspark.sql.functions import when

# Defina o DataFrame original BASE_PAGAMENTOS
# Suponha que você já tenha lido ou criado este DataFrame

# Aplicar a lógica de transformação
df_base_pagamentos_1 = df_base_pagamentos.withColumn(
    "ACORDO_PAGAMENTO",
    when(df_base_pagamentos["SUB TIPO"].isin("ACORDO - TRABALHISTA", "RNO - ACORDO - TRABALHISTA"), 1).otherwise(0)
).withColumn(
    "CONDENACAO_PAGAMENTO",
    when(df_base_pagamentos["SUB TIPO"].isin("CONDENAÇÃO - TRABALHISTA", "RNO - CONDENAÇÃO - TRABALHISTA"), 1).otherwise(0)
).withColumn(
    "INCONTROVERSO_PAGAMENTO",
    when(df_base_pagamentos["SUB TIPO"].isin("CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)", "RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)"), 1).otherwise(0)
).distinct()




# COMMAND ----------

# Supondo que base_pagamentos_2 é o DataFrame que você está utilizando
df_base_pagamentos_1.createOrReplaceTempView("BASE_PAGAMENTOS_2")

# Utilizando SQL em PySpark para sumarizar os valores
df_base_pagamentos_2 = spark.sql("""
    SELECT `PROCESSO - ID`,
           SUM(ACORDO_PAGAMENTO) AS ACORDO_PAGAMENTO,
           SUM(CONDENACAO_PAGAMENTO) AS CONDENACAO_PAGAMENTO,
           SUM(INCONTROVERSO_PAGAMENTO) AS INCONTROVERSO_PAGAMENTO
    FROM BASE_PAGAMENTOS_2
    GROUP BY `PROCESSO - ID`
""")


# COMMAND ----------


trabalhista_gerencial_base_1.createOrReplaceTempView("TRAB")
df_base_pagamentos_2.createOrReplaceTempView("PAG")


# Executar a consulta SQL para a junção e seleção das colunas necessárias
tb_fech_civel_consolidado = spark.sql("""
    SELECT 
        TRAB.*,
        PAG.ACORDO_PAGAMENTO,
        PAG.CONDENACAO_PAGAMENTO,
        PAG.INCONTROVERSO_PAGAMENTO
    FROM TRAB
    LEFT JOIN PAG
    ON TRAB.`(PROCESSO) ID` = PAG.`PROCESSO - ID`

""")


# COMMAND ----------

trabalhista_gerencial_base_pag = tb_fech_civel_consolidado.dropDuplicates(["(PROCESSO) ID"])

# COMMAND ----------

trabalhista_gerencial_base_pag.count()

# COMMAND ----------

from pyspark.sql.functions import when

# Aplicar a lógica de condição
trabalhista_gerencial_base_final = trabalhista_gerencial_base_pag.withColumn(
    "MARCACAO_PGTO",
    when(
        (col("ACORDO_PAGAMENTO").isin(list(range(1, 20)))) |
        (col("CONDENACAO_PAGAMENTO").isin(list(range(1, 20)))) |
        (col("INCONTROVERSO_PAGAMENTO").isin(list(range(1, 20)))),
        1
    ).otherwise(0)
).distinct()

# COMMAND ----------

from pyspark.sql.functions import greatest, col, when
from pyspark.sql.types import FloatType

# Definir a coluna VALOR_CALCULO_INTERNO com Base nos Valores da Base do Calculista
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
        .when((col("FASE_AJUSTADA") == "EXECUÇÃO") & ~col("TIPO DE CALCULO").isin("CALCULO HOMOLOGADO","Reclamada","Perito") & (col("VALOR_CALCULO_INTERNO") != 0), "PROVISÃO")
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
# MAGIC #####2.1 Inserir informações estratégicas sobre a negociação dos processos e cálculo das alçadas time Trabalhista

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

# COMMAND ----------

# Conferência dos valores de alçadas para negociação
df_valid_risco = trabalhista_gerencial_risco7.groupBy().agg(sum("VALOR_DE_RISCO1").alias("RISCO"),
                                               sum("ALCADA_1").alias("ALCADA1"),
                                               sum("ALCADA_2").alias("ALCADA2"),
                                               sum("ALCADA_3").alias("ALCADA3"),
                                               sum("PROVISAO_TOTAL_PASSIVO_M").alias("PROVISAO"))


# COMMAND ----------

display(df_valid_risco)

# COMMAND ----------

# Seleção de nomes advogados contumazes
from pyspark.sql.functions import col

advogado_trat_0 = trabalhista_gerencial_risco7.filter(
    (col('ADVOGADO DA PARTE CONTRARIA').like('%MARCOS ROBERTO DIAS%')) |
    (col('ADVOGADO DA PARTE CONTRARIA').like('%SANDRA CRISTINA DIAS%')) |
    (col('ADVOGADO DA PARTE CONTRARIA').like('%DANIELLE CRISTINA VIEIRA DE SOUZA DIAS%')) |
    (col('ADVOGADO DA PARTE CONTRARIA').like('%THIAGO MARTINS RABELO%'))
)

# Remoção dos nomes duplicados da base
advogado_trat_0 = advogado_trat_0.dropDuplicates(['ADVOGADO DA PARTE CONTRARIA'])

# Seleção de nomes dos advogados contumazes sem a duplicação - lista a ser inserida na base para tratamento
advogado_trat_01 = advogado_trat_0.select('ADVOGADO DA PARTE CONTRARIA')


# COMMAND ----------

display(advogado_trat_01)

# COMMAND ----------


# Marcação processo MRD
trabalhista_gerencial_valores = trabalhista_gerencial_risco7.withColumn(
    'CLASSIFICADOR_MRD',
    when(
        col('ADVOGADO DA PARTE CONTRARIA').isin(
            'MARCOS ROBERTO DIAS',
            'ADVOGADO MARCOS ROBERTO DIAS'
            'MARCOS ROBERTO DIAS - 87946',
            'MARCOS ROBERTO DIAS - 87946',
            ' MARCOS ROBERTO DIAS',
            'MARCOS ROBERTO DIAS  - 87.946 MG',
            'MARCOS ROBERTO DIAS - 0087946 MG',
            'MARCOS ROBERTO DIAS - 159843',
            'MARCOS ROBERTO DIAS - 87946 MG',
            'ALESSANDRA CRISTINA DIAS',
            'ALESSANDRA CRISTINA DIAS - 144802N/MG',
            'ALESSANDRA CRISTINA DIAS - VV',
            'DANIELLE CRISTINA VIEIRA DE SOUZA DIAS',
            'DANIELLE CRISTINA VIEIRA DE SOUZA DIAS - 116893',
            'DANIELLE CRISTINA VIEIRA DE SOUZA DIAS - 116893N/MG',
            'THIAGO MARTINS RABELO',
            'THIAGO MARTINS RABELO - 154211',
            'THIAGO MARTINS RABELO - 154211N/MG'
        ),
        'MRD'
    ).otherwise('NÃO')
)


# COMMAND ----------

display(trabalhista_gerencial_valores)

# COMMAND ----------

from pyspark.sql.functions import col, when

terceiros_inelegiveis = [
    'ACHEI MONTADOR RIGONI INTERMEDIACOES DE NEGOCIOS LTDA',
    'AVANT TRANSPORTES E LOCACAO LTDA',
    'CHAO BRASIL LOGISTICA E DISTRIBUICAO EIRELI',
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
    'ESQUADRA - TRANSPORTE DE VALORES & SEGURANÇA LTDA',
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
    when(col("PARTE CONTRARIA DATA DISPENSA ").isNull(), "ATIVO")
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



# Ordenamento das variáveis da base de acordos 

trabalhista_gerencial_org = trabalhista_gerencial_trat_2.select(
    "(PROCESSO) ID",
    "PASTA",
    "AREA DO DIREITO",
    "SUB-AREA DO DIREITO",
    "ACAO",
    "GRUPO",
    "ESFERA",
    "DATA REGISTRADO",
    "STATUS",
    "CARTEIRA",
    "EMPRESA",
    "ADVOGADO RESPONSAVEL",
    "VALOR DA CAUSA",
    "(PROCESSO) ESTADO",
    "(PROCESSO) COMARCA",
    "(PROCESSO) FORO/TRIBUNAL/ORGAO",
    "(PROCESSO) VARA/ORGAO",
    "DISTRIBUICAO",
    "NUMERO DO PROCESSO",
    "PARTE CONTRARIA NOME",
    "ESCRITORIO",
    "CLASSIFICACAO",
    "PARTE CONTRARIA DATA ADMISSAO",
    "PARTE CONTRARIA DATA DISPENSA ",
    "PARTE CONTRARIA - CARGO - CARGO (GRUPO)",
    "PARTE CONTRARIA - CARGO - CARGO (SUB-GRUPO)",
    "PARTE CONTRARIA CPF",
    "DATA DE ENCERRAMENTO NO TRIBUNAL",
    "DATA DE REGISTRO DO ENCERRAMENTO",
    "DATA DE SOLICITACAO DE ENCERRAMENTO",
    "MOTIVO DE ENCERRAMENTO ",
    "DATA DA BAIXA PROVISORIA",
    "MOTIVO DA BAIXA PROVISORIA",
    "PARTE CONTRARIA - MOTIVO DO DESLIGAMENTO",
    "MOTIVO DA CONDENACAO",
    "FASE",
    "CADASTRANTE",
    "DATA DA REATIVACAO",
    "ADVOGADO DA PARTE CONTRARIA",
    "MATRICULA",
    "DATA AUDIENCIA INICIAL",
    "RESPONSABILIDADE EMPRESA PERCENTUAL",
    "RESPONSABILIDADE SOCIO PERCENTUAL",
    "RESPONS_SOCIO_PERCENTUAL",
    "TERCEIRO PRINCIPAL",
    "OUTRAS PARTES / NAO-CLIENTES",
    "PARTE CONTRARIA CARGO",
    "CENTRO DE CUSTO / AREA DEMANDANTE - UNIDADE",
    "`CENTRO DE CUSTO / AREA DEMANDANTE - CODIGO`",
    "FILIAL48",
    "BANDEIRA",
    "RESPONSAVEL DIRETORIA",
    "DIRETORIA",
    "REGIONAL",
    "CENTRO DE CUSTO / AREA DEMANDANTE - NOME",
    "CC DO SOCIO",
    "DEMITIDOS POR ESTRUTURACAO",
    "DATA DE REABERTURA",
    "DATA DE RECEBIMENTO",
    "TIPO DE CONTINGENCIA",
    "ASSISTENTE JURIDICO",
    "RESPONSABILIDADE FIXA",
    #"PAGE.REPORT.OUTROSPARTESAUTORCPFCNPJ",
    #"`PAGE.REPORT.OUTROSPARTESAUTORCPFCNPJ`",
    "PEDIDO - VALOR ATUALIZADO",
    "NATUREZA OPERACIONAL",
    "OBSERVACOES DA TRATATIVA",
    "NOVO TERCEIRO",
    "CNPJ TERCEIRO PRINCIPAL",
    "VALOR PROVISIONADO",
    "VALOR PROVISIONADO ATUALIZADO",
    "ASSUNTO / SUB ASSUNTO72",
    "ASSUNTO / SUB ASSUNTO73",
    "OBSERVACAO DE ENCERRAMENTO",
    "RNO 2020",
    "STATUS EM FOLHA",
    "POSSUI OUTRA ACAO?",
    "ADVOGADO OFENSOR",
    "LISTA DE ADVOGADO OFENSOR",
    "PEDIDO",
    "SALDO REMOTO",
    "ADVOGADO ESCRITORIO",
    "GRUPO DE TERCEIROS",
    "POSSIVEIS TESTEMUNHAS (MATRICULA - CPF) - GRUPO",
    "POSSIVEIS TESTEMUNHAS 2 (MATRICULA - CPF)",
    "DEFINICAO DE ESTRATEGIA - CADASTRO - ESTRATEGIA (ACORDO/DEFESA)",
    "DEFINICAO DE ESTRATEGIA - CADASTRO - JUSTIFICATIVA (ACORDO/DEFESA)",
    "DEFINICAO DE ESTRATEGIA - DOCUMENTOS - ESTRATEGIA (ACORDO/DEFESA)",
    "DEFINICAO DE ESTRATEGIA - DOCUMENTOS - JUSTIFICATIVA (ACORDO/DEFESA)",
    "DEFINICAO DE ESTRATEGIA - DOCUMENTOS - PERCENTUAL DE ACORDO (%)",
    "DEFINICAO DE ESTRATEGIA - PRE AUDIENCIA - ESTRATEGIA (ACORDO/DEFESA)",
    "DEFINICAO DE ESTRATEGIA - PRE AUDIENCIA - JUSTIFICATIVA (ACORDO/DEFESA)",
    "DEFINICAO DE ESTRATEGIA - PRE AUDIENCIA - PERCENTUAL DE ACORDO (%)",
    "DEFINICAO DE ESTRATEGIA  - AUDIENCIA - ESTRATEGIA (ACORDO/DEFESA)",
    "DEFINICAO DE ESTRATEGIA - AUDIENCIA - JUSTIFICATIVA (ACORDO/DEFESA)",
    "DEFINICAO DE ESTRATEGIA - AUDIENCIA - PERCENTUAL DE ACORDO (%)",
    "DEFINICAO DE ESTRATEGIA - DECISAO - ESTRATEGIA (ACORDO/DEFESA)",
    "DEFINICAO DE ESTRATEGIA - DECISAO - JUSTIFICATIVA (ACORDO/DEFESA)",
    "REGRA DE PROVISAO - DEFINIR ESTRATEGIA DE ATUACAO (ACORDO/DEFESA)",
    "ULTIMA POSICAO | ESTRATEGIA - JUSTIFICATIVA (ACORDO/DEFESA)",
    "JUSTIFICATIVA DO AJUSTE DE ESTRATEGIA DE ATUACAO",
    "FILIAL102",
    "SEGREDO DE JUSTICA",
    "AREA INTERNA",
    "AREA INTERNA - ESTRATEGICO",
    "REGRAS MACRO TRABALHISTA",
    "PARA QUAL STATUS DESEJA ALTERAR?",
    "MOTIVO DA ALTERACAO DE STATUS",
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
    (col("ADVOGADO DA PARTE CONTRARIA").like("%ALAN HONJOYA%")) |
    (col("ADVOGADO DA PARTE CONTRARIA").like("%ANDRESSA RAYSSA DE SOUZA%")) |
    (col("ADVOGADO DA PARTE CONTRARIA").like("%CARLOS EDUARDO JORGE BERNARDINI%")) |
    (col("ADVOGADO DA PARTE CONTRARIA").like("%DENER MANGOLIN%")) |
    (col("ADVOGADO DA PARTE CONTRARIA").like("%EDUARDO ZIPPIN KNIJNIK%")) |
    (col("ADVOGADO DA PARTE CONTRARIA").like("%FELIPPE AUGUSTO%")) |
    (col("ADVOGADO DA PARTE CONTRARIA").like("%LARISSA CANTO AZEVEDO%")) |
    (col("ADVOGADO DA PARTE CONTRARIA").like("%RICARDO MIRICO%")) |
    (col("ADVOGADO DA PARTE CONTRARIA").like("%HENRIQUE LUIZ DOS SANTOS%"))|
    (col("ADVOGADO DA PARTE CONTRARIA").like("PAULO ROBERTO SANTORO%"))
 

)

# REMOÇÃO DOS NOMES DUPLICADOS DA BASE
advo_estrategia_dedup = advo_estrategia.dropDuplicates(["ADVOGADO DA PARTE CONTRARIA"])




# COMMAND ----------

advo_trat_2 = advo_estrategia_dedup.select("ADVOGADO DA PARTE CONTRARIA").orderBy("ADVOGADO DA PARTE CONTRARIA")

# COMMAND ----------

display(advo_trat_2)

# COMMAND ----------


from pyspark.sql.functions import col, when


# Defina a lógica de mapeamento para a coluna ADVOGADO_CONHECIMENTO
ADVOGADO_CONHECIMENTO_expr = (
    when(
        (col("ADVOGADO DA PARTE CONTRARIA").isin(
            #'ANDRESSA RAYSSA DE SOUZA - 172788 MG',
            #'ANDRESSA RAYSSA DE SOUZA - 58741 SC',
            #'ANDRESSA RAYSSA DE SOUZA - 58741B SC',
            #'ANDRESSA RAYSSA DE SOUZA - 58741-B SC',
            #'ANDRESSA RAYSSA DE SOUZA - 58741B SC',
            #'FELIPPE AUGUSTO SOUZA SANTOS - 310160 SP',
            #'HENRIQUE LUIZ DOS SANTOS NETO - 40247 GO',
            #'HENRIQUE LUIZ DOS SANTOS NETO - GO40247',
            'LARISSA CANTO AZEVEDO - 50765 GO')
        ) & (col("FASE_AJUSTADA") == 'CONHECIMENTO'),
        "SIM"
    )
    .when(
        col("ADVOGADO DA PARTE CONTRARIA").isin(
            'ALAN HONJOYA - 280907 SP',
            'CARLOS EDUARDO JORGE BERNARDINI',
            'CARLOS EDUARDO JORGE BERNARDINI  - 242.289 SP',
            'CARLOS EDUARDO JORGE BERNARDINI - 242289',
            'CARLOS EDUARDO JORGE BERNARDINI - 242289 SP',
             'DENER MANGOLIN',
             'DENER MANGOLIN  -  222.137 SP',
             'DENER MANGOLIN - 222137',
             'DENER MANGOLIN - 222137 SP',
            #'BRUNO DAL-BÓ PAMPLONA',
             'EDUARDO ZIPPIN KNIJNIK',
             'EDUARDO ZIPPIN KNIJNIK - 342.490 SP',    
             'EDUARDO ZIPPIN KNIJNIK - 342490 SP',
             'EDUARDO ZIPPIN KNIJNIK - 71366',
            ' RICARDO MIRICO ARONIS - 64079 RS',
            'RICARDO MIRICO ARONIS',
            'PAULO ROBERTO SANTORO SALOMÃO - 199.085 SP'
        ),
        "SIM"
    )
    .otherwise("NÃO")
)


# COMMAND ----------

# Aplique a expressão definida à coluna ADVOGADO_CONHECIMENTO
trabalhista_gerencial_org_1 = trabalhista_gerencial_org\
    .withColumn("ADVOGADO_CONHECIMENTO", ADVOGADO_CONHECIMENTO_expr)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3 - Inclusão de Filtros e Definição de Processos Elegíveis 

# COMMAND ----------

# MAGIC %md
# MAGIC #####3.1 Aplicação das regras para processos aptos

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
trabalhista_gerencial_org_1.createOrReplaceTempView("TB_GERENCIAL_1")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_1 = spark.sql("""
    SELECT *,
           CASE 
               WHEN `REGRA DE PROVISAO - DEFINIR ESTRATEGIA DE ATUACAO (ACORDO/DEFESA)` NOT IN ('DEFESA') THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN `REGRA DE PROVISAO - DEFINIR ESTRATEGIA DE ATUACAO (ACORDO/DEFESA)` IN ('DEFESA') THEN 'Processo com Estratégia Defesa + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO1
    FROM TB_GERENCIAL_1
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_1.createOrReplaceTempView("TB_GERENCIAL_2")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_2 = spark.sql("""
    SELECT *,
           CASE 
               WHEN `ESCRITORIO` NOT IN ('FAC (TRABALHISTA)') THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN `ESCRITORIO` IN ('FAC (TRABALHISTA)') THEN 'Escritório FAC  + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO2
    FROM TB_GERENCIAL_2
""")


# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_2.createOrReplaceTempView("TB_GERENCIAL_3")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_3 = spark.sql("""
    SELECT *,
           CASE 
               WHEN `PARTE CONTRARIA DATA DISPENSA ` IS NOT NULL AND `PARTE CONTRARIA DATA DISPENSA ` != '' THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN `PARTE CONTRARIA DATA DISPENSA ` IS NULL OR `PARTE CONTRARIA DATA DISPENSA ` = '' THEN 'Processo sem data de desligamento + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO3
    FROM TB_GERENCIAL_3
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_3 .createOrReplaceTempView("TB_GERENCIAL_4")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_4 = spark.sql("""
    SELECT *,
           CASE 
               WHEN `PARTE CONTRARIA - MOTIVO DO DESLIGAMENTO` NOT IN ("DESPEDIDA POR JUSTA CAUSA, PELO EMPREGADOR", "JUSTA CAUSA", "DISPENSA COM JUSTA CAUSA") THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN `PARTE CONTRARIA - MOTIVO DO DESLIGAMENTO` IN ("DESPEDIDA POR JUSTA CAUSA, PELO EMPREGADOR", "JUSTA CAUSA", "DISPENSA COM JUSTA CAUSA") AND FASE_AJUSTADA = "CONHECIMENTO" THEN 'Processo com demissão por justa causa + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO4
    FROM TB_GERENCIAL_4
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_4.createOrReplaceTempView("TB_GERENCIAL_5")

# Selecionae 
tb_base_apta_5 = spark.sql("""
    SELECT *,
           CASE 
               WHEN `SUB-AREA DO DIREITO` NOT IN ("CONTENCIOSO COLETIVO", "Preventivo Auto de Infração", "PREVENTIVO AUTO DE INFRAÇÃO") 
                AND `NATUREZA OPERACIONAL` NOT IN ("SINDICATO / MINISTERIO PUBLICO", "ADMINISTRATIVO") THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN `SUB-AREA DO DIREITO` IN ("CONTENCIOSO COLETIVO", "Preventivo Auto de Infração", "PREVENTIVO AUTO DE INFRAÇÃO") 
                OR `NATUREZA OPERACIONAL` IN ("SINDICATO / MINISTERIO PUBLICO", "ADMINISTRATIVO") THEN 'Processo Contencioso Coletivo, Auto de Infração, Ministerio Publico, Adminsitrativo + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO5
    FROM TB_GERENCIAL_5
""")

# COMMAND ----------

tb_base_apta_5.createOrReplaceTempView("TB_GERENCIAL_6")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_6 = spark.sql("""
    SELECT *,
           CASE 
               WHEN `NATUREZA OPERACIONAL` NOT IN ("TERCEIRO SOLVENTE") THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN `NATUREZA OPERACIONAL` IN ("TERCEIRO SOLVENTE") THEN 'Processo de terceiro solvente + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO6
    FROM TB_GERENCIAL_6
""")


# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_6.createOrReplaceTempView("TB_GERENCIAL_7")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_7 = spark.sql("""
    SELECT *,
           CASE 
               WHEN VALOR_DE_RISCO1 > 0 THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN VALOR_DE_RISCO1 <= 0 THEN 'Processo com risco nulo + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO7
    FROM TB_GERENCIAL_7
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_7.createOrReplaceTempView("TB_GERENCIAL_8")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_8 = spark.sql("""
    SELECT *,
           CASE 
               WHEN `ACAO` IN ("", "RECLAMAÇÃO TRABALHISTA") THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN `ACAO` NOT IN ("", "RECLAMAÇÃO TRABALHISTA") THEN 'Processo com ação não negociável + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO8
    FROM TB_GERENCIAL_8
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_8.createOrReplaceTempView("TB_GERENCIAL_9")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_9 = spark.sql("""
    SELECT *,
           CASE 
               WHEN FASE_AJUSTADA = 'CONHECIMENTO' AND VALOR_DE_RISCO1 <= 250000 THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN FASE_AJUSTADA = 'CONHECIMENTO' AND VALOR_DE_RISCO1 > 250000 THEN 'Processo em conhecimento com risco superior a 250.000 + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO9
    FROM TB_GERENCIAL_9
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_9.createOrReplaceTempView("TB_GERENCIAL_10")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_10= spark.sql("""
    SELECT *,
           CASE 
               WHEN FASE_AJUSTADA <> 'CONHECIMENTO' AND CLASSIFICADOR_MRD = 'NÃO' THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN FASE_AJUSTADA = 'CONHECIMENTO' AND CLASSIFICADOR_MRD = 'MRD' THEN 'Processo MRD em Conhecimento + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO10
    FROM TB_GERENCIAL_10
""")


# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_10.createOrReplaceTempView("TB_GERENCIAL_11")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_11 = spark.sql("""
    SELECT *,
           CASE 
               WHEN MARCACAO_PGTO = 0 THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN MARCACAO_PGTO <> 0 THEN 'Processo possui pagamento + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO11
    FROM TB_GERENCIAL_11
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_11.createOrReplaceTempView("TB_GERENCIAL_12")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_12 = spark.sql("""
    SELECT *,
           CASE 
               WHEN COMPARTILHADO_FK = 'NAO' THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN COMPARTILHADO_FK = 'SIM' THEN 'Processo compartilhado FK + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO12
    FROM TB_GERENCIAL_12
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_12.createOrReplaceTempView("TB_GERENCIAL_13")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_13 = spark.sql("""
    SELECT *,
           CASE 
               WHEN ADVOGADO_CONHECIMENTO = 'NÃO' THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN ADVOGADO_CONHECIMENTO NOT IN ('NÃO') THEN 'Processo não apto, advogado contumaz + '
               ELSE '' 
           END AS MOTIVO_NAO_APTO13
    FROM TB_GERENCIAL_13
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_13.createOrReplaceTempView("TB_GERENCIAL_14")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_14 = spark.sql("""
    SELECT *,
           CASE 
               WHEN TERCEIRO_INELEGIVEL = 'Sem Restrição' THEN 'Processo apto para acordo'
               ELSE '' 
           END AS MOTIVO_APTO,
           CASE 
               WHEN TERCEIRO_INELEGIVEL = 'Terceiro totalmente inelegível' THEN 'Processo Terceiro Totalmente Inelegível'
               ELSE '' 
           END AS MOTIVO_NAO_APTO14
    FROM TB_GERENCIAL_14
""")

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_base_apta_14.createOrReplaceTempView("TB_GERENCIAL_15")

# Executar a consulta SQL para aplicar a lógica de transformação
tb_base_apta_15 = spark.sql("""
    SELECT *,
           CASE 
               WHEN TERCEIRO_INELEGIVEL = 'Terceiro elegível apenas somente após a fase recursal (com sentença condenatória)' 
                    AND FASE_AJUSTADA = 'CONHECIMENTO' 
                    AND `NOVO TERCEIRO` IN (
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
                    ) THEN 'Terceiro elegível apenas somente após a fase recursal (com sentença condenatória)'
               ELSE '' 
           END AS MOTIVO_NAO_APTO15
          
    FROM TB_GERENCIAL_15
""")


# COMMAND ----------

from pyspark.sql import functions as F

tb_base_apta_16 = tb_base_apta_15.withColumn(
    "MOTIVOS_NAO_APTO", 
    F.concat_ws(" ", 
                tb_base_apta_15["MOTIVO_NAO_APTO1"], 
                tb_base_apta_15["MOTIVO_NAO_APTO2"], 
                tb_base_apta_15["MOTIVO_NAO_APTO3"],
                tb_base_apta_15["MOTIVO_NAO_APTO4"],
                tb_base_apta_15["MOTIVO_NAO_APTO5"],
                tb_base_apta_15["MOTIVO_NAO_APTO6"],
                tb_base_apta_15["MOTIVO_NAO_APTO7"],
                tb_base_apta_15["MOTIVO_NAO_APTO8"],
                tb_base_apta_15["MOTIVO_NAO_APTO9"],
                tb_base_apta_15["MOTIVO_NAO_APTO10"],
                tb_base_apta_15["MOTIVO_NAO_APTO11"],
                tb_base_apta_15["MOTIVO_NAO_APTO12"],
                tb_base_apta_15["MOTIVO_NAO_APTO13"],
                tb_base_apta_15["MOTIVO_NAO_APTO14"],
                tb_base_apta_15["MOTIVO_NAO_APTO15"])
)

# COMMAND ----------

# Remover espaços extras e definir a coluna MOTIVOS_NAO_APTO como "Processo apto para acordo" quando vazia
tb_base_apta_17 = tb_base_apta_16.withColumn(
    "MOTIVOS_NAO_APTO_FINAL", 
    F.trim(F.when(
        (F.col("MOTIVO_NAO_APTO1") == "") &
        (F.col("MOTIVO_NAO_APTO2") == "") &
        (F.col("MOTIVO_NAO_APTO3") == "") &
        (F.col("MOTIVO_NAO_APTO4") == "") &
        (F.col("MOTIVO_NAO_APTO5") == "") &
        (F.col("MOTIVO_NAO_APTO6") == "") &
        (F.col("MOTIVO_NAO_APTO7") == "") &
        (F.col("MOTIVO_NAO_APTO8") == "") &
        (F.col("MOTIVO_NAO_APTO9") == "") &
        (F.col("MOTIVO_NAO_APTO10") == "") &
        (F.col("MOTIVO_NAO_APTO11") == "") &
        (F.col("MOTIVO_NAO_APTO12") == "") &
        (F.col("MOTIVO_NAO_APTO13") == "") &
        (F.col("MOTIVO_NAO_APTO14") == "") &
        (F.col("MOTIVO_NAO_APTO15") == ""), 
        F.lit("Processo apto para acordo")
    ).otherwise(F.col("MOTIVOS_NAO_APTO")))
)

# COMMAND ----------


from pyspark.sql.functions import col, when

# Defina a coluna ATIVO_EXECUCAO no DataFrame df_20
tb_base_apta_18 = tb_base_apta_17.withColumn("ATIVO_EXECUCAO", 
                         when((col("FASE_AJUSTADA") == "EXECUÇÃO") & (col("PARTE CONTRARIA DATA DISPENSA ").isNull()), "OK")
                         .otherwise("NAO"))


# Defina a coluna PGTO_CONDENACAO no DataFrame df_21
tb_base_apta_19 = tb_base_apta_18.withColumn("PGTO_CONDENACAO", 
                         when((col("FASE_AJUSTADA").isin("EXECUÇÃO", "RECURSAL")) & (col("MARCACAO_PGTO") == 1), "OK")
                         .otherwise("NAO"))



# COMMAND ----------

#Salvar arquivo final na pasra correspondente ao mês da base de precificação gerada
#Disponibilizar arquivo na pasta do mês para importação

import pandas as pd
from shutil import copyfile

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = tb_base_apta_19 .toPandas()

# Save the Pandas DataFrame to an Excel file
local_path = f'/local_disk0/tmp/BASE_APTA_SETEMBRO_24.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='BASE_ELEGIVEL', engine='xlsxwriter')

# Copy the file from the local disk to the desired volume
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/BASE_ELEGIVEL.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

from pyspark.sql import functions as F

count_processo_apto = tb_base_apta_17.filter(F.col("MOTIVOS_NAO_APTO_FINAL") == "Processo apto para acordo").count()
display(count_processo_apto)
