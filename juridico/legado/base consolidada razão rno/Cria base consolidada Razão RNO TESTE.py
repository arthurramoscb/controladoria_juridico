# Databricks notebook source
# MAGIC %md
# MAGIC # Atualiza a base do Razão e RNO do mês e consolida com a base histórica

# COMMAND ----------

# Parametro do formato de data do arquivo. Ex: 20240423
dbutils.widgets.text("nmtabela", "")

# Parametro do formato do nome da tabela gerada ao final. Ex: 202406
dbutils.widgets.text("nmmes", "")

# Parametro do formato do nome da tabela do mês anterior Ex: 202405
dbutils.widgets.text("mesant", "")

# Parametro no formato data. Ex: 2024-06-01
# dbutils.widgets.text("dtmes", "")

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transforma a tabela delta em dataframe

# COMMAND ----------

# Cria o dataframe com a base gerencial do mês de referência
nmtabela = dbutils.widgets.get("nmtabela")
df_consolidado = spark.table(f"databox.juridico_comum.trab_ger_consolida_{nmtabela}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Tira a duplicidade da base gerencial

# COMMAND ----------

# Tira a duplicidade da base pela coluna do ID do processo
df_consolidado = df_consolidado.dropDuplicates(["PROCESSO_ID"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega as bases do Razão e RNO

# COMMAND ----------

# Caminho das pastas e arquivos

nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/'

arquivo_razao = f'Razão 30_10_Fechamento.xlsx'
arquivo_rno = f'COMPOSIÇÃO RNO OUTUBRO 2024.xlsx'

path_razao = diretorio_origem + arquivo_razao
path_rno = diretorio_origem + arquivo_rno

path_razao
path_rno 

# COMMAND ----------

import pandas as pd

# Read Excel file into Pandas DataFrame
pd_df_razao = pd.read_excel(path_razao, sheet_name='Contencioso', skiprows=1)
pd_df_razao_bartira = pd.read_excel(path_razao, sheet_name='Razão Bartira', skiprows=1)
pd_df_rno = pd.read_excel(path_rno, sheet_name='CONTINGÊNCIAS', skiprows=1)

# Convert Pandas DataFrames to Spark DataFrames
df_razao = spark.createDataFrame(pd_df_razao)
df_razao_bartira = spark.createDataFrame(pd_df_razao_bartira)
df_rno = spark.createDataFrame(pd_df_rno)

# COMMAND ----------

# MAGIC %md
# MAGIC #Razão Contencioso

# COMMAND ----------

# MAGIC %md
# MAGIC ####Prepara a base do Razão

# COMMAND ----------

df_razao.createOrReplaceTempView("TB_RAZAO")

df_razao_1 = spark.sql("""
	SELECT *
        ,COALESCE(`ID.1`, 0) AS ID_
    FROM TB_RAZAO
  WHERE `Área` = 'TRABALHISTA'
 """)

df_razao_1 = df_razao_1.drop("ID") \
    .withColumnRenamed("ID_", "ID")

df_razao_1.createOrReplaceTempView("TB_RAZAO_1")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Substitui os valores com ID Zero por números sequenciais

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as F

# Supondo que df seja o DataFrame original
# É necessário ajustar o nome do DataFrame original e criar a coluna `NMMES` como desejado

# Adicionar uma coluna com a contagem crescente
window_spec = Window.partitionBy("ID").orderBy("ID")
df_razao_1 = df_razao_1.withColumn("CONT", F.row_number().over(window_spec))

# Adicionar a coluna `ID_1` com o valor calculado
df_razao_1 = df_razao_1.withColumn("ID_1", F.expr("100 + CONT"))

# Substituir os valores de ID zero por `ID_1`
df_razao_1 = df_razao_1.withColumn("ID_2", F.when(F.col("ID") == 0, F.col("ID_1")).otherwise(F.col("ID")))

# Renomear a coluna `ID_2` para `ID` e remover as colunas desnecessárias
df_razao_1a = df_razao_1.drop("ID", "ID_1", "CONT").withColumnRenamed("ID_2", "ID")

# Ordena o dataframe
df_razao_1a = df_razao_1a.orderBy("ID", "Centro Custo")

# Cria View do dataframe
df_razao_1a.createOrReplaceTempView("TB_RAZAO_1A")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz a marcação para os ID´s com dois ou mais Centro de Custo

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number, asc, desc
from pyspark.sql.window import Window

# Criar uma sessão do Spark
spark = SparkSession.builder.appName("SAS to PySpark Conversion").getOrCreate()

# Supondo que TB_RAZAO_NMMES_1A seja um DataFrame existente
# TB_RAZAO_NMMES_1A = spark.read.csv('path_to_your_csv_file')

# Renomear coluna e ordenar por ID e Centro_Custo
df_razao_1a = df_razao_1a.withColumnRenamed('Centro Custo', 'Centro_Custo').orderBy(['ID', 'Centro_Custo'])

# Remover duplicatas mantendo apenas as primeiras ocorrências de ID e Centro_Custo
df_razao_1b = df_razao_1a.dropDuplicates(['ID', 'Centro_Custo'])

# Janela para particionar por ID e ordenar pelas linhas
window_spec = Window.partitionBy('ID').orderBy('Centro_Custo')

# Adicionar a coluna FLAG_CC que conta o número de Centros de Custo por ID
df_razao_1b = df_razao_1b.withColumn('FLAG_CC', row_number().over(window_spec))

# Filtrar apenas a última ocorrência de cada ID (similar ao IF LAST.ID THEN OUTPUT)
df_razao_1c = df_razao_1b.withColumn('max_flag', count('Centro_Custo').over(window_spec)) \
                                 .filter(col('FLAG_CC') == col('max_flag')) \
                                 .select('ID', 'FLAG_CC')

# Ordena o dataframe
df_razao_1c = df_razao_1c.orderBy(asc("ID"), desc("FLAG_CC"))

# Tira a duplicidade
df_razao_1c = df_razao_1c.dropDuplicates(['ID'])

# COMMAND ----------

display(df_razao_1b)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz o ajuste do Centro de Custo para os processos duplicados

# COMMAND ----------

df_consolidado.createOrReplaceTempView("TB_GER_CONSOLIDADA")
df_razao_1c.createOrReplaceTempView("TB_RAZAO_1C")

df_razao_1d = spark.sql("""
    SELECT A.*
   ,(CASE WHEN C.FLAG_CC = 1 THEN A.`Centro Custo`
			ELSE B.CENTRO_DE_CUSTO_AREA_DEMANDANTE_CODIGO END ) AS `Centro de Custo (M)`
   
FROM TB_RAZAO_1A AS A
LEFT JOIN TB_GER_CONSOLIDADA AS B ON A.ID = B.PROCESSO_ID
LEFT JOIN TB_RAZAO_1C AS C ON A.ID = C.ID
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Agrupa os tipos de pagamentos

# COMMAND ----------

df_razao_1d.createOrReplaceTempView("TB_RAZAO_1D")

df_razao_2 = spark.sql("""
    SELECT 
            ID AS `ID PROCESSO`
            ,`De / Para`
            ,SUM(Valor) AS Valor      
FROM TB_RAZAO_1D
GROUP BY 1, 2
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Separa a base de acordo com cada "de para"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Inicializa a SparkSession
spark = SparkSession.builder.appName("DePara").getOrCreate()

# Carrega a tabela TB_RAZAO_NMMES_2 (substitua com a leitura correta dos dados)
# df = spark.read.format("csv").option("header", "true").load("path_to_TB_RAZAO_NMMES_2.csv")

# Função DePara
def depara(df_razao_2, nm_tabela, tp_depara):
    df_filtered = df_razao_2.filter(upper(col('De / Para')) == tp_depara.upper())
    df_transformed = df_filtered.withColumn(f"{nm_tabela}_RAZAO", col("VALOR").cast("decimal(18,2)"))
    df_transformed = df_transformed.drop("VALOR").drop("De / Para")
    df_transformed.createOrReplaceTempView(f"TB_DP_{nm_tabela}")

# Lista de parâmetros para a função depara
depara_params = [
    ("ACORDO", "ACORDO"),
    ("BX_NAO_COBRAVEIS", "BAIXA NÃO COBRÁVEIS"),
    ("NAO_COBRAVEIS", "NÃO COBRÁVEIS"),
    ("CNOVA", "CNOVA"),
    ("CONDENACAO", "CONDENAÇÃO"),
    ("CREDITOS_TERCEIROS", "CRÉDITOS TERCEIROS"),
    ("FK", "FK"),
    ("GPA", "GPA"),
    ("IMPOSTOS", "IMPOSTOS"),
    ("LIBERACAO_DEP_JUD", "LIBERAÇÃO DEPÓSITOS JUDICIAIS"),
    ("LIBERACAO_DEP_JUD_CI", "LIBERAÇÃO DEPÓSITOS JUDICIAIS (CI)"),
    ("PAGAMENTO_CUSTAS", "PAGAMENTO DE CUSTAS"),
    ("PENHORA", "PENHORAS"),
    ("RECLASSIFICACAO", "RECLASSIFICAÇÕES"),
    ("COMPLEMENTO_FK", "COMPLEMENTO FK"),
    ("RECLASSIFICACOES_FK", "RECLASSIFICAÇÕES FK"),
    ("AJUSTE_CNOVA", "AJUSTE CNOVA"),
    ("AJUSTE_DEP_JUD", "AJUSTE DEPOSITO JUDICIAL"),
    ("AJUSTE_GPA", "AJUSTE GPA")
]

# Aplicando a função depara para cada conjunto de parâmetros
for nm_tabela, tp_depara in depara_params:
    depara(df_razao_2, nm_tabela, tp_depara)

# Agora você pode acessar os DataFrames transformados temporários usando spark.sql("SELECT * FROM TB_DP_<NOME>")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Cria base final com as informações do Razão

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col
from pyspark.sql.functions import expr

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Razao").getOrCreate()

# Carregar as tabelas
tb_dp_acordo = spark.table("TB_DP_ACORDO")
tb_dp_cnova = spark.table("TB_DP_CNOVA")
tb_dp_condenacao = spark.table("TB_DP_CONDENACAO")
tb_dp_creditos_terceiros = spark.table("TB_DP_CREDITOS_TERCEIROS")
tb_dp_fk = spark.table("TB_DP_FK")
tb_dp_gpa = spark.table("TB_DP_GPA")
tb_dp_impostos = spark.table("TB_DP_IMPOSTOS")
tb_dp_liberacao_dep_jud = spark.table("TB_DP_LIBERACAO_DEP_JUD")
tb_dp_liberacao_dep_jud_ci = spark.table("TB_DP_LIBERACAO_DEP_JUD_CI")
tb_dp_pagamento_custas = spark.table("TB_DP_PAGAMENTO_CUSTAS")
tb_dp_penhora = spark.table("TB_DP_PENHORA")
tb_dp_bx_nao_cobraveis = spark.table("TB_DP_BX_NAO_COBRAVEIS")
tb_dp_nao_cobraveis = spark.table("TB_DP_NAO_COBRAVEIS")
tb_dp_reclassificacao = spark.table("TB_DP_RECLASSIFICACAO")
tb_dp_complemento_fk = spark.table("TB_DP_COMPLEMENTO_FK")
tb_dp_reclassificacoes_fk = spark.table("TB_DP_RECLASSIFICACOES_FK")
tb_dp_ajuste_cnova = spark.table("TB_DP_AJUSTE_CNOVA")
tb_dp_ajuste_dep_jud = spark.table("TB_DP_AJUSTE_DEP_JUD")
tb_dp_ajuste_gpa = spark.table("TB_DP_AJUSTE_GPA")

# Realizar o merge das tabelas
df_dp_razao = tb_dp_acordo.alias("A") \
    .join(tb_dp_cnova.alias("B"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_condenacao.alias("C"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_creditos_terceiros.alias("D"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_fk.alias("E"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_gpa.alias("F"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_impostos.alias("G"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_liberacao_dep_jud.alias("H"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_liberacao_dep_jud_ci.alias("I"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_pagamento_custas.alias("J"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_penhora.alias("L"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_bx_nao_cobraveis.alias("M"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_nao_cobraveis.alias("N"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_reclassificacao.alias("O"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_complemento_fk.alias("P"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_reclassificacoes_fk.alias("Q"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_ajuste_cnova.alias("R"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_ajuste_dep_jud.alias("S"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_ajuste_gpa.alias("T"), on='ID PROCESSO', how='outer')

# Substituir valores nulos por 0
df_dp_razao = df_dp_razao.withColumn("AJUSTE_GPA_RAZAO", coalesce(df_dp_razao["AJUSTE_GPA_RAZAO"], lit(0))) \
        .withColumn("AJUSTE_DEP_JUD_RAZAO", coalesce(df_dp_razao["AJUSTE_DEP_JUD_RAZAO"], lit(0))) \
        .withColumn("AJUSTE_CNOVA_RAZAO", coalesce(df_dp_razao["AJUSTE_CNOVA_RAZAO"], lit(0))) \
        .withColumn("ACORDO_RAZAO", coalesce(df_dp_razao["ACORDO_RAZAO"], lit(0))) \
        .withColumn("CONDENACAO_RAZAO", coalesce(df_dp_razao["CONDENACAO_RAZAO"], lit(0))) \
        .withColumn("IMPOSTOS_RAZAO", coalesce(df_dp_razao["IMPOSTOS_RAZAO"], lit(0))) \
        .withColumn("PENHORA_RAZAO", coalesce(df_dp_razao["PENHORA_RAZAO"], lit(0))) \
        .withColumn("LIBERACAO_DEP_JUD_RAZAO", coalesce(df_dp_razao["LIBERACAO_DEP_JUD_RAZAO"], lit(0))) \
        .withColumn("PAGAMENTO_CUSTAS_RAZAO", coalesce(df_dp_razao["PAGAMENTO_CUSTAS_RAZAO"], lit(0))) \
        .withColumn("CNOVA_RAZAO", coalesce(df_dp_razao["CNOVA_RAZAO"], lit(0))) \
        .withColumn("CREDITOS_TERCEIROS_RAZAO", coalesce(df_dp_razao["CREDITOS_TERCEIROS_RAZAO"], lit(0))) \
        .withColumn("FK_RAZAO", coalesce(df_dp_razao["FK_RAZAO"], lit(0))) \
        .withColumn("GPA_RAZAO", coalesce(df_dp_razao["GPA_RAZAO"], lit(0))) \
        .withColumn("LIBERACAO_DEP_JUD_CI_RAZAO", coalesce(df_dp_razao["LIBERACAO_DEP_JUD_CI_RAZAO"], lit(0))) \
        .withColumn("BX_NAO_COBRAVEIS_RAZAO", coalesce(df_dp_razao["BX_NAO_COBRAVEIS_RAZAO"], lit(0))) \
        .withColumn("NAO_COBRAVEIS_RAZAO", coalesce(df_dp_razao["NAO_COBRAVEIS_RAZAO"], lit(0))) \
        .withColumn("RECLASSIFICACAO_RAZAO", coalesce(df_dp_razao["RECLASSIFICACAO_RAZAO"], lit(0))) \
        .withColumn("COMPLEMENTO_FK_RAZAO", coalesce(df_dp_razao["COMPLEMENTO_FK_RAZAO"], lit(0))) \
        .withColumn("RECLASSIFICACOES_FK_RAZAO", coalesce(df_dp_razao["RECLASSIFICACOES_FK_RAZAO"], lit(0))) 


# Calcula o total despesa
df_dp_razao = df_dp_razao.withColumn("TOTAL_DESPESA_RAZAO", expr(
    "AJUSTE_GPA_RAZAO + AJUSTE_DEP_JUD_RAZAO + AJUSTE_CNOVA_RAZAO + ACORDO_RAZAO + "
    "CONDENACAO_RAZAO + IMPOSTOS_RAZAO + PENHORA_RAZAO + LIBERACAO_DEP_JUD_RAZAO + "
    "PAGAMENTO_CUSTAS_RAZAO + BX_NAO_COBRAVEIS_RAZAO + NAO_COBRAVEIS_RAZAO + "
    "COMPLEMENTO_FK_RAZAO + RECLASSIFICACOES_FK_RAZAO + RECLASSIFICACAO_RAZAO"
))

# Calculate the total credito
df_dp_razao = df_dp_razao.withColumn("TOTAL_CREDITO_RAZAO", expr(
    "CNOVA_RAZAO + CREDITOS_TERCEIROS_RAZAO + FK_RAZAO + GPA_RAZAO + "
    "LIBERACAO_DEP_JUD_CI_RAZAO"
))

# Calculate the total consolidado
df_dp_razao = df_dp_razao.withColumn("TOTAL_CONSOLIDADO_RAZAO", expr(
    "AJUSTE_GPA_RAZAO + AJUSTE_DEP_JUD_RAZAO + AJUSTE_CNOVA_RAZAO + ACORDO_RAZAO + "
    "CONDENACAO_RAZAO + IMPOSTOS_RAZAO + PENHORA_RAZAO + LIBERACAO_DEP_JUD_RAZAO + "
    "PAGAMENTO_CUSTAS_RAZAO + CNOVA_RAZAO + CREDITOS_TERCEIROS_RAZAO + FK_RAZAO + "
    "GPA_RAZAO + LIBERACAO_DEP_JUD_CI_RAZAO + BX_NAO_COBRAVEIS_RAZAO + "
    "NAO_COBRAVEIS_RAZAO + RECLASSIFICACAO_RAZAO + COMPLEMENTO_FK_RAZAO + "
    "RECLASSIFICACOES_FK_RAZAO"
))

# Salvar a tabela final
# merged_df.write.saveAsTable("TB_DP_RAZAO_{}".format(NMMES))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Agrupa os pagamentos por número de conta

# COMMAND ----------

# df_consolidado.createOrReplaceTempView("TB_GER_CONSOLIDADA")
# df_razao_3.createOrReplaceTempView("TB_RAZAO_3")

df_razao_conta = spark.sql("""
SELECT ID AS `ID PROCESSO`
        ,Conta
    	,SUM(Valor) AS Valor
FROM TB_RAZAO_1A
WHERE `De / Para` <> '#N/A'
GROUP BY 1, 2
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz a transposição das contas de linhas para colunas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Transposição").getOrCreate()

# Assuming 'df_razao_conta' DataFrame is already loaded with data

# Add an index column to maintain the original order of accounts
window_spec = Window.partitionBy("`ID PROCESSO`").orderBy("CONTA")
df = df_razao_conta.withColumn("row_num", row_number().over(window_spec))

# Transpose rows to columns
df_pivot = df.groupBy("`ID PROCESSO`").pivot("row_num").agg({"CONTA": "first"})

# Rename transposed columns
df_razao_conta_1 = df_pivot.select(
    col("ID PROCESSO"),
    col("1").alias("CONTA_1"),
    col("2").alias("CONTA_2")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz a transposição dos valores das contas de linhas para colunas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Transposição").getOrCreate()

# Assuming 'df_razao_conta' DataFrame is already loaded with data

# Add an index column to maintain the original order of accounts
window_spec = Window.partitionBy("`ID PROCESSO`").orderBy("VALOR")
df = df_razao_conta.withColumn("row_num", row_number().over(window_spec))

# Transpose rows to columns
df_pivot = df.groupBy("`ID PROCESSO`").pivot("row_num").agg({"VALOR": "first"})

# Rename transposed columns
df_razao_conta_2 = df_pivot.select(
    col("ID PROCESSO"),
    col("1").alias("VALOR_1"),
    col("2").alias("VALOR_2")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Junta a tabela de contas com os valores

# COMMAND ----------

from pyspark.sql import SparkSession

# Criação da sessão Spark
spark = SparkSession.builder.appName("RazaoContaMerge").getOrCreate()

# Realizar a junção das tabelas com base no campo 'ID PROCESSO'
merged_df = df_razao_conta_1.join(df_razao_conta_2, on=['ID PROCESSO'], how='outer')

# Selecionar as colunas necessárias
columns_to_select = ['ID PROCESSO', 'CONTA_1', 'VALOR_1', 'CONTA_2', 'VALOR_2']
df_razao_conta_3 = merged_df.select(*columns_to_select)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega os valores das contas na tabela principal do razão

# COMMAND ----------

# Faz o merge entre as duas tabelas
from pyspark.sql import SparkSession

# Inicializar a Spark session
spark = SparkSession.builder.appName("MergeTables").getOrCreate()

# Realizar a junção das tabelas
df_dp_razao_1 = df_dp_razao.join(df_razao_conta_3, on='ID PROCESSO', how='outer')


# COMMAND ----------

# MAGIC %md
# MAGIC #Razão Bartira

# COMMAND ----------

# MAGIC %md
# MAGIC ####Prepara a base do Razão Bartira

# COMMAND ----------

df_razao_bartira.createOrReplaceTempView("TB_RAZAO_BART")

df_razao_bart_1 = spark.sql("""
	SELECT *
        ,COALESCE(ID, 0) AS ID_
    FROM TB_RAZAO_BART
  WHERE upper(`Área`) = 'TRABALHISTA' AND `Tipo de Pagamento`  IN  ('Pagamento','Pagamentos','PAGAMENTO','PAGAMENTOS','RECUP.INDENIZ.TRAB')
 """)

df_razao_bart_1 = df_razao_bart_1.drop("ID") \
    .withColumnRenamed("ID_", "ID")

df_razao_bart_1.createOrReplaceTempView("TB_RAZAO_BART_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Substitui os valores com ID Zero por números sequenciais

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as F

# Supondo que df seja o DataFrame original
# É necessário ajustar o nome do DataFrame original e criar a coluna `NMMES` como desejado

# Adicionar uma coluna com a contagem crescente
window_spec = Window.partitionBy("ID").orderBy("ID")
df_razao_bart_1 = df_razao_bart_1.withColumn("CONT", F.row_number().over(window_spec))

# Adicionar a coluna `ID_1` com o valor calculado
df_razao_bart_1 = df_razao_bart_1.withColumn("ID_1", F.expr("2000 + CONT"))

# Substituir os valores de ID zero por `ID_1`
df_razao_bart_1 = df_razao_bart_1.withColumn("ID_2", F.when(F.col("ID") == 0, F.col("ID_1")).otherwise(F.col("ID")))

# Renomear a coluna `ID_2` para `ID` e remover as colunas desnecessárias
df_razao_bart_1a = df_razao_bart_1.drop("ID", "ID_1", "CONT").withColumnRenamed("ID_2", "ID")

df_razao_bart_1a.createOrReplaceTempView("TB_RAZAO_BART_1A")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz a marcação para os ID´s com dois ou mais Centro de Custo

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

# Criar uma sessão do Spark
spark = SparkSession.builder.appName("SAS to PySpark Conversion").getOrCreate()

# Renomear coluna e ordenar por ID e Centro_Custo
df_razao_bart_1a = df_razao_bart_1a.withColumnRenamed('Centro de custo', 'Centro_Custo').orderBy(['ID', 'Centro_Custo'])

# Remover duplicatas mantendo apenas as primeiras ocorrências de ID e Centro_Custo
df_razao_bart_1b = df_razao_bart_1a.dropDuplicates(['ID', 'Centro_Custo'])

# Janela para particionar por ID e ordenar pelas linhas
window_spec = Window.partitionBy('ID').orderBy('Centro_Custo')

# Adicionar a coluna FLAG_CC que conta o número de Centros de Custo por ID
df_razao_bart_1b = df_razao_bart_1b.withColumn('FLAG_CC', row_number().over(window_spec))

# Filtrar apenas a última ocorrência de cada ID (similar ao IF LAST.ID THEN OUTPUT)
df_razao_bart_1c = df_razao_bart_1b.withColumn('max_flag', count('Centro_Custo').over(window_spec)) \
                                 .filter(col('FLAG_CC') == col('max_flag')) \
                                 .select('ID', 'FLAG_CC')

# Ordena o dataframe
df_razao_bart_1c = df_razao_bart_1c.orderBy(asc("ID"), desc("FLAG_CC"))

# Tira a duplicidade
df_razao_bart_1c = df_razao_bart_1c.dropDuplicates(['ID'])
                                 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz o ajuste do Centro de Custo para os processos duplicados

# COMMAND ----------

df_consolidado.createOrReplaceTempView("TB_GER_CONSOLIDADA")
df_razao_bart_1c.createOrReplaceTempView("TB_RAZAO_BART_1C")

df_razao_bart_1d = spark.sql("""
    SELECT A.*
   ,(CASE WHEN C.FLAG_CC = 1 THEN A.`Centro de custo`
			ELSE B.CENTRO_DE_CUSTO_AREA_DEMANDANTE_CODIGO END ) AS `Centro de Custo (M)`
   
FROM TB_RAZAO_BART_1A AS A
LEFT JOIN TB_GER_CONSOLIDADA AS B ON A.ID = B.PROCESSO_ID
LEFT JOIN TB_RAZAO_BART_1C AS C ON A.ID = C.ID
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Agrupa os tipos de pagamentos

# COMMAND ----------

df_razao_bart_1d.createOrReplaceTempView("TB_RAZAO_BART_1D")

df_razao_bart_2 = spark.sql("""
    SELECT ID AS `ID PROCESSO`
            ,`De / Para`
            ,SUM(`Débito - Crédito`) AS Valor      
FROM TB_RAZAO_BART_1D
GROUP BY 1, 2
""")

# COMMAND ----------

display(df_razao_bart_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Separa a base de acordo com cada "de para"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Inicializa a SparkSession
spark = SparkSession.builder.appName("DePara").getOrCreate()

# Função DePara
def depara(df_razao_bart_2, nm_tabela, tp_depara):
    df_filtered = df_razao_bart_2.filter(upper(col('De / Para')) == tp_depara.upper())
    df_transformed = df_filtered.withColumn(f"{nm_tabela}_RAZAO", col("VALOR").cast("decimal(18,2)"))
    df_transformed = df_transformed.drop("VALOR").drop("De / Para")
    df_transformed.createOrReplaceTempView(f"TB_DP_BART_{nm_tabela}")

# Lista de parâmetros para a função depara
depara_params = [
    ("ACORDO", "ACORDO"),
    ("CONDENACAO", "CONDENAÇÃO"),
    ("CREDITOS", "CRÉDITO"),
    ("IMPOSTOS", "IMPOSTOS"),
    ("FK", "FK"),
    ("RECUP_INDENIZ_TRAB", "RECUP.INDENIZ.TRAB"),
    ("COMPLEMENTO_FK", "COMPLEMENTO FK")
]

# Aplicando a função depara para cada conjunto de parâmetros
for nm_tabela, tp_depara in depara_params:
    depara(df_razao_bart_2, nm_tabela, tp_depara)

# Agora você pode acessar os DataFrames transformados temporários usando spark.sql("SELECT * FROM TB_DP_<NOME>")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Cria base final com as informações do Razão

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, lit, coalesce
from pyspark.sql.functions import expr
from pyspark.sql import functions as F

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Razao").getOrCreate()

# Carregar as tabelas
tb_dp_acordo = spark.table("TB_DP_BART_ACORDO")
tb_dp_condenacao = spark.table("TB_DP_BART_CONDENACAO")
tb_dp_creditos = spark.table("TB_DP_BART_CREDITOS")
tb_dp_impostos = spark.table("TB_DP_BART_IMPOSTOS")
tb_dp_fk = spark.table("TB_DP_BART_FK")
tb_dp_recup_indeniz_trab = spark.table("TB_DP_BART_RECUP_INDENIZ_TRAB")
tb_dp_complemento_fk = spark.table("TB_DP_BART_COMPLEMENTO_FK")

# Realizar o merge das tabelas
df_dp_bart_razao = tb_dp_acordo.alias("A") \
    .join(tb_dp_condenacao.alias("B"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_creditos.alias("C"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_impostos.alias("D"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_fk.alias("E"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_recup_indeniz_trab.alias("F"), on='ID PROCESSO', how='outer') \
    .join(tb_dp_complemento_fk.alias("G"), on='ID PROCESSO', how='outer')

# Calcula o total despesa

# Substituir valores nulos por 0
df_dp_bart_razao = df_dp_bart_razao.withColumn("ACORDO_RAZAO", coalesce(df_dp_bart_razao["ACORDO_RAZAO"], lit(0))) \
        .withColumn("CONDENACAO_RAZAO", coalesce(df_dp_bart_razao["CONDENACAO_RAZAO"], lit(0))) \
        .withColumn("IMPOSTOS_RAZAO", coalesce(df_dp_bart_razao["IMPOSTOS_RAZAO"], lit(0))) \
        .withColumn("RECUP_INDENIZ_TRAB_RAZAO", coalesce(df_dp_bart_razao["RECUP_INDENIZ_TRAB_RAZAO"], lit(0))) \
        .withColumn("COMPLEMENTO_FK_RAZAO", coalesce(df_dp_bart_razao["COMPLEMENTO_FK_RAZAO"], lit(0))) \
        .withColumn("FK_RAZAO", coalesce(df_dp_bart_razao["FK_RAZAO"], lit(0))) \
        .withColumn("CREDITOS_RAZAO", coalesce(df_dp_bart_razao["CREDITOS_RAZAO"], lit(0)))

# Calcula o total despesa
df_dp_bart_razao = df_dp_bart_razao.withColumn("TOTAL_DESPESA_RAZAO", expr(
    "ACORDO_RAZAO + CONDENACAO_RAZAO + IMPOSTOS_RAZAO + RECUP_INDENIZ_TRAB_RAZAO + COMPLEMENTO_FK_RAZAO"
))

# Calcula o total credito
df_dp_bart_razao = df_dp_bart_razao.withColumn("TOTAL_CREDITO_RAZAO", expr(
    "CREDITOS_RAZAO"
))

# Calcula o total consolidado
df_dp_bart_razao = df_dp_bart_razao.withColumn("TOTAL_CONSOLIDADO_RAZAO", expr(
    "ACORDO_RAZAO + CONDENACAO_RAZAO + IMPOSTOS_RAZAO + CREDITOS_RAZAO + "
    "FK_RAZAO + RECUP_INDENIZ_TRAB_RAZAO + COMPLEMENTO_FK_RAZAO"
))

# Salvar a tabela final
# merged_df.write.saveAsTable("TB_DP_RAZAO_{}".format(NMMES))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Agrupa os pagamentos por número de conta

# COMMAND ----------

# df_consolidado.createOrReplaceTempView("TB_GER_CONSOLIDADA")
# df_razao_bart_3.createOrReplaceTempView("TB_RAZAO_BART_3")

df_razao_bart_conta = spark.sql("""
SELECT ID AS `ID PROCESSO`
        ,Conta
    	,SUM(`Débito - Crédito`) AS Valor
FROM TB_RAZAO_BART_1A
WHERE `De / Para` <> '#N/A'
GROUP BY 1, 2
""")

# COMMAND ----------

display(df_razao_bart_conta)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz a transposição das contas de linhas para colunas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Transposição").getOrCreate()

# Assuming 'df_razao_conta' DataFrame is already loaded with data

# Add an index column to maintain the original order of accounts
window_spec = Window.partitionBy("`ID PROCESSO`").orderBy("Conta")
df = df_razao_bart_conta.withColumn("row_num", row_number().over(window_spec))

df_pivot = df.groupBy("`ID PROCESSO`").pivot("row_num", [1, 2]).agg({"Conta": "first"})



# COMMAND ----------

# Rename transposed columns
df_razao_bart_conta_1 = df_pivot.select(
    col("ID PROCESSO"),
    col("1").alias("CONTA_1"),
    col("2").alias("CONTA_2")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz a transposição dos valores das contas de linhas para colunas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Transposição").getOrCreate()

# Assuming 'df_razao_conta' DataFrame is already loaded with data

# Add an index column to maintain the original order of accounts
window_spec = Window.partitionBy("`ID PROCESSO`").orderBy("VALOR")
df = df_razao_bart_conta.withColumn("row_num", row_number().over(window_spec))

# Transpose rows to columns
df_pivot = df.groupBy("`ID PROCESSO`").pivot("row_num", [1, 2]).agg({"Valor": "first"})

# Rename transposed columns
df_razao_bart_conta_2 = df_pivot.select(
    col("ID PROCESSO"),
    col("1").alias("VALOR_1"),
    col("2").alias("VALOR_2")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Junta a tabela de contas com os valores

# COMMAND ----------

from pyspark.sql import SparkSession

# Criação da sessão Spark
spark = SparkSession.builder.appName("RazaoContaMerge").getOrCreate()

# Realizar a junção das tabelas com base no campo 'ID PROCESSO'
merged_df = df_razao_bart_conta_1.join(df_razao_bart_conta_2, on=['ID PROCESSO'], how='outer')

# Selecionar as colunas necessárias
columns_to_select = ['ID PROCESSO', 'CONTA_1', 'VALOR_1', 'CONTA_2', 'VALOR_2']
df_razao_bart_conta_3 = merged_df.select(*columns_to_select)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega os valores das contas na tabela principal do razão

# COMMAND ----------

# Faz o merge entre as duas tabelas
from pyspark.sql import SparkSession

# Inicializar a Spark session
spark = SparkSession.builder.appName("MergeTables").getOrCreate()

# Realizar a junção das tabelas
df_dp_bart_razao_1 = df_dp_bart_razao.join(df_razao_bart_conta_3, on='ID PROCESSO', how='outer')


# COMMAND ----------

# MAGIC %md
# MAGIC ####Junta as bases do Razão Contencioso e Bartira

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Create a Spark session
spark = SparkSession.builder.appName("JoinDataFrames").getOrCreate()

# Get columns from both dataframes
cols_df1 = set(df_dp_razao_1.columns)
cols_df2 = set(df_dp_bart_razao_1.columns)

# Find columns that are missing in each dataframe
missing_in_df1 = cols_df2 - cols_df1
missing_in_df2 = cols_df1 - cols_df2

# Add missing columns with null values to each dataframe
for col in missing_in_df1:
    df_dp_razao_1 = df_dp_razao_1.withColumn(col, lit(None))

for col in missing_in_df2:
    df_dp_bart_razao_1 = df_dp_bart_razao_1.withColumn(col, lit(None))

# Ensure the columns order is the same
df_dp_razao_1 = df_dp_razao_1.select(sorted(df_dp_razao_1.columns))
df_dp_bart_razao_1 = df_dp_bart_razao_1.select(sorted(df_dp_bart_razao_1.columns))

# Union the dataframes
df_dp_razao_11 = df_dp_razao_1.union(df_dp_bart_razao_1)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega as informações da base gerencial

# COMMAND ----------

df_dp_razao_11.createOrReplaceTempView("TB_DP_RAZAO_11")
df_consolidado.createOrReplaceTempView("TB_GER_CONSOLIDADA")
df_razao_bart_1d.createOrReplaceTempView("TB_RAZAO_BART_1D")

df_dp_razao_2 = spark.sql("""
    SELECT A.*
		,B.AREA_DO_DIREITO AS `Área do Direito`
		,B.`PARTE_CONTRARIA_CARGO_CARGO_GRUPO` AS `Objeto Assunto/Cargo (M)`
		,B.`PARTE_CONTRARIA_CARGO_CARGO_SUB_GRUPO` AS `Sub Objeto Assunto/Cargo (M)`
		-- ,C.`Centro de Custo (M)` AS `Centro de Custo (M)`
		,CASE WHEN C.`Centro de Custo (M)` IS NULL THEN D.`Centro de Custo (M)`
			ELSE C.`Centro de Custo (M)` END AS `Centro de Custo (M)`
		,B.DATA_DE_REABERTURA AS `Reabertura`
		,B.DATA_REGISTRADO AS `DATACADASTRO`
		,B.DEMITIDOS_POR_ESTRUTURACAO AS `Demitido por Reestruturação`
		,B.DISTRIBUICAO AS `Distribuição`
		,B.EMPRESA AS `Empresa (M)`
		,B.ESCRITORIO AS `Escritorio`
		,B.FASE AS `FASE (M)`
		,B.GRUPO AS `Grupo (M)`
		,B.NATUREZA_OPERACIONAL AS `Natureza Operacional (M)`
		,B.NUMERO_DO_PROCESSO AS `Nº Processo`
		,B.PASTA
		,B.RESPONSABILIDADE_EMPRESA_PERCENTUAL AS `% Empresa (M)`
		,B.RESPONSABILIDADE_SOCIO_PERCENTUAL AS `% Sócio (M)`
		,B.STATUS AS `STATUS (M)`
		,B.SUB_AREA_DO_DIREITO AS `SUB-ÁREA DO DIREITO`
		,B.VALOR_DA_CAUSA AS `Vlr Causa`
		,B.DEFINICAO_DE_ESTRATEGIA_DECISAO_ESTRATEGIA_ACORDO_DEFESA AS ESTRATEGIA
		,(CASE WHEN B.PROCESSO_ID IS NULL THEN 0 ELSE 1 END) AS FLAG
FROM TB_DP_RAZAO_11 AS A
LEFT JOIN TB_GER_CONSOLIDADA AS B ON A.`ID PROCESSO` = B.PROCESSO_ID
LEFT JOIN TB_RAZAO_1D AS C ON  `ID PROCESSO` = C.ID
LEFT JOIN TB_RAZAO_BART_1D AS D ON  `ID PROCESSO` = D.ID
 
""")

# Remover duplicados
df_dp_razao_2 = df_dp_razao_2.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #RNO

# COMMAND ----------

# MAGIC %md
# MAGIC ####Substitui os valores com ID Zero por números sequenciais

# COMMAND ----------

df_rno.createOrReplaceTempView("TB_RNO")

from pyspark.sql import Window
import pyspark.sql.functions as F

# Supondo que df seja o DataFrame original
# É necessário ajustar o nome do DataFrame original e criar a coluna `NMMES` como desejado

# Adicionar uma coluna com a contagem crescente
window_spec = Window.partitionBy("ID").orderBy("ID")
df_rno = df_rno.withColumn("CONT", F.row_number().over(window_spec))

# Adicionar a coluna `ID_1` com o valor calculado
df_rno = df_rno.withColumn("ID_1", F.expr("4000 + CONT"))

# Substituir os valores de ID zero por `ID_1`
df_rno = df_rno.withColumn("ID_2", F.when(F.col("ID") == 0, F.col("ID_1")).otherwise(F.col("ID")))

# Renomear a coluna `ID_2` para `ID` e remover as colunas desnecessárias
df_rno_1a = df_rno.drop("ID", "ID_1", "CONT").withColumnRenamed("ID_2", "ID")

df_rno_1a.createOrReplaceTempView("TB_RNO_1A")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz a marcação para os ID´s com dois ou mais Centro de Custo

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

# Criar uma sessão do Spark
spark = SparkSession.builder.appName("SAS to PySpark Conversion").getOrCreate()

# Renomear coluna e ordenar por ID e Centro_Custo
df_rno_1a = df_rno_1a.withColumnRenamed('Centro Custo', 'Centro_Custo').orderBy(['ID', 'Centro_Custo'])

# Remover duplicatas mantendo apenas as primeiras ocorrências de ID e Centro_Custo
df_rno_1b = df_rno_1a.dropDuplicates(['ID', 'Centro_Custo'])

# Janela para particionar por ID e ordenar pelas linhas
window_spec = Window.partitionBy('ID').orderBy('Centro_Custo')

# Adicionar a coluna FLAG_CC que conta o número de Centros de Custo por ID
df_rno_1b = df_rno_1b.withColumn('FLAG_CC', row_number().over(window_spec))

# Filtrar apenas a última ocorrência de cada ID (similar ao IF LAST.ID THEN OUTPUT)
df_rno_1c = df_rno_1b.withColumn('max_flag', count('Centro_Custo').over(window_spec)) \
                                 .filter(col('FLAG_CC') == col('max_flag')) \
                                 .select('ID', 'FLAG_CC')

# Ordena o dataframe
df_rno_1c = df_rno_1c.orderBy(asc("ID"), desc("FLAG_CC"))

# Tira a duplicidade
df_rno_1c = df_rno_1c.dropDuplicates(['ID'])
                                 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz o ajuste do Centro de Custo para os processos duplicados

# COMMAND ----------

df_consolidado.createOrReplaceTempView("TB_GER_CONSOLIDADA")
df_rno_1c.createOrReplaceTempView("TB_RNO_1C")

df_rno_1d = spark.sql("""
    SELECT A.*
   ,(CASE WHEN C.FLAG_CC = 1 THEN A.`Centro Custo`
			ELSE B.CENTRO_DE_CUSTO_AREA_DEMANDANTE_CODIGO END ) AS `Centro de Custo (M)`
   
FROM TB_RNO_1A AS A
LEFT JOIN TB_GER_CONSOLIDADA AS B ON A.ID = B.PROCESSO_ID
LEFT JOIN TB_RNO_1C AS C ON A.ID = C.ID
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cria a base final com as informações do RNO

# COMMAND ----------

df_rno_1d.createOrReplaceTempView("TB_RNO_1D")

df_dp_rno = spark.sql("""
    SELECT ID AS `ID PROCESSO`
            ,RNO
            ,SUM(`Montante em moeda interna`) AS VALOR_RNO      
FROM TB_RNO_1D
GROUP BY 1, 2
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega as informações da base gerencial

# COMMAND ----------

df_dp_rno.createOrReplaceTempView("TB_DP_RNO")
df_consolidado.createOrReplaceTempView("TB_GER_CONSOLIDADA")

df_dp_rno_1 = spark.sql("""
    SELECT A.*
		,B.AREA_DO_DIREITO AS `Área do Direito`
		,B.`PARTE_CONTRARIA_CARGO_CARGO_GRUPO` AS `Objeto Assunto/Cargo (M)`
		,B.`PARTE_CONTRARIA_CARGO_CARGO_SUB_GRUPO` AS `Sub Objeto Assunto/Cargo (M)`
		,C.`Centro de Custo (M)` AS `Centro de Custo (M)`
		,B.DATA_DE_REABERTURA AS `Reabertura`
		,B.DATA_REGISTRADO AS `DATACADASTRO`
		,B.DEMITIDOS_POR_ESTRUTURACAO AS `Demitido por Reestruturação`
		,B.DISTRIBUICAO AS `Distribuição`
		,B.EMPRESA AS `Empresa (M)`
		,B.ESCRITORIO AS `Escritorio`
		,B.FASE AS `FASE (M)`
		,B.GRUPO AS `Grupo (M)`
		,B.NATUREZA_OPERACIONAL AS `Natureza Operacional (M)`
		,B.NUMERO_DO_PROCESSO AS `Nº Processo`
		,B.PASTA
		,B.RESPONSABILIDADE_EMPRESA_PERCENTUAL AS `% Empresa (M)`
		,B.RESPONSABILIDADE_SOCIO_PERCENTUAL AS `% Sócio (M)`
		,B.STATUS AS `STATUS (M)`
		,B.SUB_AREA_DO_DIREITO AS `SUB-ÁREA DO DIREITO`
		,B.VALOR_DA_CAUSA AS `Vlr Causa`
		,B.DEFINICAO_DE_ESTRATEGIA_DECISAO_ESTRATEGIA_ACORDO_DEFESA AS ESTRATEGIA
		,(CASE WHEN B.PROCESSO_ID IS NULL THEN 0 ELSE 1 END) AS FLAG
FROM TB_DP_RNO AS A
LEFT JOIN TB_GER_CONSOLIDADA AS B ON A.`ID PROCESSO` = B.PROCESSO_ID
LEFT JOIN TB_RNO_1D AS C ON  `ID PROCESSO` = C.ID
   
""")

# Remover duplicados
df_dp_rno_1 = df_dp_rno_1.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #Prepara a base de fechamento do mês

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cria a coluna "Processo com Documento"

# COMMAND ----------

# df_rno_4.createOrReplaceTempView("TB_RNO_4")
# df_consolidado.createOrReplaceTempView("TB_GER_CONSOLIDADA")
# df_rno_3.createOrReplaceTempView("TB_RNO_3")

nmmes = dbutils.widgets.get("nmmes")

tb_fecham_trab = spark.sql(f"""
    SELECT *
        ,(CASE WHEN DOC IN ('ACORDO 1 - PÓS ANÁLISE DE SUBSÍDIOS - DOCUMENTAÇÃO COMPLETA, MAS É DESFAVORÁVEL'
                    ,'ACORDO 1 - PÓS ANÁLISE DE SUBSÍDIOS - DOCUMENTAÇÃO ENTREGUE FORA DO PRAZO DA TAREFA DE SUBSÍDIOS'
                    ,'ACORDO 1 - PÓS ANÁLISE DE SUBSÍDIOS - DOCUMENTAÇÃO NÃO ATENDE (DOCUMENTOS INSUFICIENTES PARA DEFESA)'
                    ,'ACORDO 2 - PÓS AUDIÊNCIA - AUDIÊNCIA ADIADA/CONCILIAÇÃO FRUSTRADA - DOCUMENTAÇÃO NÃO ATENDE OU É DESFAVORÁVEL'
                    ,'ACORDO 2 - PÓS AUDIÊNCIA - AUDIÊNCIA FAVORÁVEL - MAS DOCUMENTAÇÃO NÃO FOI SUFICIENTE NA APRESENTAÇÃO DA DEFESA'
                    ,'ACORDO 2 - PÓS AUDIÊNCIA - CONCILIAÇÃO REALIZADA - (DOCUMENTAÇÃO NÃO FOI SUFICIENTE NA APRESENTAÇÃO DA DEFESA)'
                    ,'ACORDO 3 - PÓS ACÓRDÃO - NULIDADE DA SENTENÇA/RETORNO AO CONHECIMENTO - DOCUMENTAÇÃO NÃO ATENDE'
                    ,'2 AUDIÊNCIA| ACORDO - PÓS AUDIÊNCIA - AUDIÊNCIA ADIADA/CONCILIAÇÃO FRUSTRADA - DOCUMENTAÇÃO NÃO ATENDE OU É DESFAVORÁVEL'
                    ,'2 AUDIÊNCIA | ACORDO - PÓS AUDIÊNCIA - AUDIÊNCIA ADIADA/CONCILIAÇÃO FRUSTRADA - DOCUMENTAÇÃO NÃO ATENDE OU É DESFAVORÁVEL'
                    ,'1 SUBSÍDIOS | ACORDO - PÓS ANÁLISE DE SUBSÍDIOS - DOCUMENTAÇÃO COMPLETA, MAS É DESFAVORÁVEL'
                    ,'2 AUDIÊNCIA | ACORDO - PÓS AUDIÊNCIA - CONCILIAÇÃO REALIZADA - (DOCUMENTAÇÃO NÃO FOI SUFICIENTE NA APRESENTAÇÃO DA DEFESA)'
                    ,'3 DECISÃO | ACORDO - PÓS ACÓRDÃO - NULIDADE DA SENTENÇA/RETORNO AO CONHECIMENTO - DOCUMENTAÇÃO NÃO ATENDE'
                    ,'1 SUBSÍDIOS | ACORDO - PÓS ANÁLISE DE SUBSÍDIOS - DOCUMENTAÇÃO NÃO ATENDE (DOCUMENTOS INSUFICIENTES PARA DEFESA)'
                    ,'2 AUDIÊNCIA| ACORDO - PÓS AUDIÊNCIA - AUDIÊNCIA FAVORÁVEL - MAS DOCUMENTAÇÃO NÃO FOI SUFICIENTE NA APRESENTAÇÃO DA DEFESA'
                    ,'2 AUDIÊNCIA | ACORDO - PÓS AUDIÊNCIA - AUDIÊNCIA FAVORÁVEL - MAS DOCUMENTAÇÃO NÃO FOI SUFICIENTE NA APRESENTAÇÃO DA DEFESA'
                    ,'2 AUDIÊNCIA| ACORDO - PÓS AUDIÊNCIA - CONCILIAÇÃO REALIZADA - (DOCUMENTAÇÃO NÃO FOI SUFICIENTE NA APRESENTAÇÃO DA DEFESA)'
                    ,'1 SUBSÍDIOS | ACORDO - PÓS ANÁLISE DE SUBSÍDIOS - DOCUMENTAÇÃO ENTREGUE FORA DO PRAZO DA TAREFA DE SUBSÍDIOS')
            THEN 'SIM' ELSE 'NÃO' END) AS PROCESSO_COM_DOCUMENTO_1
    FROM databox.juridico_comum.tb_fecham_trab_{nmmes}
""")

# Renomeia colunas listadas abaixo
df_fecham_trab = tb_fecham_trab.drop("PROCESSO_COM_DOCUMENTO","Centro_de_Custo_M_1","Centro_de_Custo_M").withColumnRenamed("PROCESSO_COM_DOCUMENTO_1", "PROCESSO_COM_DOCUMENTO")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Prepara a base do fechamento

# COMMAND ----------

# Caminho das pastas e arquivos

mesant = dbutils.widgets.get("mesant")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/trabalhista_base_financeiro/'

# Importa a tabela do mês anterior
arquivo = f'20.09.2024 Fechamento Trabalhista.xlsx'

# Cria o caminho completo do arquivo
path_tb_fech = diretorio_origem + arquivo

# Carrega as planilhas em Spark Data Frames
df_base_fechamento = read_excel(path_tb_fech, f"'Base Fechamento'!A2")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega o centro de custo da base do fechamento

# COMMAND ----------

# Tira a duplicidade do dataframe
df_base_fechamento = df_base_fechamento.dropDuplicates(["ID PROCESSO"])

# COMMAND ----------

df_fecham_trab.createOrReplaceTempView("TB_FECH_TRAB")
df_base_fechamento.createOrReplaceTempView("TB_FECH_FINANC")

tb_fecham_trab_1a = spark.sql(f"""
    SELECT A.*
        ,B.`Centro de Custo (M-1)` AS Centro_de_Custo_M_1
        ,B.`Centro de Custo (M)` AS Centro_de_Custo_M
    FROM TB_FECH_TRAB AS A
    LEFT JOIN TB_FECH_FINANC AS B ON A.ID_PROCESSO=B.`ID PROCESSO`
""")

# Tira a duplicidade do dataframe
# tb_fecham_trab_1a = df_base_fechamento.dropDuplicates(["ID PROCESSO"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega as informações do Crédito - Razão

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Inicializar Spark session
spark = SparkSession.builder.appName("SAS_to_PySpark").getOrCreate()

# Realizar a junção (merge) das tabelas pela coluna 'ID PROCESSO'
merged_df = tb_fecham_trab_1a.alias('a').join(df_dp_razao_2.alias('b'), col('a.ID_PROCESSO') == col('b.ID PROCESSO'), 'outer')

# Selecionar as colunas da tabela df_fecham_trab e as colunas adicionais da tabela df_dp_razao_2
# Supondo que queremos todas as colunas da primeira tabela e somente as colunas adicionais (diferentes) da segunda tabela

# Obter os nomes das colunas de ambas as tabelas
tb_fecham_trab_1a_columns = set([column.upper() for column in tb_fecham_trab_1a.columns])
df_dp_razao_2_columns = set([column.upper() for column in df_dp_razao_2.columns])

# Encontrar as colunas adicionais (diferentes) na tabela TB_DP_RAZAO_&NMMES._2
additional_columns = df_dp_razao_2_columns - tb_fecham_trab_1a_columns

# Selecionar as colunas necessárias
selected_columns = tb_fecham_trab_1a.columns + list(additional_columns)

# Adicionar a coluna 'ID'
df_fecham_trab_1 = merged_df.select(
    # [col('a.ID_PROCESSO').alias('ID')] + 
    [col(f'a.{column_name}') for column_name in tb_fecham_trab_1a.columns] + 
    [col(f'b.{column_name}') for column_name in additional_columns]
)

# Cria a coluna ID e apaga a coluna ID PROCESSO
df_fecham_trab_1 = df_fecham_trab_1.withColumn("ID", when(col("ID_PROCESSO").isNull(), col("ID PROCESSO")).otherwise(col("ID_PROCESSO")))
df_fecham_trab_1 = df_fecham_trab_1.drop("ID PROCESSO")


# Substitui o campo centro_de_custo_m se for nulo
df_fecham_trab_1 = df_fecham_trab_1.withColumn("Centro_de_Custo_M2", when(col("Centro_de_Custo_M").isNull(), col("CENTRO DE CUSTO (M)")).otherwise(col("Centro_de_Custo_M")))
df_fecham_trab_1 = df_fecham_trab_1.drop("Centro_de_Custo_M")
df_fecham_trab_1 = df_fecham_trab_1.withColumnRenamed("Centro_de_Custo_M2", "Centro_de_Custo_M")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega as informações do Crédito - RNO

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializar Spark session
spark = SparkSession.builder.appName("SAS_to_PySpark").getOrCreate()

# Realizar a junção (merge) das tabelas pela coluna 'ID PROCESSO'
merged_df = df_fecham_trab_1.alias('a').join(df_dp_rno_1.alias('b'), col('a.ID_PROCESSO') == col('b.ID PROCESSO'), 'outer')

# Selecionar as colunas da tabela df_fecham_trab e as colunas adicionais da tabela df_dp_razao_2
# Supondo que queremos todas as colunas da primeira tabela e somente as colunas adicionais (diferentes) da segunda tabela

# Obter os nomes das colunas de ambas as tabelas
df_fecham_trab_1_columns = set(df_fecham_trab_1.columns)
df_dp_rno_1_columns = set(df_dp_rno_1.columns)


df_fecham_trab_1_columns = set([column.upper() for column in df_fecham_trab_1.columns])
df_dp_rno_1_columns = set([column.upper() for column in df_dp_rno_1.columns])


# Encontrar as colunas adicionais (diferentes) na tabela TB_DP_RAZAO_&NMMES._2
additional_columns = df_dp_rno_1_columns - df_fecham_trab_1_columns

# Selecionar as colunas necessárias
selected_columns = df_fecham_trab_1.columns + list(additional_columns)

# Adicionar a coluna 'ID'
df_fecham_trab_2 = merged_df.select(
    # [col('a.ID_PROCESSO').alias('ID')] + 
    [col(f'a.{column_name}') for column_name in df_fecham_trab_1.columns] + 
    [col(f'b.{column_name}') for column_name in additional_columns]
)

df_fecham_trab_2 = df_fecham_trab_2.withColumn("ID2", when(col("ID_PROCESSO").isNull(), col("ID PROCESSO")).otherwise(col("ID_PROCESSO")))
df_fecham_trab_2 = df_fecham_trab_2.withColumn("ID3", when(col("ID2").isNull(), col("ID")).otherwise(col("ID2")))
df_fecham_trab_2 = df_fecham_trab_2.drop("ID", "ID2")
df_fecham_trab_2 = df_fecham_trab_2.withColumnRenamed("ID3", "ID")

# Substitui o campo centro_de_custo_m se for nulo
df_fecham_trab_2 = df_fecham_trab_2.withColumn("Centro_de_Custo_M2", when(col("Centro_de_Custo_M").isNull(), col("CENTRO DE CUSTO (M)")).otherwise(col("Centro_de_Custo_M")))
df_fecham_trab_2 = df_fecham_trab_2.drop("Centro_de_Custo_M")
df_fecham_trab_2 = df_fecham_trab_2.withColumnRenamed("Centro_de_Custo_M2", "Centro_de_Custo_M")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Corrige o campo Fase

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col
from pyspark.sql.types import DateType
from datetime import datetime

# Inicializa a Spark Session
spark = SparkSession.builder.appName("ConvertSAS").getOrCreate()

# Define a coluna FASE_FINAL baseada na condição
df_fecham_trab_2 = df_fecham_trab_2.withColumn("FASE_FINAL", when(col("FASE_M").isNull(), col("FASE (M)")).otherwise(col("FASE_M")))

# Remove as colunas desnecessárias
cols_to_drop = ["FASE_M", "FASE (M)", "STATUS (M)"]
df_fecham_trab_2 = df_fecham_trab_2.drop(*cols_to_drop)

# Renomeia as colunas conforme necessário
df_fecham_trab_3 = df_fecham_trab_2.withColumnRenamed("STATUS_M", "STATUS (M)") \
       .withColumnRenamed("FASE_FINAL", "FASE (M)")

# Cria a coluna Processo Esteira
df_fecham_trab_3 = df_fecham_trab_3.withColumn("PROCESSO - ESTEIRA", col("CARTEIRA"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Converte as colunas com formato número para Decimal

# COMMAND ----------

from pyspark.sql.types import DecimalType

# Função para converter colunas para o formato Decimal
def convert_to_decimal(df, columns):
    for column in columns:
        df = df.withColumn(column, col(column).cast("double"))
        # df = df.withColumn(column, col(column).cast("decimal(18,2)"))
    return df

# Lista de colunas a serem convertidas para Decimal
colunas_numeros = ['AJUSTE_GPA_RAZAO', 'AJUSTE_DEP_JUD_RAZAO', 'AJUSTE_CNOVA_RAZAO', 'ACORDO_RAZAO', 'CONDENACAO_RAZAO',
'IMPOSTOS_RAZAO', 'PENHORA_RAZAO', 'LIBERACAO_DEP_JUD_RAZAO', 'PAGAMENTO_CUSTAS_RAZAO', 'CNOVA_RAZAO', 
'CREDITOS_TERCEIROS_RAZAO', 'FK_RAZAO', 'GPA_RAZAO', 'LIBERACAO_DEP_JUD_CI_RAZAO', 'BX_NAO_COBRAVEIS_RAZAO', 'NAO_COBRAVEIS_RAZAO', 'RECLASSIFICACAO_RAZAO', 'COMPLEMENTO_FK_RAZAO', 'RECLASSIFICACOES_FK_RAZAO', 'CREDITOS_RAZAO', 'TOTAL_CONSOLIDADO_RAZAO','RECUP_INDENIZ_TRAB_RAZAO','TOTAL_CREDITO_RAZAO','TOTAL_DESPESA_RAZAO']

# Converter as colunas especificadas para Decimal
df_fecham_trab_3 = convert_to_decimal(df_fecham_trab_3, colunas_numeros)

# Remove as colunas 'ID' 
df_fecham_trab_3 = df_fecham_trab_3.drop("ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Corrige os valores do Razão e RNO para os casos em duplicidade

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Inicializa a Spark session
spark = SparkSession.builder.appName("Fechamento_Trab").getOrCreate()

# Define a janela para partição e ordenação por 'ID'
windowSpec = Window.partitionBy("ID_PROCESSO").orderBy("ID_PROCESSO")

# Adiciona uma coluna de contagem que reinicia para cada partição 'ID'
df = df_fecham_trab_3.withColumn("CONT", F.row_number().over(windowSpec))

# Aplica as condições conforme o código SAS
df = df.withColumn(
    "ACORDO_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("ACORDO_RAZAO"))
).withColumn(
    "CNOVA_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("CNOVA_RAZAO"))
).withColumn(
    "CONDENACAO_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("CONDENACAO_RAZAO"))
).withColumn(
    "CREDITOS_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("CREDITOS_RAZAO"))
).withColumn(
    "CREDITOS_TERCEIROS_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("CREDITOS_TERCEIROS_RAZAO"))
).withColumn(
    "FK_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("FK_RAZAO"))
).withColumn(
    "COMPLEMENTO_FK_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("COMPLEMENTO_FK_RAZAO"))
).withColumn(
    "GPA_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("GPA_RAZAO"))
).withColumn(
    "IMPOSTOS_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("IMPOSTOS_RAZAO"))
).withColumn(
    "LIBERACAO_DEP_JUD_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("LIBERACAO_DEP_JUD_RAZAO"))
).withColumn(
    "LIBERACAO_DEP_JUD_CI_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("LIBERACAO_DEP_JUD_CI_RAZAO"))
).withColumn(
    "PAGAMENTO_CUSTAS_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("PAGAMENTO_CUSTAS_RAZAO"))
).withColumn(
    "PENHORA_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("PENHORA_RAZAO"))
).withColumn(
    "BX_NAO_COBRAVEIS_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("BX_NAO_COBRAVEIS_RAZAO"))
).withColumn(
    "NAO_COBRAVEIS_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("NAO_COBRAVEIS_RAZAO"))
).withColumn(
    "RECLASSIFICACAO_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("RECLASSIFICACAO_RAZAO"))
).withColumn(
    "RECLASSIFICACOES_FK_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("RECLASSIFICACOES_FK_RAZAO"))
).withColumn(
    "AJUSTE_CNOVA_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("AJUSTE_CNOVA_RAZAO"))
).withColumn(
    "AJUSTE_DEP_JUD_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("AJUSTE_DEP_JUD_RAZAO"))
).withColumn(
    "AJUSTE_GPA_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("AJUSTE_GPA_RAZAO"))
).withColumn(
    "TOTAL_DESPESA_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("TOTAL_DESPESA_RAZAO"))
).withColumn(
    "TOTAL_CREDITO_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("TOTAL_CREDITO_RAZAO"))
).withColumn(
    "TOTAL_CONSOLIDADO_RAZAO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("TOTAL_CONSOLIDADO_RAZAO"))
).withColumn(
#     "TOTAL_FK_RAZAO", 
#     F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("TOTAL_FK_RAZAO"))
# ).withColumn(
    "RNO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("RNO"))
).withColumn(
    "VALOR_RNO", 
    F.when((F.col("CONT") >= 2) & (F.col("LINHA") == 'linha01'), None).otherwise(F.col("VALOR_RNO"))
)

# Aplica a condição para a coluna 'ID PROCESSO'
df = df.withColumn(
    "ID PROCESSO", 
    F.when((F.col("ID_PROCESSO") >= 100) & (F.col("ID_PROCESSO") <= 5000), 0).otherwise(F.col("ID PROCESSO"))
)

# Remove as colunas 'ID' e 'CONT'
df_fecham_trab_4 = df.drop("CONT")


# COMMAND ----------

from pyspark.sql.functions import col

# Transforma a variável 'Centro_de_Custo_M' de string para long
df_fecham_trab_4 = df_fecham_trab_4.withColumn("Centro_de_Custo_M", col("Centro_de_Custo_M").cast("long"))

# COMMAND ----------

display(df_fecham_trab_4)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega as bases auxiliares

# COMMAND ----------

# Caminho das pastas e arquivos

nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/'

inss_decaido = f'Consolidado INSS Decadente 22 e 23.xlsx'
arquivo_dp_bu = f'De para Centro de Custo e BU_v2 - ago-2022.xlsx'
arquivo_dp_funcional = f'Centro de Custo e Área Funcional_OKENN_280623.xlsx'
arquivo_dp_func_bloq = f'Cecos Trabalhistas Bloqueados - De para.xlsx'

path_inss_decaido = diretorio_origem + inss_decaido
path_dp_bu = diretorio_origem + arquivo_dp_bu
path_dp_funcional = diretorio_origem + arquivo_dp_funcional
path_dp_func_bloq = diretorio_origem + arquivo_dp_func_bloq

path_inss_decaido


# COMMAND ----------

import pandas as pd

# Read Excel file into Pandas DataFrame
pd_df_inss_decaido = pd.read_excel(path_inss_decaido, sheet_name='BASE_CONSOLIDADA', skiprows=0)
pd_df_dp_bu = pd.read_excel(path_dp_bu, sheet_name='Base BU Centro de Custo', skiprows=0)
pd_df_dp_funcional = pd.read_excel(path_dp_funcional, sheet_name='OKENN', skiprows=0)
pd_df_dp_func_bloq = pd.read_excel(path_dp_func_bloq, sheet_name='Cecos', skiprows=0)

# Convert Pandas DataFrames to Spark DataFrames
df_inss_decaido = spark.createDataFrame(pd_df_inss_decaido)
df_dp_bu = spark.createDataFrame(pd_df_dp_bu)
df_dp_funcional = spark.createDataFrame(pd_df_dp_funcional)
df_dp_func_bloq = spark.createDataFrame(pd_df_dp_func_bloq)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Prepara a tabela do fechamento financeiro

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, lit
from pyspark.sql.types import DateType

# Cria a sessão do Spark
spark = SparkSession.builder.appName("FechamTrabRazaoRNO").getOrCreate()

# Filtra os dados
df_fecham_trab_4 = df_fecham_trab_4.withColumn("DATACADASTRO", col("DATACADASTRO").cast(DateType()))

# Adiciona a coluna NOVO_X_LEGADO com base na condição
df_fecham_trab_4 = df_fecham_trab_4.withColumn(
    "NOVO_X_LEGADO",
    when(col("DATACADASTRO") >= lit("2020-01-01"), "NOVO  ").otherwise("LEGADO")
)

# Remove espaços em branco da coluna ESTRATEGIA e aplica as transformações
df_fecham_trab_4 = df_fecham_trab_4.withColumn("ESTRATEGIA_", 
                   when(expr("ESTRATEGIA IN ('ACO', 'ACOR', 'ACORD', 'ACORDO')"), 'ACORDO')
                   .when(expr("ESTRATEGIA IN ('DEF', 'DEFE', 'DEFES', 'DEFESA')"), 'DEFESA')
                   .otherwise(col("ESTRATEGIA"))
)

# Remove colunas indesejadas e renomeia a coluna ESTRATEGIA_
fecham_trab_razao_rno = df_fecham_trab_4.drop("ESTRATEGIA", "FLAG").withColumnRenamed("ESTRATEGIA_", "ESTRATEGIA")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega os "de paras" na base

# COMMAND ----------

# Importar funções do PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper

# Criar sessão do Spark
spark = SparkSession.builder.appName("ConvertSASCode").getOrCreate()

# Realizar o join e aplicar as transformações
fecham_trab_razao_rno_1 = fecham_trab_razao_rno \
    .join(df_dp_bu, fecham_trab_razao_rno['Centro_de_Custo_M'] == df_dp_bu['Centro de custo'], 'left') \
    .join(df_dp_funcional, fecham_trab_razao_rno['Centro_de_Custo_M'] == df_dp_funcional['centro'], 'left') \
    .select(
        fecham_trab_razao_rno["*"],
        when(fecham_trab_razao_rno['Empresa (M)'] == 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'BARTIRA').otherwise(df_dp_bu['BU**']).alias('BU'),
        when(fecham_trab_razao_rno['Empresa (M)'] == 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'BARTIRA').otherwise(df_dp_funcional['nome2']).alias('VP'),
        when(fecham_trab_razao_rno['Empresa (M)'] == 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'BARTIRA').otherwise(df_dp_funcional['nome3']).alias('DIRETORIA'),
        when(fecham_trab_razao_rno['Empresa (M)'] == 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'BARTIRA').otherwise(upper(df_dp_funcional['nome responsavel'])).alias('RESPONSAVEL_AREA'),
        when(fecham_trab_razao_rno['Empresa (M)'] == 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'BART').otherwise(df_dp_funcional['area funcional']).alias('AREA_FUNCIONAL')
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega o "de para" de centro de custos bloqueados

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Cria uma sessão Spark
spark = SparkSession.builder.appName("ConversionExample").getOrCreate()

# Realizar o join
joined_df = fecham_trab_razao_rno_1.join(
    df_dp_func_bloq,
    fecham_trab_razao_rno_1['Centro_de_Custo_M'] == df_dp_func_bloq['CECO BLOQUEADO'],
    'left'
)

# Aplicar a lógica do CASE WHEN
fecham_trab_razao_rno_2 = joined_df.withColumn(
    "AREA_FUNCIONAL_1",
    when(
        col("AREA_FUNCIONAL").isNull(),
        col("ÁREA FUNCIONAL (DE PARA)")
    ).otherwise(col("AREA_FUNCIONAL"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Faz o "de para" das áreas funcionais

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Inicializa uma SparkSession
spark = SparkSession.builder.appName("SAS_to_PySpark").getOrCreate()

# Faz o de para das áreas funcionais
fecham_trab_razao_rno_3 = fecham_trab_razao_rno_2.withColumn("DP_VP_DIRETORIA", 
                   when((col("VP") == "ADMINISTRATIVO E LOGISTICA") & 
                        col("DIRETORIA").isin('ASAP LOG', 'DIR EXEC LOGISTICA E SUPPLY', 'LOGISTICA ENVVIAS RETIRA'), "LOGISTICA")
                   .when((col("VP") == "ADMINISTRATIVO E LOGISTICA") & 
                         ~col("DIRETORIA").isin('ASAP LOG', 'DIR EXEC LOGISTICA E SUPPLY', 'LOGISTICA ENVVIAS RETIRA'), "ADMINISTRATIVO")
                   .when((col("VP") == "VIAHUB E LOGISTICA") & 
                         col("DIRETORIA").isin('CNT', 'DIR EXEC LOGISTICA E SUPPLY', 'LOGISTICA ENVVIAS RETIRA', 'ASAP LOG'), "LOGISTICA")
                   .when((col("VP") == "VIAHUB E LOGISTICA") & 
                         ~col("DIRETORIA").isin('CNT', 'DIR EXEC LOGISTICA E SUPPLY', 'LOGISTICA ENVVIAS RETIRA', 'ASAP LOG'), "VIAHUB")
                   .when(col("VP") == "ADMINISTRATIVO", "ADMINISTRATIVO")
                   .when(col("VP") == "COMERCIAL E VENDAS", "COMERCIAL E VENDAS")
                   .when(col("VP") == "CFO", "CFO")
                   .when(col("VP") == "LOGISTICA", "LOGISTICA")
                   .when(col("VP") == "VIAHUB", "VIAHUB")
                   .when(col("VP") == "", "OUTROS")
                   .otherwise(col("VP")))

# Remover colunas desnecessárias e renomear a coluna
fecham_trab_razao_rno_3 = fecham_trab_razao_rno_3.drop("AREA_FUNCIONAL", "`Valor INSS Decaído`")
fecham_trab_razao_rno_3 = fecham_trab_razao_rno_3.withColumnRenamed("AREA_FUNCIONAL_1", "AREA_FUNCIONAL")



# COMMAND ----------

# MAGIC %md
# MAGIC ####Agrupa a base do INSS Decaído

# COMMAND ----------

df_inss_decaido.createOrReplaceTempView("TB_INSS_DECAIDO")

df_inss_decaido_f = spark.sql("""
 	SELECT `ID PROCESSO`
        ,`MÊS` AS MES 
		,SUM(`VALOR INSS DECAÍDO`) AS `VALOR INSS DECAÍDO`
	FROM TB_INSS_DECAIDO
	GROUP BY 1, 2 
 """)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega o INSS Decaído na base

# COMMAND ----------

fecham_trab_razao_rno_3.createOrReplaceTempView("TB_FECH_RAZAO_RNO_3")
df_inss_decaido_f.createOrReplaceTempView("TB_INSS_DECAIDO_F")

fecham_trab_razao_rno_4 = spark.sql("""
 	SELECT A.* 
		,B.`VALOR INSS DECAÍDO`
	FROM TB_FECH_RAZAO_RNO_3 AS A
    LEFT JOIN TB_INSS_DECAIDO_F AS B ON A.`ID PROCESSO` = B.`ID PROCESSO` AND A.MES_FECH = B.MES	
 """)

# COMMAND ----------

fecham_trab_razao_rno_4.count() #31608

# COMMAND ----------

# MAGIC %md
# MAGIC ####Exclui colunas não utilizadas

# COMMAND ----------

fecham_trab_razao_rno_5 =fecham_trab_razao_rno_4.drop("CARTEIRA", "DT_ULT_PGTO", "ACORDOS", "CONDENAÇÃO", "PENHORA", "GARANTIA", "IMPOSTO", "OUTROS_PAGAMENTOS", "TOTAL_PAGAMENTOS", "CECO BLOQUEADO", "CECO (DE PARA)", "STATUS (DE PARA)", "DESCRIÇÃO (DE PARA)", "ÁREA FUNCIONAL (DE PARA)", "ID PROCESSO", "ÁREA DO DIREITO", "SUB-ÁREA DO DIREITO", "GRUPO (M)",
    "EMPRESA (M)", "CENTRO DE CUSTO (M)", "OBJETO ASSUNTO/CARGO (M)", "SUB OBJETO ASSUNTO/CARGO (M)", "NATUREZA OPERACIONAL (M)", "DISTRIBUIÇÃO", "Nº PROCESSO", "VLR CAUSA", "% SÓCIO (M)", "% EMPRESA (M)", "DEMITIDO POR REESTRUTURAÇÃO", "VALOR INSS DECAÍDO")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Criar colunas faltantes no dataframe

# COMMAND ----------

fecham_trab_razao_rno_6 = (fecham_trab_razao_rno_5
    .withColumn("Média de Pagamento", lit(0))
    .withColumn("% Risco", lit(0))
    .withColumn("Correção  (M-1)", lit(0))
    .withColumn("CUSTAS_RAZAO", lit(0))
    .withColumn("RECLASSIFICACAO_FK_RAZAO", lit(0))
    .withColumn("TOTAL_FK_RAZAO", lit(0))
    .withColumn("CONTA_3", lit(0))
    .withColumn("VALOR_CONTA_3", lit(0))
    .withColumn("Cluster Aging Tempo de Empresa", lit(0))
    .withColumn("Média de Pagamento NOVO", lit(0))
    .withColumn("FLAG_BART", lit(0)))

# COMMAND ----------

fecham_trab_razao_rno_6 = (fecham_trab_razao_rno_6.withColumnRenamed("MOTIVO_MOVIMENTAÇÃO", "MOTIVO_MOVIMENTAÇÃO2")
                           .withColumnRenamed("Observações", "Observações2")


)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renomeia as colunas

# COMMAND ----------

fecham_trab_razao_rno_7 = (fecham_trab_razao_rno_6
    .withColumnRenamed("ID_PROCESSO", "ID PROCESSO")
    .withColumnRenamed("AREA_DO_DIREITO", "Área do Direito")
    .withColumnRenamed("SUB_AREA_DO_DIREITO", "Sub-área do Direito")
    .withColumnRenamed("GRUPO_M_1", "Grupo (M-1)")
    .withColumnRenamed("EMPRESA_M_1", "Empresa (M-1)")
    .withColumnRenamed("GRUPO_M", "Grupo (M)")
    .withColumnRenamed("EMPRESA_M", "Empresa (M)")
    .withColumnRenamed("STATUS_M_1", "STATUS (M-1)")
    .withColumnRenamed("Centro_de_Custo_M_1", "Centro de Custo (M-1)")
    .withColumnRenamed("Centro_de_Custo_M", "Centro de Custo (M)")
    .withColumnRenamed("OBJETO_ASSUNTO_CARGO_M_1", "Objeto Assunto/Cargo (M-1)")
    .withColumnRenamed("SUB_OBJETO_ASSUNTO_CARGO_M_1", "Sub Objeto Assunto/Cargo (M-1)")
    .withColumnRenamed("OBJETO_ASSUNTO_CARGO_M", "Objeto Assunto/Cargo (M)")
    .withColumnRenamed("SUB_OBJETO_ASSUNTO_CARGO_M", "Sub Objeto Assunto/Cargo (M)")
    .withColumnRenamed("ORGAO_OFENSOR_FLUXO_M_1", "Órgão ofensor (Fluxo) (M-1)")
    .withColumnRenamed("ORGAO_OFENSOR_FLUXO_M", "Órgão ofensor (Fluxo) (M)")
    .withColumnRenamed("NATUREZA_OPERACIONAL_M_1", "Natureza Operacional (M-1)")
    .withColumnRenamed("NATUREZA_OPERACIONAL_M", "Natureza Operacional (M)")
    .withColumnRenamed("DISTRIBUICAO", "Distribuição")
    .withColumnRenamed("NO_PROCESSO", "Nº Processo")
    .withColumnRenamed("VLR_CAUSA", "Vlr Causa")
    .withColumnRenamed("%_SOCIO_M_1", "% Sócio (M-1)")
    .withColumnRenamed("%_EMPRESA_M_1", "% Empresa (M-1)")
    .withColumnRenamed("%_SOCIO_M", "% Sócio (M)")
    .withColumnRenamed("%_EMPRESA_M", "% Empresa (M)")
    .withColumnRenamed("PROVISAO_M_1", "Provisão (M-1)")
    .withColumnRenamed("CORRECAO_M_1", "Correção (M-1)")
    .withColumnRenamed("PROVISAO_TOTAL_M_1", "Provisão Total (M-1)")
    .withColumnRenamed("CLASSIFICACAO_MOV_M", "Classificação Mov. (M)")
    .withColumnRenamed("PROVISAO_MOV_M", "Provisão Mov. (M)")
    .withColumnRenamed("CORRECAO_MOV_M", "Correção Mov. (M)")
    .withColumnRenamed("PROVISAO_MOV_TOTAL_M", "Provisão Mov. Total (M)")
    .withColumnRenamed("PROVISAO_TOTAL_M", "Provisão Total (M)")
    .withColumnRenamed("CORRECAO_M", "Correção (M)")
    .withColumnRenamed("PROVISAO_TOTAL_PASSIVO_M", "Provisão Total Passivo (M)")
    .withColumnRenamed("SOCIO:_PROVISAO_M_1", "Socio: Provisão (M-1)")
    .withColumnRenamed("SOCIO:_CORRECAO_M_1", "Socio: Correção (M-1)")
    .withColumnRenamed("SOCIO:_PROVISAO_TOTAL_M_1", "Socio: Provisão Total (M-1)")
    .withColumnRenamed("SOCIO:_CLASSIFICACAO_MOV_M", "SOCIO: Classificação Mov. (M)")
    .withColumnRenamed("SOCIO:_PROVISAO_MOV_M", "SOCIO: Provisão Mov. (M)")
    .withColumnRenamed("SOCIO:_CORRECAO_MOV_M", "SOCIO: Correção Mov. (M)")
    .withColumnRenamed("SOCIO:_PROVISAO_MOV_TOTAL_M", "SOCIO: Provisão Mov. Total (M)")
    .withColumnRenamed("SOCIO:_PROVISAO_TOTAL_M", "SOCIO: Provisão Total (M)")
    .withColumnRenamed("SOCIO:_CORRECAO_M_1_0001", "SOCIO: Correção (M-1)_0001")
    .withColumnRenamed("SOCIO:_CORRECAO_M", "SOCIO: Correção (M)")
    .withColumnRenamed("SOCIO:_PROV_TOTAL_PASSIVO_M", "SOCIO: Prov. Total Passivo (M)")
    .withColumnRenamed("EMPRESA:_PROVISAO_M_1", "Empresa: Provisão (M-1)")
    .withColumnRenamed("EMPRESA:_CORRECAO_M_1", "Empresa: Correção (M-1)")
    .withColumnRenamed("EMPRESA:_PROVISAO_TOTAL_M_1", "Empresa: Provisão Total (M-1)")
    .withColumnRenamed("EMPRESA:_CLASSIFICACAO_MOV_M", "EMPRESA: Classificação Mov.(M)")
    .withColumnRenamed("EMPRESA:_PROVISAO_MOV_M", "EMPRESA: Provisão Mov. (M)")
    .withColumnRenamed("EMPRESA:_CORRECAO_MOV_M", "EMPRESA: Correção Mov. (M)")
    .withColumnRenamed("EMPRESA:_PROVISAO_MOV_TOTAL_M", "EMPRESA: Provisão Mov.Total(M)")
    .withColumnRenamed("EMPRESA:_PROVISAO_TOTAL_M", "EMPRESA: Provisão Total (M)")
    .withColumnRenamed("EMPRESA:_CORRECAO_M_1_0001", "EMPRESA: Correção (M-1)_0001")
    .withColumnRenamed("EMPRESA:_CORRECAO_M", "EMPRESA: Correção (M)")
    .withColumnRenamed("EMPRESA:_PROV_TOTAL_PASSIVO_M", "EMPRESA: Prov Total Passivo(M)")
    .withColumnRenamed("DEMITIDO_POR_REESTRUTURACAO", "Demitido por Reestruturação")
    .withColumnRenamed("INDICACAO_PROCESSO_ESTRATEGICO", "Indicação Processo Estratégico")
    .withColumnRenamed("SUB_TIPO", "SUB TIPO")
    .withColumnRenamed("PARCELAMENTO_CONDENACAO", "PARCELAMENTO CONDENAÇÃO")
    .withColumnRenamed("PARCELAMENTO_ACORDO", "PARCELAMENTO ACORDO")
    .withColumnRenamed("DOC", "DOC.")
    .withColumnRenamed("VALOR_INSS_DECAIDO", "Valor INSS Decaído")
    .withColumnRenamed("ESTRATEGIA_M_1", "ESTRATÉGIA (M-1)")
    .withColumnRenamed("ELEGIVEL_M_1", "ELEGÍVEL (M-1)")
    .withColumnRenamed("FASE_M_1", "FASE (M-1)")
    .withColumnRenamed("TIPO_DE_CALCULO_M_1", "TIPO DE CALCULO (M-1)")
    .withColumnRenamed("ELEGIVEL_M", "ELEGIVEL (M)")
    .withColumnRenamed("TIPO_DE_CALCULO_M", "TIPO DE CALCULO (M)")
    .withColumnRenamed("MOTIVO_MOVIMENTACAO", "MOTIVO_MOVIMENTAÇÃO")
    # .withColumnRenamed("PAGAMENTO_CUSTAS_RAZAO", "CUSTAS_RAZAO")
    .withColumnRenamed("VALOR_1", "VALOR_CONTA_1")
    .withColumnRenamed("VALOR_2", "VALOR_CONTA_2")
    .withColumnRenamed("OBSERVACOES", "Observações")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Prepara a base histórica do RNO - Razão

# COMMAND ----------

# Caminho das pastas e arquivos

mesant = dbutils.widgets.get("mesant")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/'


# Importa a tabela do mês anterior
arquivo = f'df_fech_trab_razao_rno_{mesant}_f.xlsx'

# Cria o caminho completo do arquivo
path_razao_rno = diretorio_origem + arquivo

path_razao_rno

# COMMAND ----------

# import pandas as pd

mesant = dbutils.widgets.get("mesant")

# Carrega as planilhas em Spark Data Frames
df_hist_razao_rno = read_excel(path_razao_rno, f"'Planilha1'!A1")

# COMMAND ----------

nmmes = dbutils.widgets.get("nmmes")

df_hist_razao_rno.createOrReplaceTempView(f'TB_HIST_FECHAM_TRAB_RAZAO_RNO_{nmmes}')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Ordena a tabela do mês

# COMMAND ----------

fecham_trab_razao_rno_7 = fecham_trab_razao_rno_7.select(
    "`LINHA`",	"`ID PROCESSO`",	"`PASTA`",	"`Área do Direito`",	"`Sub-área do Direito`",	"`Grupo (M-1)`",	"`Empresa (M-1)`",	"`Grupo (M)`",	"`Empresa (M)`",	"`STATUS (M-1)`",	"`STATUS (M)`",	"`Centro de Custo (M-1)`",	"`Centro de Custo (M)`",	"`DATACADASTRO`",	"`REABERTURA`",	"`Objeto Assunto/Cargo (M-1)`",	"`Sub Objeto Assunto/Cargo (M-1)`",	"`Objeto Assunto/Cargo (M)`",	"`Sub Objeto Assunto/Cargo (M)`",	"`Órgão ofensor (Fluxo) (M-1)`",	"`Órgão ofensor (Fluxo) (M)`",	"`Natureza Operacional (M-1)`",	"`Natureza Operacional (M)`",	"`Média de Pagamento`",	"`% Risco`",	"`Distribuição`",	"`Nº Processo`",	"`ESCRITORIO`",	"`Vlr Causa`",	"`% Sócio (M-1)`",	"`% Empresa (M-1)`",	"`% Sócio (M)`",	"`% Empresa (M)`",	"`Provisão (M-1)`",	"`Correção (M-1)`",	"`Provisão Total (M-1)`",	"`Classificação Mov. (M)`",	"`Provisão Mov. (M)`",	"`Correção Mov. (M)`",	"`Provisão Mov. Total (M)`",	"`Provisão Total (M)`",	"`Correção  (M-1)`",	"`Correção (M)`",	"`Provisão Total Passivo (M)`",	"`Socio: Provisão (M-1)`",	"`Socio: Correção (M-1)`",	"`Socio: Provisão Total (M-1)`",	"`SOCIO: Classificação Mov. (M)`",	"`SOCIO: Provisão Mov. (M)`",	"`SOCIO: Correção Mov. (M)`",	"`SOCIO: Provisão Mov. Total (M)`",	"`SOCIO: Provisão Total (M)`",	"`SOCIO: Correção (M-1)_0001`",	"`SOCIO: Correção (M)`",	"`SOCIO: Prov. Total Passivo (M)`",	"`Empresa: Provisão (M-1)`",	"`Empresa: Correção (M-1)`",	"`Empresa: Provisão Total (M-1)`",	"`EMPRESA: Classificação Mov.(M)`",	"`EMPRESA: Provisão Mov. (M)`",	"`EMPRESA: Correção Mov. (M)`",	"`EMPRESA: Provisão Mov.Total(M)`",	"`EMPRESA: Provisão Total (M)`",	"`EMPRESA: Correção (M-1)_0001`",	"`EMPRESA: Correção (M)`",	"`EMPRESA: Prov Total Passivo(M)`",	"`Demitido por Reestruturação`",	"`Indicação Processo Estratégico`",	"`NOVOS`",	"`REATIVADOS`",	"`ENCERRADOS`",	"`ESTOQUE`",	"`SUB TIPO`",	"`TIPO_PGTO`",	"`PARCELAMENTO CONDENAÇÃO`",	"`PARCELAMENTO ACORDO`",	"`SUM_OF_VALOR`",	"`OUTRAS_ADICOES`",	"`OUTRAS_REVERSOES`",	"`DOC.`",	"`Valor INSS Decaído`",	"`ESTRATÉGIA (M-1)`",	"`ELEGÍVEL (M-1)`",	"`FASE (M-1)`",	"`TIPO DE CALCULO (M-1)`",	"`ELEGIVEL (M)`",	"`FASE (M)`",	"`TIPO DE CALCULO (M)`",	"`MOTIVO_MOVIMENTAÇÃO`",	"`MOTIVO_PAGAMENTO`",	"`DP_FASE`",	"`PROCESSO - ESTEIRA`",	"`ESTRATEGIA`",	"`BU`",	"`VP`",	"`DIRETORIA`",	"`RESPONSAVEL_AREA`",	"`AREA_FUNCIONAL`",	"`DP_VP_DIRETORIA`",	"`PROCESSO_COM_DOCUMENTO`",	"`ACORDO_RAZAO`",	"`CNOVA_RAZAO`",	"`CONDENACAO_RAZAO`",	"`CREDITOS_TERCEIROS_RAZAO`",	"`FK_RAZAO`",	"`GPA_RAZAO`",	"`IMPOSTOS_RAZAO`",	"`LIBERACAO_DEP_JUD_RAZAO`",	"`LIBERACAO_DEP_JUD_CI_RAZAO`",	"`PAGAMENTO_CUSTAS_RAZAO`",	"`PENHORA_RAZAO`",	"`BX_NAO_COBRAVEIS_RAZAO`",	"`NAO_COBRAVEIS_RAZAO`",	"`CUSTAS_RAZAO`",	"`RECLASSIFICACAO_RAZAO`",	"`COMPLEMENTO_FK_RAZAO`",	"`RECLASSIFICACOES_FK_RAZAO`",	"`RECLASSIFICACAO_FK_RAZAO`",	"`TOTAL_FK_RAZAO`",	"`CREDITOS_RAZAO`",	"`RECUP_INDENIZ_TRAB_RAZAO`",	"`TOTAL_DESPESA_RAZAO`",	"`TOTAL_CREDITO_RAZAO`",	"`TOTAL_CONSOLIDADO_RAZAO`",	"`RNO`",	"`VALOR_RNO`",	"`CONTA_1`",	"`VALOR_CONTA_1`",	"`CONTA_2`",	"`VALOR_CONTA_2`",	"`CONTA_3`",	"`VALOR_CONTA_3`",	"`NOVO_X_LEGADO`",	"`MES_FECH`",	"`Observações`",	"`Cluster Aging Tempo de Empresa`",	"`Média de Pagamento NOVO`",	"`AJUSTE_CNOVA_RAZAO`",	"`AJUSTE_DEP_JUD_RAZAO`",	"`AJUSTE_GPA_RAZAO`",	"`FLAG_BART`",

)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Arruma MES_FECH

# COMMAND ----------

from pyspark.sql.functions import *

nmtabela = dbutils.widgets.get("nmtabela")
nmano = nmtabela[:4]
nmmes = nmtabela[4:-2]
mes_fech = f'{nmano}-{nmmes}-01'

fecham_trab_razao_rno_7 = fecham_trab_razao_rno_7.fillna({'MES_FECH': mes_fech})

# COMMAND ----------

from pyspark.sql.functions import col

fecham_trab_razao_rno_7 = fecham_trab_razao_rno_7.withColumn("Centro de Custo (M)", col("Centro de Custo (M)").cast("int"))
fecham_trab_razao_rno_7 = fecham_trab_razao_rno_7.withColumn("Centro de Custo (M-1)", col("Centro de Custo (M-1)").cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC # TABELA FIT ABAIXO

# COMMAND ----------

# Cria excel FIT
fecham_trab_razao_rno_fit = fecham_trab_razao_rno_7.select(
"`ID PROCESSO`",
"`EMPRESA: Provisão Mov. (M)`",
"`EMPRESA: Correção (M)`",
"`Indicação Processo Estratégico`",
"`NOVOS`",	
"`REATIVADOS`",	
"`ENCERRADOS`",
"`TIPO_PGTO`",
"`SUM_OF_VALOR`",
"`OUTRAS_ADICOES`",
"`OUTRAS_REVERSOES`",
"`Valor INSS Decaído`",
"`MOTIVO_MOVIMENTAÇÃO`",	
"`MOTIVO_PAGAMENTO`",
"`BU`",
"`AREA_FUNCIONAL`",
"`ACORDO_RAZAO`",	
"`CNOVA_RAZAO`",
"`CONDENACAO_RAZAO`",
"`CREDITOS_TERCEIROS_RAZAO`",
"`FK_RAZAO`",
"`GPA_RAZAO`",
"`IMPOSTOS_RAZAO`",	
"`LIBERACAO_DEP_JUD_RAZAO`",
"`LIBERACAO_DEP_JUD_CI_RAZAO`",
"`PAGAMENTO_CUSTAS_RAZAO`",
"`PENHORA_RAZAO`",
"`BX_NAO_COBRAVEIS_RAZAO`",
"`NAO_COBRAVEIS_RAZAO`",
"`CUSTAS_RAZAO`",
"`RECLASSIFICACAO_RAZAO`",
"`COMPLEMENTO_FK_RAZAO`",
"`RECLASSIFICACOES_FK_RAZAO`",
"`RECLASSIFICACAO_FK_RAZAO`",
"`TOTAL_FK_RAZAO`",
"`CREDITOS_RAZAO`",	
"`TOTAL_DESPESA_RAZAO`",
"`TOTAL_CREDITO_RAZAO`",
"`TOTAL_CONSOLIDADO_RAZAO`",
"`RNO`",
"`VALOR_RNO`",
"`CONTA_1`",
"`VALOR_CONTA_1`",
"`CONTA_2`",
"`VALOR_CONTA_2`",
"`NOVO_X_LEGADO`",
"`MES_FECH`",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela FIT2 Abaixo

# COMMAND ----------

# Cria excel FIT
fecham_trab_razao_rno_fit2 = fecham_trab_razao_rno_7.select(
"`LINHA`",
"`ID PROCESSO`",
"`Centro de Custo (M)`",
"`Provisão Total (M)`",
"`EMPRESA: PROVISÃO MOV. (M)`", 
"`EMPRESA: CORREÇÃO (M)`",
"`DEMITIDO POR REESTRUTURAÇÃO`",
"`Indicação Processo Estratégico`",
"`NOVOS`",
"`REATIVADOS`",
"`ENCERRADOS`",
"`TIPO_PGTO`",
"`SUM_OF_VALOR`",
"`OUTRAS_ADICOES`",
"`OUTRAS_REVERSOES`",
"`VALOR INSS DECAÍDO`",
"`MOTIVO_MOVIMENTAÇÃO`",
"`MOTIVO_PAGAMENTO`",
"`BU`",
"`AREA_FUNCIONAL`",
"`ACORDO_RAZAO`",
"`CNOVA_RAZAO`", 
"`CONDENACAO_RAZAO`", 
"`CREDITOS_TERCEIROS_RAZAO`", 
"`FK_RAZAO`", 
"`GPA_RAZAO`", 
"`IMPOSTOS_RAZAO`", 
"`LIBERACAO_DEP_JUD_RAZAO`", 
"`LIBERACAO_DEP_JUD_CI_RAZAO`", 
"`PENHORA_RAZAO`", 
"`RECLASSIFICACAO_RAZAO`", 
"`CREDITOS_RAZAO`", 
"`TOTAL_DESPESA_RAZAO`", 
"`TOTAL_CREDITO_RAZAO`", 
"`TOTAL_CONSOLIDADO_RAZAO`", 
"`RNO`", 
"`VALOR_RNO`",
"`MES_FECH`",
"`NOVO_X_LEGADO`",
"`CONTA_1`",
"`VALOR_CONTA_1`", 
"`CONTA_2`", 
"`VALOR_CONTA_2`",
"`CONTA_3`",
"`VALOR_CONTA_3`", 
"`COMPLEMENTO_FK_RAZAO`", 
"`RECLASSIFICACOES_FK_RAZAO`", 
"`RECLASSIFICACAO_FK_RAZAO`", 
"`TOTAL_FK_RAZAO`", 
"`AJUSTE_CNOVA_RAZAO`", 
"`AJUSTE_DEP_JUD_RAZAO`", 
"`AJUSTE_GPA_RAZAO`",
# "`INSS DECAÍDO`",
# "`INSS DECAÍDO LANÇAMENTO EXTRAORDINÁRIO`"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela historico abaixo

# COMMAND ----------

# MAGIC %md
# MAGIC ####Junta a base do mês com a base histórica

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cria base final em excel

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = fecham_trab_razao_rno_7.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_fech_trab_razao_rno_{nmmes}_f.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='FECH_RAZAO_RNO')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/df_fech_trab_razao_rno_{nmmes}_MES.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cria base final em excel FIT

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = fecham_trab_razao_rno_fit.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_fech_trab_razao_rno_{nmmes}_f.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='FECH_RAZAO_RNO')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/df_fech_trab_razao_rno_{nmmes}_FIT.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FIT 2 excel

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = fecham_trab_razao_rno_fit2.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_fech_trab_razao_rno_{nmmes}_fit2.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='FECH_RAZAO_RNO')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/df_fech_trab_razao_rno_{nmmes}_FIT2.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

df_fech_rno_trab_set_24 = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/df_fech_trab_razao_rno_09_MES.xlsx'

df_fech_trab_set_24_final= read_excel(df_fech_rno_trab_set_24,'A1')

# COMMAND ----------

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("Centro de Custo (M)", col("Centro de Custo (M)").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("Centro de Custo (M-1)", col("Centro de Custo (M-1)").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("ACORDO_RAZAO", col("ACORDO_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("CNOVA_RAZAO", col("CNOVA_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("COMPLEMENTO_FK_RAZAO", col("COMPLEMENTO_FK_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("RECLASSIFICACOES_FK_RAZAO", col("RECLASSIFICACOES_FK_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("TOTAL_FK_RAZAO", col("TOTAL_FK_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("CREDITOS_RAZAO", col("CREDITOS_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("RECUP_INDENIZ_TRAB_RAZAO", col("RECUP_INDENIZ_TRAB_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("TOTAL_DESPESA_RAZAO", col("TOTAL_DESPESA_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("TOTAL_CREDITO_RAZAO", col("TOTAL_CREDITO_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("TOTAL_CREDITO_RAZAO", col("TOTAL_CREDITO_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("TOTAL_CONSOLIDADO_RAZAO", col("TOTAL_CONSOLIDADO_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("CONTA_1", col("CONTA_1").cast("double"))


df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("VALOR_CONTA_1", col("VALOR_CONTA_1").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("VALOR_CONTA_2", col("VALOR_CONTA_2").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("VALOR_CONTA_3", col("VALOR_CONTA_3").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("AJUSTE_CNOVA_RAZAO", col("AJUSTE_CNOVA_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("AJUSTE_DEP_JUD_RAZAO", col("AJUSTE_DEP_JUD_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("AJUSTE_GPA_RAZAO", col("AJUSTE_GPA_RAZAO").cast("double"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("AJUSTE_GPA_RAZAO", col("AJUSTE_GPA_RAZAO").cast("double"))



# COMMAND ----------

from pyspark.sql.functions import col

df_fech_trab_set_24_final = df_fech_trab_set_24_final.filter(col("MES_FECH") >= "2024-09-01")

# COMMAND ----------

from pyspark.sql.functions import col

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn(
    "SOCIO: Classificação Mov. (M)", 
    col("`SOCIO: Classificação Mov. (M)`").cast("float")
)

# COMMAND ----------

from pyspark.sql.functions import col

# Convertendo todas as colunas listadas para float
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("Provisão Total Passivo (M)", col("Provisão Total Passivo (M)").cast("float"))
df_fech_trab_set_24_final= df_fech_trab_set_24_final.withColumn("Socio: Provisão (M-1)", col("`Socio: Provisão (M-1)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final .withColumn("Socio: Correção (M-1)", col("`Socio: Correção (M-1)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("Socio: Provisão Total (M-1)", col("Socio: Provisão Total (M-1)").cast("float"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("SOCIO: Provisão Mov. (M)", col("`SOCIO: Provisão Mov. (M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("SOCIO: Correção Mov. (M)", col("`SOCIO: Correção Mov. (M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("SOCIO: Provisão Mov. Total (M)", col("`SOCIO: Provisão Mov. Total (M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("SOCIO: Provisão Total (M)", col("`SOCIO: Provisão Total (M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("SOCIO: Correção (M-1)_0001", col("`SOCIO: Correção (M-1)_0001`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("SOCIO: Correção (M)", col("`SOCIO: Correção (M)`").cast("float"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("SOCIO: Prov. Total Passivo (M)", col("`SOCIO: Prov. Total Passivo (M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("Empresa: Provisão (M-1)", col("`Empresa: Provisão (M-1)`").cast("float"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("Empresa: Correção (M-1)", col("`Empresa: Correção (M-1)`").cast("float"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("Empresa: Provisão Total (M-1)", col("`Empresa: Provisão Total (M-1)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("EMPRESA: Classificação Mov.(M)", col("`EMPRESA: Classificação Mov.(M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("EMPRESA: Provisão Mov. (M)", col("`EMPRESA: Provisão Mov. (M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("EMPRESA: Correção Mov. (M)", col("`EMPRESA: Correção Mov. (M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("EMPRESA: Provisão Mov.Total(M)", col("`EMPRESA: Provisão Mov.Total(M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("EMPRESA: Provisão Total (M)", col("`EMPRESA: Provisão Total (M)`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("EMPRESA: Correção (M-1)_0001", col("`EMPRESA: Correção (M-1)_0001`").cast("float"))
df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("EMPRESA: Correção (M)", col("`EMPRESA: Correção (M)`").cast("float"))

df_fech_trab_set_24_final = df_fech_trab_set_24_final.withColumn("EMPRESA: Prov Total Passivo(M)", col("`EMPRESA: Prov Total Passivo(M)`").cast("float"))


# COMMAND ----------

df_fech_trab_set_24_report = df_fech_trab_set_24_final.select(
    "LINHA", "ID PROCESSO", "PASTA", "`Área do Direito`", "`Sub-área do Direito`", "`Grupo (M-1)`", 
    "`Empresa (M-1)`", "`Grupo (M)`", "`Empresa (M)`", "`STATUS (M-1)`", "`STATUS (M)`", "`Centro de Custo (M-1)`", 
    "`Centro de Custo (M)`", "DATACADASTRO", "Reabertura", "`Objeto Assunto/Cargo (M-1)`", 
    "`Sub Objeto Assunto/Cargo (M-1)`", "`Objeto Assunto/Cargo (M)`", "`Sub Objeto Assunto/Cargo (M)`", 
    "`Órgão ofensor (Fluxo) (M-1)`", "`Órgão ofensor (Fluxo) (M)`", "`Natureza Operacional (M-1)`", 
    "`Natureza Operacional (M)`", "`Média de Pagamento`", "`% Risco`", "Distribuição", "`Nº Processo`", 
    "Escritorio", "`Vlr Causa`", "`% Sócio (M-1)`", "`% Empresa (M-1)`", "`% Sócio (M)`", "`% Empresa (M)`", 
    "`Provisão (M-1)`", "`Correção (M-1)`", "`Provisão Total (M-1)`", "`Classificação Mov. (M)`", 
    "`Provisão Mov. (M)`", "`Correção Mov. (M)`", "`Provisão Mov. Total (M)`", "`Provisão Total (M)`", 
    "`Correção  (M-1)`", "`Correção (M)`", "`Provisão Total Passivo (M)`", "`Socio: Provisão (M-1)`", 
    "`Socio: Correção (M-1)`", "`Socio: Provisão Total (M-1)`", "`SOCIO: Classificação Mov. (M)`", 
    "`SOCIO: Provisão Mov. (M)`", "`SOCIO: Correção Mov. (M)`", "`SOCIO: Provisão Mov. Total (M)`", 
    "`SOCIO: Provisão Total (M)`", "`SOCIO: Correção (M-1)_0001`", "`SOCIO: Correção (M)`", 
    "`SOCIO: Prov. Total Passivo (M)`", "`Empresa: Provisão (M-1)`", "`Empresa: Correção (M-1)`", 
    "`Empresa: Provisão Total (M-1)`", "`EMPRESA: Classificação Mov.(M)`", "`EMPRESA: Provisão Mov. (M)`", 
    "`EMPRESA: Correção Mov. (M)`", "`EMPRESA: Provisão Mov.Total(M)`", "`EMPRESA: Provisão Total (M)`", 
    "`EMPRESA: Correção (M-1)_0001`", "`EMPRESA: Correção (M)`", "`EMPRESA: Prov Total Passivo(M)`", 
    "`Demitido por Reestruturação`", "`Indicação Processo Estratégico`", "NOVOS", "REATIVADOS", "ENCERRADOS", 
    "ESTOQUE", "`SUB TIPO`", "`TIPO_PGTO`", "`PARCELAMENTO CONDENAÇÃO`", "`PARCELAMENTO ACORDO`", "`SUM_of_VALOR`", 
    "`OUTRAS_ADICOES`", "`OUTRAS_REVERSOES`", "`DOC.`", "`Valor INSS Decaído`", "`ESTRATÉGIA (M-1)`", 
    "`ELEGÍVEL (M-1)`", "`FASE (M-1)`", "`TIPO DE CALCULO (M-1)`", "`ELEGIVEL (M)`", "`FASE (M)`", 
    "`TIPO DE CALCULO (M)`", "`MOTIVO_MOVIMENTAÇÃO`", "`MOTIVO_PAGAMENTO`", "DP_FASE", "`PROCESSO - ESTEIRA`", 
    "ESTRATEGIA", "BU", "VP", "DIRETORIA", "RESPONSAVEL_AREA", "AREA_FUNCIONAL", "DP_VP_DIRETORIA", 
    "`PROCESSO_COM_DOCUMENTO`", "`ACORDO_RAZAO`", "`CNOVA_RAZAO`", "`CONDENACAO_RAZAO`", "`CREDITOS_TERCEIROS_RAZAO`", 
    "`FK_RAZAO`", "`GPA_RAZAO`", "`IMPOSTOS_RAZAO`", "`LIBERACAO_DEP_JUD_RAZAO`", "`LIBERACAO_DEP_JUD_CI_RAZAO`", 
    "`PAGAMENTO_CUSTAS_RAZAO`", "`PENHORA_RAZAO`", "`BX_NAO_COBRAVEIS_RAZAO`", "`NAO_COBRAVEIS_RAZAO`", 
    "`TOTAL_DESPESA_RAZAO`", "`TOTAL_CREDITO_RAZAO`", "`TOTAL_CONSOLIDADO_RAZAO`", "RNO", "`VALOR_RNO`", 
    "MES_FECH", "`RECLASSIFICACAO_RAZAO`", "CONTA_1", "`VALOR_CONTA_1`", "CONTA_2", "`VALOR_CONTA_2`", 
    "`CREDITOS_RAZAO`", "`NOVO_X_LEGADO`"

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
df_fech_trab_set_24_report_pandas = df_fech_trab_set_24_report.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_fech_trab_set_24_report.xlsx'
df_fech_trab_set_24_report_pandas.to_excel(local_path, index=False, sheet_name='FECH_RAZAO_RNO')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/FECHAM_TRAB_RAZAO_RNO_202409_report.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

FECHAM_TRAB_RAZAO_RNO_202408 = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/FECHAM_TRAB_RAZAO_RNO_202408.xlsx'

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL= read_excel(FECHAM_TRAB_RAZAO_RNO_202408,'A1')

# COMMAND ----------

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("Centro de Custo (M)", col("Centro de Custo (M)").cast("double"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("Centro de Custo (M-1)", col("Centro de Custo (M-1)").cast("double"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("ACORDO_RAZAO", col("ACORDO_RAZAO").cast("double"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("CNOVA_RAZAO", col("CNOVA_RAZAO").cast("double"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("COMPLEMENTO_FK_RAZAO", col("COMPLEMENTO_FK_RAZAO").cast("double"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("RECLASSIFICACOES_FK_RAZAO", col("RECLASSIFICACOES_FK_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("TOTAL_FK_RAZAO", col("TOTAL_FK_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("CREDITOS_RAZAO", col("CREDITOS_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("RECUP_INDENIZ_TRAB_RAZAO", col("RECUP_INDENIZ_TRAB_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("TOTAL_DESPESA_RAZAO", col("TOTAL_DESPESA_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("TOTAL_CREDITO_RAZAO", col("TOTAL_CREDITO_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("TOTAL_CREDITO_RAZAO", col("TOTAL_CREDITO_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("TOTAL_CONSOLIDADO_RAZAO", col("TOTAL_CONSOLIDADO_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("CONTA_1", col("CONTA_1").cast("double"))


FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("VALOR_CONTA_1", col("VALOR_CONTA_1").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("VALOR_CONTA_2", col("VALOR_CONTA_2").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("VALOR_CONTA_3", col("VALOR_CONTA_3").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("AJUSTE_CNOVA_RAZAO", col("AJUSTE_CNOVA_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("AJUSTE_DEP_JUD_RAZAO", col("AJUSTE_DEP_JUD_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("AJUSTE_GPA_RAZAO", col("AJUSTE_GPA_RAZAO").cast("double"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("AJUSTE_GPA_RAZAO", col("AJUSTE_GPA_RAZAO").cast("double"))




# COMMAND ----------

from pyspark.sql.functions import col

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.filter(col("MES_FECH") >= "2024-07-01")

# COMMAND ----------

from pyspark.sql.functions import col

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn(
    "SOCIO: Classificação Mov. (M)", 
    col("`SOCIO: Classificação Mov. (M)`").cast("float")
)

# COMMAND ----------

from pyspark.sql.functions import col

# Convertendo todas as colunas listadas para float
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("Provisão Total Passivo (M)", col("Provisão Total Passivo (M)").cast("float"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL= FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("Socio: Provisão (M-1)", col("`Socio: Provisão (M-1)`").cast("float"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL =FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("Socio: Correção (M-1)", col("`Socio: Correção (M-1)`").cast("float"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("Socio: Provisão Total (M-1)", col("Socio: Provisão Total (M-1)").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("SOCIO: Provisão Mov. (M)", col("`SOCIO: Provisão Mov. (M)`").cast("float"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("SOCIO: Correção Mov. (M)", col("`SOCIO: Correção Mov. (M)`").cast("float"))
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("SOCIO: Provisão Mov. Total (M)", col("`SOCIO: Provisão Mov. Total (M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("SOCIO: Provisão Total (M)", col("`SOCIO: Provisão Total (M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("SOCIO: Correção (M-1)_0001", col("`SOCIO: Correção (M-1)_0001`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("SOCIO: Correção (M)", col("`SOCIO: Correção (M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("SOCIO: Prov. Total Passivo (M)", col("`SOCIO: Prov. Total Passivo (M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("Empresa: Provisão (M-1)", col("`Empresa: Provisão (M-1)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("Empresa: Correção (M-1)", col("`Empresa: Correção (M-1)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("Empresa: Provisão Total (M-1)", col("`Empresa: Provisão Total (M-1)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("EMPRESA: Classificação Mov.(M)", col("`EMPRESA: Classificação Mov.(M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("EMPRESA: Provisão Mov. (M)", col("`EMPRESA: Provisão Mov. (M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("EMPRESA: Correção Mov. (M)", col("`EMPRESA: Correção Mov. (M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("EMPRESA: Provisão Mov.Total(M)", col("`EMPRESA: Provisão Mov.Total(M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("EMPRESA: Provisão Total (M)", col("`EMPRESA: Provisão Total (M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("EMPRESA: Correção (M-1)_0001", col("`EMPRESA: Correção (M-1)_0001`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("EMPRESA: Correção (M)", col("`EMPRESA: Correção (M)`").cast("float"))

FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.withColumn("EMPRESA: Prov Total Passivo(M)", col("`EMPRESA: Prov Total Passivo(M)`").cast("float"))


# COMMAND ----------

fecham_trab_razao_rno_report = FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.select(
    "LINHA", "ID PROCESSO", "PASTA", "`Área do Direito`", "`Sub-área do Direito`", "`Grupo (M-1)`", 
    "`Empresa (M-1)`", "`Grupo (M)`", "`Empresa (M)`", "`STATUS (M-1)`", "`STATUS (M)`", "`Centro de Custo (M-1)`", 
    "`Centro de Custo (M)`", "DATACADASTRO", "Reabertura", "`Objeto Assunto/Cargo (M-1)`", 
    "`Sub Objeto Assunto/Cargo (M-1)`", "`Objeto Assunto/Cargo (M)`", "`Sub Objeto Assunto/Cargo (M)`", 
    "`Órgão ofensor (Fluxo) (M-1)`", "`Órgão ofensor (Fluxo) (M)`", "`Natureza Operacional (M-1)`", 
    "`Natureza Operacional (M)`", "`Média de Pagamento`", "`% Risco`", "Distribuição", "`Nº Processo`", 
    "Escritorio", "`Vlr Causa`", "`% Sócio (M-1)`", "`% Empresa (M-1)`", "`% Sócio (M)`", "`% Empresa (M)`", 
    "`Provisão (M-1)`", "`Correção (M-1)`", "`Provisão Total (M-1)`", "`Classificação Mov. (M)`", 
    "`Provisão Mov. (M)`", "`Correção Mov. (M)`", "`Provisão Mov. Total (M)`", "`Provisão Total (M)`", 
    "`Correção  (M-1)`", "`Correção (M)`", "`Provisão Total Passivo (M)`", "`Socio: Provisão (M-1)`", 
    "`Socio: Correção (M-1)`", "`Socio: Provisão Total (M-1)`", "`SOCIO: Classificação Mov. (M)`", 
    "`SOCIO: Provisão Mov. (M)`", "`SOCIO: Correção Mov. (M)`", "`SOCIO: Provisão Mov. Total (M)`", 
    "`SOCIO: Provisão Total (M)`", "`SOCIO: Correção (M-1)_0001`", "`SOCIO: Correção (M)`", 
    "`SOCIO: Prov. Total Passivo (M)`", "`Empresa: Provisão (M-1)`", "`Empresa: Correção (M-1)`", 
    "`Empresa: Provisão Total (M-1)`", "`EMPRESA: Classificação Mov.(M)`", "`EMPRESA: Provisão Mov. (M)`", 
    "`EMPRESA: Correção Mov. (M)`", "`EMPRESA: Provisão Mov.Total(M)`", "`EMPRESA: Provisão Total (M)`", 
    "`EMPRESA: Correção (M-1)_0001`", "`EMPRESA: Correção (M)`", "`EMPRESA: Prov Total Passivo(M)`", 
    "`Demitido por Reestruturação`", "`Indicação Processo Estratégico`", "NOVOS", "REATIVADOS", "ENCERRADOS", 
    "ESTOQUE", "`SUB TIPO`", "`TIPO_PGTO`", "`PARCELAMENTO CONDENAÇÃO`", "`PARCELAMENTO ACORDO`", "`SUM_of_VALOR`", 
    "`OUTRAS_ADICOES`", "`OUTRAS_REVERSOES`", "`DOC.`", "`Valor INSS Decaído`", "`ESTRATÉGIA (M-1)`", 
    "`ELEGÍVEL (M-1)`", "`FASE (M-1)`", "`TIPO DE CALCULO (M-1)`", "`ELEGIVEL (M)`", "`FASE (M)`", 
    "`TIPO DE CALCULO (M)`", "`MOTIVO_MOVIMENTAÇÃO`", "`MOTIVO_PAGAMENTO`", "DP_FASE", "`PROCESSO - ESTEIRA`", 
    "ESTRATEGIA", "BU", "VP", "DIRETORIA", "RESPONSAVEL_AREA", "AREA_FUNCIONAL", "DP_VP_DIRETORIA", 
    "`PROCESSO_COM_DOCUMENTO`", "`ACORDO_RAZAO`", "`CNOVA_RAZAO`", "`CONDENACAO_RAZAO`", "`CREDITOS_TERCEIROS_RAZAO`", 
    "`FK_RAZAO`", "`GPA_RAZAO`", "`IMPOSTOS_RAZAO`", "`LIBERACAO_DEP_JUD_RAZAO`", "`LIBERACAO_DEP_JUD_CI_RAZAO`", 
    "`PAGAMENTO_CUSTAS_RAZAO`", "`PENHORA_RAZAO`", "`BX_NAO_COBRAVEIS_RAZAO`", "`NAO_COBRAVEIS_RAZAO`", 
    "`TOTAL_DESPESA_RAZAO`", "`TOTAL_CREDITO_RAZAO`", "`TOTAL_CONSOLIDADO_RAZAO`", "RNO", "`VALOR_RNO`", 
    "MES_FECH", "`RECLASSIFICACAO_RAZAO`", "CONTA_1", "`VALOR_CONTA_1`", "CONTA_2", "`VALOR_CONTA_2`", 
    "`CREDITOS_RAZAO`", "`NOVO_X_LEGADO`"
)

# COMMAND ----------



# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL_pandas = fecham_trab_razao_rno_report.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL.xlsx'
FECHAM_TRAB_RAZAO_RNO_202408_HIST_FINAL_pandas.to_excel(local_path, index=False, sheet_name='FECH_RAZAO_RNO')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/FECHAM_TRAB_RAZAO_RNO_202408_report.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

df_fech_trab_razao_rno_09_MES = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/df_fech_trab_razao_rno_09_MES.xlsx'

df_fech_trab_razao_rno_09_MES_final= read_excel(df_fech_trab_razao_rno_09_MES,'A1')

# COMMAND ----------

from pyspark.sql.functions import col

FECHAM_TRAB_RAZAO_RNO_2023A202409_2023 = FECHAM_TRAB_RAZAO_RNO_2023A202409.filter(col("MES_FECH") >= "2023-12-01")

# COMMAND ----------

path_df_ago_24= '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/FECHAM_TRAB_RAZAO_RNO_202408.xlsx'

df_path_df_ago_24 = read_excel(path_df_ago_24,'A1')

# COMMAND ----------

df_rno_hist_2224_fit2 = df_path_df_ago_24.select(
    "`LINHA`",
    "`ID PROCESSO`",
    "`PASTA`",
    "`Área do Direito`",
    "`Sub-área do Direito`", 
    "`Grupo (M-1)`",
    "`Empresa (M-1)`",
    "`Grupo (M)`",
    "`Empresa (M)`",
    "`STATUS (M-1)`",
    "`STATUS (M)`",
    "`Centro de Custo (M-1)`",
    "`Centro de Custo (M)`",
    "`DATACADASTRO`",
    "`OUTRAS_REVERSOES`",
    "`VALOR INSS DECAÍDO`",
    "`MOTIVO_MOVIMENTAÇÃO`",
    "`MOTIVO_PAGAMENTO`",
    "`BU`",
    "`AREA_FUNCIONAL`",
    "`ACORDO_RAZAO`",
    "`CNOVA_RAZAO`", 
    "`CONDENACAO_RAZAO`", 
    "`CREDITOS_TERCEIROS_RAZAO`", 
    "`FK_RAZAO`", 
    "`GPA_RAZAO`", 
    "`IMPOSTOS_RAZAO`", 
    "`LIBERACAO_DEP_JUD_RAZAO`", 
    "`LIBERACAO_DEP_JUD_CI_RAZAO`", 
    "`PENHORA_RAZAO`", 
    "`RECLASSIFICACAO_RAZAO`", 
    "`CREDITOS_RAZAO`", 
    "`TOTAL_DESPESA_RAZAO`", 
    "`TOTAL_CREDITO_RAZAO`", 
    "`TOTAL_CONSOLIDADO_RAZAO`", 
    "`RNO`", 
    "`VALOR_RNO`",
    "`MES_FECH`",
    "`NOVO_X_LEGADO`",
    "`CONTA_1`",
    "`VALOR_CONTA_1`", 
    "`CONTA_2`", 
    "`VALOR_CONTA_2`",
    "`CONTA_3`",
    "`VALOR_CONTA_3`", 
    "`COMPLEMENTO_FK_RAZAO`", 
    "`RECLASSIFICACOES_FK_RAZAO`", 
    "`RECLASSIFICACAO_FK_RAZAO`", 
    "`TOTAL_FK_RAZAO`", 
    "`AJUSTE_CNOVA_RAZAO`", 
    "`AJUSTE_DEP_JUD_RAZAO`", 
    "`AJUSTE_GPA_RAZAO`",
    # "`INSS DECAÍDO`",
    # "`INSS DECAÍDO LANÇAMENTO EXTRAORDINÁRIO`"
)

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

nmmes = dbutils.widgets.get("nmmes")

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_rno_hist_2224_fit2.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/FECHAM_TRAB_RAZAO_RNO_{nmmes}_FIT2.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='FECH_RAZAO_RNO')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/FECHAM_TRAB_RAZAO_RNO_{nmmes}_FIT2.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# Cria excel FIT
fecham_trab_razao_rno_fit = df_path_df_ago_24.select(
"`ID PROCESSO`",
"`EMPRESA: Provisão Mov. (M)`",
"`EMPRESA: Correção (M)`",
"`Indicação Processo Estratégico`",
"`NOVOS`",	
"`REATIVADOS`",	
"`ENCERRADOS`",
"`TIPO_PGTO`",
"`SUM_OF_VALOR`",
"`OUTRAS_ADICOES`",
"`OUTRAS_REVERSOES`",
"`Valor INSS Decaído`",
"`MOTIVO_MOVIMENTAÇÃO`",	
"`MOTIVO_PAGAMENTO`",
"`BU`",
"`AREA_FUNCIONAL`",
"`ACORDO_RAZAO`",	
"`CNOVA_RAZAO`",
"`CONDENACAO_RAZAO`",
"`CREDITOS_TERCEIROS_RAZAO`",
"`FK_RAZAO`",
"`GPA_RAZAO`",
"`IMPOSTOS_RAZAO`",	
"`LIBERACAO_DEP_JUD_RAZAO`",
"`LIBERACAO_DEP_JUD_CI_RAZAO`",
"`PAGAMENTO_CUSTAS_RAZAO`",
"`PENHORA_RAZAO`",
"`BX_NAO_COBRAVEIS_RAZAO`",
"`NAO_COBRAVEIS_RAZAO`",
"`CUSTAS_RAZAO`",
"`RECLASSIFICACAO_RAZAO`",
"`COMPLEMENTO_FK_RAZAO`",
"`RECLASSIFICACOES_FK_RAZAO`",
"`RECLASSIFICACAO_FK_RAZAO`",
"`TOTAL_FK_RAZAO`",
"`CREDITOS_RAZAO`",	
"`TOTAL_DESPESA_RAZAO`",
"`TOTAL_CREDITO_RAZAO`",
"`TOTAL_CONSOLIDADO_RAZAO`",
"`RNO`",
"`VALOR_RNO`",
"`CONTA_1`",
"`VALOR_CONTA_1`",
"`CONTA_2`",
"`VALOR_CONTA_2`",
"`NOVO_X_LEGADO`",
"`MES_FECH`",
)

# COMMAND ----------

# Cria excel FIT
fecham_trab_razao_rno_fit2 = df_path_df_ago_24.select(
"`LINHA`",
"`ID PROCESSO`",
"`Centro de Custo (M)`",
"`Provisão Total (M)`",
"`EMPRESA: PROVISÃO MOV. (M)`", 
"`EMPRESA: CORREÇÃO (M)`",
"`DEMITIDO POR REESTRUTURAÇÃO`",
"`Indicação Processo Estratégico`",
"`NOVOS`",
"`REATIVADOS`",
"`ENCERRADOS`",
"`TIPO_PGTO`",
"`SUM_OF_VALOR`",
"`OUTRAS_ADICOES`",
"`OUTRAS_REVERSOES`",
"`VALOR INSS DECAÍDO`",
"`MOTIVO_MOVIMENTAÇÃO`",
"`MOTIVO_PAGAMENTO`",
"`BU`",
"`AREA_FUNCIONAL`",
"`ACORDO_RAZAO`",
"`CNOVA_RAZAO`", 
"`CONDENACAO_RAZAO`", 
"`CREDITOS_TERCEIROS_RAZAO`", 
"`FK_RAZAO`", 
"`GPA_RAZAO`", 
"`IMPOSTOS_RAZAO`", 
"`LIBERACAO_DEP_JUD_RAZAO`", 
"`LIBERACAO_DEP_JUD_CI_RAZAO`", 
"`PENHORA_RAZAO`", 
"`RECLASSIFICACAO_RAZAO`", 
"`CREDITOS_RAZAO`", 
"`TOTAL_DESPESA_RAZAO`", 
"`TOTAL_CREDITO_RAZAO`", 
"`TOTAL_CONSOLIDADO_RAZAO`", 
"`RNO`", 
"`VALOR_RNO`",
"`MES_FECH`",
"`NOVO_X_LEGADO`",
"`CONTA_1`",
"`VALOR_CONTA_1`", 
"`CONTA_2`", 
"`VALOR_CONTA_2`",
"`CONTA_3`",
"`VALOR_CONTA_3`", 
"`COMPLEMENTO_FK_RAZAO`", 
"`RECLASSIFICACOES_FK_RAZAO`", 
"`RECLASSIFICACAO_FK_RAZAO`", 
"`TOTAL_FK_RAZAO`", 
"`AJUSTE_CNOVA_RAZAO`", 
"`AJUSTE_DEP_JUD_RAZAO`", 
"`AJUSTE_GPA_RAZAO`",
# "`INSS DECAÍDO`",
# "`INSS DECAÍDO LANÇAMENTO EXTRAORDINÁRIO`"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ajuste . por ,

# COMMAND ----------

from pyspark.sql.functions import format_number, col, expr

path_hist = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rno/FECHAM_TRAB_RAZAO_RNO_202408_HIST.xlsx'

df_hist = read_excel(path_hist,f"'FECH_RAZAO_RNO'!A1")

colunas_para_converter = ["Média de Pagamento", "Vlr Causa"]

for cols in df_hist.columns:
    if 'TOTAL' in cols:
        colunas_para_converter.append(cols)
    if 'VALOR' in cols:
        colunas_para_converter.append(cols)

for coluna in colunas_para_converter:
    df_hist = df_hist.withColumn(coluna, col(coluna).cast('float'))

# # Converte as colunas para float
# for coluna in colunas_para_converter:
#     df_hist = df_hist.withColumn(
#         coluna,
#         regexp_replace(format_number(col(coluna), 2), '\.', ',')
#     )


# COMMAND ----------

display(df_hist)
