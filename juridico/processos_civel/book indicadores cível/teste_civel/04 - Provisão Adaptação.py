# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela de Provisão Cível
# MAGIC
# MAGIC **Inputs**\
# MAGIC Tabela Pagamentos Cível
# MAGIC
# MAGIC
# MAGIC **Outputs**\
# MAGIC Tabela:\
# MAGIC A DEFINIR

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Une tabelas Fechamento Cível

# COMMAND ----------

tb_fech_civel_provisao = spark.table("databox.juridico_comum.tb_fech_civel_provisao")

# COMMAND ----------

display(tb_fech_civel_provisao)

# COMMAND ----------

tb_fech_civel_provisao.count()

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, when, lit, asc, desc

# COMMAND ----------

# Trata coluna ESTRATEGIA
df_fech_civel_provisao = tb_fech_civel_provisao.withColumn(
    "ESTRATEGIA",
    when(col("ESTRATEGIA").isin("ACO", "ACOR", "ACORD", "ACORDO"), "ACORDO")
    .when(col("ESTRATEGIA").isin("DEF", "DEFE", "DEFES", "DEFESA"), "DEFESA")
    .otherwise(col("ESTRATEGIA"))
)
df_fech_civel_provisao.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO')

# COMMAND ----------

# CRIA BASE COM OS ÚLTIMOS ENCERRAMENTOS
tb_fech_civel_provisao_a = df_fech_civel_provisao.drop( 'ACORDOS', 'CONDENACAO', 'PENHORA', 
                                                     'OUTROS_PAGAMENTOS', 'IMPOSTO', 'GARANTIA', 'TOTAL_PAGAMENTOS') \
                                                     .sort(asc('ID_PROCESSO'), desc('MES_FECH')) \
                                                     .where("ENCERRADOS = 1") \
                                                     .dropDuplicates(['ID_PROCESSO'])
tb_fech_civel_provisao_a.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_66')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Informações Base Histórica de Pagamentos Cível

# COMMAND ----------

#View
hist_pagto_civel_consolidado= spark.read.table("databox.juridico_comum.hist_pagamentos_garantias_civel_f")

# COMMAND ----------

#tabelas para junção modo SQL SPARK
tb_fech_civel_provisao_a.createOrReplaceTempView("TB_FECH_FIN_CIVEL_PROVISAO_A")
hist_pagto_civel_consolidado.createOrReplaceTempView("HIST_PGTO_CIVEL")

# COMMAND ----------

# CARRREGA OS VALORES DA BASE DE PAGAMENTOS
tb_fech_civel_provisao_b = spark.sql("""
SELECT A.*

        ,COALESCE(B.ACORDO, 0.00) AS ACORDOS
		,COALESCE(B.CONDENACAO, 0.00) AS CONDENACAO
		,COALESCE(B.PENHORA, 0.00) AS PENHORA
		,COALESCE(B.GARANTIAS,0.00) AS GARANTIA
		,COALESCE(B.IMPOSTO, 0.00) AS IMPOSTO
		,COALESCE(B.OUTROS_PAGAMENTOS, 0.00) AS OUTROS_PAGAMENTOS
		,B.DATA_EFETIVA_PAGAMENTO

FROM TB_FECH_FIN_CIVEL_PROVISAO_A AS A
LEFT JOIN HIST_PGTO_CIVEL B ON A.ID_PROCESSO = B.PROCESSO_ID
"""
)
tb_fech_civel_provisao_b.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_B')

# COMMAND ----------

display(tb_fech_civel_provisao_b)

# COMMAND ----------

# CRIA O CAMPO COM A SOMA DE TODOS OS PAGAMENTOS
tb_fech_civel_provisao_c = spark.sql("""
SELECT A.*,
    ACORDOS + CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA  AS TOTAL_PAGAMENTOS
FROM TB_FECH_FIN_CIVEL_PROVISAO_B A
"""
)
tb_fech_civel_provisao_c.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_C')

# COMMAND ----------

display(tb_fech_civel_provisao_c)

# COMMAND ----------

tb_fech_civel_provisao_d = merge_dfs(df_fech_civel_provisao, tb_fech_civel_provisao_c, ['ID_PROCESSO', 'MES_FECH','LINHA'])


# COMMAND ----------

tb_fech_civel_provisao_d.count()

# COMMAND ----------

tb_fech_civel_provisao_d.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_D')

# COMMAND ----------

df_fech_civel_provisao_e = spark.sql("""
SELECT *,
'N/A' AS  DP_FASE,
CASE WHEN ACORDOS > 0 THEN 'ACORDO'
WHEN CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA > 0 THEN 'CONDENACAO'
ELSE 'SEM ONUS'
END MOTIVO_ENC_AGRP
FROM 
TB_FECH_FIN_CIVEL_PROVISAO_D A
"""
)

df_fech_civel_provisao_e.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_2')

# COMMAND ----------

display(df_fech_civel_provisao_e)

# COMMAND ----------

# %sql
# SELECT
#   ID_PROCESSO,
#   MES_FECH,
#   FASE_M,
#   DP_FASE,
#   MOTIVO_ENC_AGRP,
#   ESTRATEGIA,
#   PROVISAO_MOV_TOTAL_M,
#   NATUREZA_OPERACIONAL_M
# FROM
#   TB_FECH_FIN_TRAB_PROVISAO_2
# WHERE
#   ID_PROCESSO IN (629050, 661541, 744740, 854476)
# AND
#  PROVISAO_MOV_TOTAL_M < 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provisão 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prep

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/processos trabalhista/book indicadores trabalhista/mes_fech"

# COMMAND ----------

# dates = df_arquivos.select('mes_fech').where("mes_fech >= '2021-07-01'").collect()

df_dates = df_arquivos.select('mes_fech').where("mes_fech >= '2021-09-01'")
dates = [row['mes_fech'] for row in df_dates.collect()]
list_mes_fech = [d.strftime("%Y-%m-%d") for d in dates]
print(list_mes_fech)

# COMMAND ----------

# Define the schema
schema_f = StructType([
    StructField("ID_PROCESSO", DoubleType(), True),
    StructField("NATUREZA_OPERACIONAL_M", StringType(), True),
    StructField("DP_FASE", StringType(), True),
    StructField("MOTIVO_ENC_AGRP", StringType(), True),
    StructField("TOTAL_PAGAMENTOS", FloatType(), True),
    StructField("MES_FECH", DateType(), True),
    StructField("VALOR_DE_PROVISAO", FloatType(), True)
])

# Create an empty DataFrame with the specified schema
df_saldo_tot_provisao_civel_f = spark.createDataFrame([], schema_f)

# COMMAND ----------

display(df_saldo_tot_provisao_civel_f)

# COMMAND ----------

for mes_fech in list_mes_fech:
    MESFECH = "'"+mes_fech+"'"
    print(MESFECH)
    break

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loop

# COMMAND ----------

for mes_fech in list_mes_fech:

  MESFECH = "'"+mes_fech+"'"
  
  # CRIA BASE COM OS VALORES NEGATIVOS DE PROVISÃO
  df_saldo_tot_prov_civel = spark.sql(f"""
  SELECT
    ID_PROCESSO,
    NATUREZA_OPERACIONAL_M,
    MES_FECH::DATE AS MES_PROV,
    PROVISAO_MOV_TOTAL_M AS VALOR_DE_PROVISAO,
    MOTIVO_ENC_AGRP,
    DP_FASE,
    ESTRATEGIA
  FROM
    TB_FECH_FIN_CIVEL_PROVISAO_2
  WHERE
    MES_FECH > '2021-07-01'
  AND MES_FECH <= {MESFECH}
  AND PROVISAO_MOV_TOTAL_M < 0
  ORDER BY
    ID_PROCESSO, MES_FECH
  """
  )

  df_saldo_tot_provisao_civel_f.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO")
  # print(df_saldo_tot_prov.count())

  # Filtra a estratégia mais recente
  df_estrategia_civel = spark.sql(f"""
  WITH ordena_estr AS (
  SELECT
    ID_PROCESSO,
    MES_FECH::DATE,
    ESTRATEGIA,
    ROW_NUMBER() OVER (PARTITION BY ID_PROCESSO ORDER BY MES_FECH DESC) AS row_num
  FROM
    TB_FECH_FIN_CIVEL_PROVISAO_2
  WHERE
    MES_FECH > '2021-07-01'
    AND MES_FECH <= {MESFECH}
    AND NATUREZA_OPERACIONAL_M != 'TERCEIRO INSOLVENTE'
    AND ESTRATEGIA = 'DEFESA'
  ORDER BY
    ID_PROCESSO ASC, MES_FECH DESC
  )
  SELECT
    ID_PROCESSO,
    MES_FECH::DATE AS MES_ULT_ESTRAT,
    ESTRATEGIA AS ULT_ESTRAT
  FROM
    ordena_estr
  WHERE
    row_num = 1
  """
  )

  df_estrategia_civel.createOrReplaceTempView("TB_ESTRATEGIA_CIVEL")
  # print(df_estrategia.count())


  # CARREGA OS DADOS DA ÚLTIMA ESTRATÉGIA
  df_saldo_tot_prov_1_civel = spark.sql(f"""
  SELECT
    A.ID_PROCESSO,
    A.NATUREZA_OPERACIONAL_M,
    A.MES_PROV,
    A.VALOR_DE_PROVISAO,
    A.MOTIVO_ENC_AGRP,
    A.DP_FASE,
    A.ESTRATEGIA,
    B.MES_ULT_ESTRAT,
    B.ULT_ESTRAT
  FROM
    TB_SALDO_TOT_PROVISAO A
  LEFT JOIN
    TB_ESTRATEGIA_CIVEL B ON A.ID_PROCESSO = B.ID_PROCESSO
  """
  )

  df_saldo_tot_prov_1.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_1")
  # print(df_saldo_tot_prov_1.count())


  # CONSIDERA APENAS OS VALORES POSTERIORES A ESTRATÉGIA DEFESA
  df_saldo_tot_prov_2_civel = spark.sql(f"""
  SELECT 
    ID_PROCESSO,
    NATUREZA_OPERACIONAL_M,
    MES_PROV,
    MOTIVO_ENC_AGRP,
    DP_FASE,
    ESTRATEGIA,
    MES_ULT_ESTRAT,
    ULT_ESTRAT,
  CASE
      WHEN ULT_ESTRAT = 'DEFESA' AND MES_PROV <= MES_ULT_ESTRAT THEN NULL
      ELSE VALOR_DE_PROVISAO 
  END AS VALOR_DE_PROVISAO
  FROM TB_SALDO_TOT_PROVISAO_1
  """
  )

  df_saldo_tot_prov_2_civel.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_2")
  # print(df_saldo_tot_prov_2.count())


  # SOMA OS VALORES DE PROVISÃO
  df_saldo_tot_prov_3_civel = spark.sql(f"""
  SELECT
    ID_PROCESSO,
    SUM(VALOR_DE_PROVISAO) * -1 AS VALOR_DE_PROVISAO,
    MAX(MES_PROV) AS MES_FECH
  FROM
    TB_SALDO_TOT_PROVISAO_2
  GROUP BY 1
  """
  )

  df_saldo_tot_prov_3.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_3")
  # print(df_saldo_tot_prov_3.count())


  # CRIA BASE COM OS PROCESSOS ENCERRADOS
  df_encerrados_civel = spark.sql(f"""
  SELECT
    ID_PROCESSO,
    NATUREZA_OPERACIONAL_M,
    DP_FASE,
    MOTIVO_ENC_AGRP,
    MES_FECH,
    TOTAL_PAGAMENTOS
  FROM
    TB_FECH_FIN_CIVEL_PROVISAO_2
  WHERE
    ENCERRADOS = 1
    AND MES_FECH = {MESFECH}
  ORDER BY
    ID_PROCESSO
  """
  )

  df_encerrados_civel.createOrReplaceTempView("TB_ENCERRADOS_CIVEL")
  # print(df_encerrados.count())


  # BUSCA O VALOR DE PROVISÃO DO MÊS
  df_saldo_tot_prov_4 = spark.sql(f"""
  SELECT
    A.ID_PROCESSO,
    A.NATUREZA_OPERACIONAL_M,
    A.DP_FASE,
    A.MOTIVO_ENC_AGRP,
    A.TOTAL_PAGAMENTOS,
    A.MES_FECH::DATE,
    B.VALOR_DE_PROVISAO
  FROM
    TB_ENCERRADOS_CIVEL AS A
    LEFT JOIN TB_SALDO_TOT_PROVISAO_3 AS B ON A.ID_PROCESSO = B.ID_PROCESSO
  """
  )

  df_saldo_tot_prov_4.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_4")

  df_saldo_tot_provisao_f = df_saldo_tot_provisao_f.unionAll(df_saldo_tot_prov_4)
  # print(df_saldo_tot_provisao_f.count())

# COMMAND ----------

df_saldo_tot_provisao_f.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_F")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TB_PROVISAO_TOTAL

# COMMAND ----------

# CRIA BASE ANALÍTICA COM OS PROCESSOS ENCERRADOS
df_provisao_total = spark.sql("""
SELECT DISTINCT
  A.ID_PROCESSO,
  A.NATUREZA_OPERACIONAL_M,
  A.DP_FASE,
  A.ESTRATEGIA,
  A.PROVISAO_MOV_M,
  A.PROVISAO_MOV_TOTAL_M,
  A.PROVISAO_TOTAL_M,
  A.PROVISAO_TOTAL_PASSIVO_M,
  A.TOTAL_PAGAMENTOS,
  A.MES_FECH
FROM
  TB_FECH_FIN_TRAB_PROVISAO_2 A
WHERE
  ENCERRADOS = 1  
"""
)

df_saldo_tot_provisao_f.createOrReplaceTempView("TB_PROVISAO_TOTAL")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MES_FECH, DP_FASE, count(ID_PROCESSO) FROM TB_PROVISAO_TOTAL GROUP BY ALL

# COMMAND ----------

"temp_validacao_provisao_trab202404"

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDAÇÃO

# COMMAND ----------

df_saldo_tot_provisao_f.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM TB_SALDO_TOT_PROVISAO_F WHERE ID_PROCESSO IN (629050)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT
# MAGIC --   A.ID_PROCESSO,
# MAGIC --   A.CONDENACAO,
# MAGIC --   B.CONDENACAO,
# MAGIC --   A.DT_ULT_PGTO,
# MAGIC --   B.DT_ULT_PGTO
# MAGIC -- FROM
# MAGIC --   TB_FECH_FIN_TRAB_PROVISAO AS A
# MAGIC --   LEFT JOIN global_temp.HIST_PAGAMENTOS_GARANTIA_TRAB_FF B ON A.ID_PROCESSO = B.PROCESSO_ID
# MAGIC -- WHERE
# MAGIC --   ID_PROCESSO IN (
# MAGIC -- --243262,
# MAGIC -- 595538)
# MAGIC -- -- 597247,
# MAGIC -- -- 657998,
# MAGIC -- -- 806740,
# MAGIC -- -- 935987,
# MAGIC -- -- 658198,
# MAGIC -- -- 678180,
# MAGIC -- -- 805963,
# MAGIC -- -- 805963)
# MAGIC -- AND A.DT_ULT_PGTO IS NOT NULL
# MAGIC -- ORDER BY
# MAGIC --   A.ID_PROCESSO

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT ID_PROCESSO,
# MAGIC --   CONDENACAO,
# MAGIC --   MES_FECH::DATE,
# MAGIC --   DT_ULT_PGTO FROM TB_FECH_FIN_TRAB_PROVISAO_99 WHERE ID_PROCESSO IN (
# MAGIC -- --243262,
# MAGIC -- 595538)
# MAGIC -- -- 597247,
# MAGIC -- -- 657998,
# MAGIC -- -- 806740,
# MAGIC -- -- 935987,
# MAGIC -- -- 658198,
# MAGIC -- -- 678180,
# MAGIC -- -- 805963,
# MAGIC -- -- 805963)
# MAGIC -- AND DT_ULT_PGTO IS NOT NULL
# MAGIC -- ORDER BY ID_PROCESSO
