# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela de Provisão Cível
# MAGIC - **Inputs**\
# MAGIC Tabelas:
# MAGIC   - União das tb_fecham_civel_{comp}
# MAGIC   -
# MAGIC - **Outputs**\
# MAGIC Arquivos: 
# MAGIC   - PROVISAO_TRAB;
# MAGIC   - TB_PROVISAO_TOTAL
# MAGIC   - TB_ENCERR_X_PROVISAO_TRAB;

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Une tabelas Fechamento Cível

# COMMAND ----------

tb_fech_civel_provisao = spark.table("databox.juridico_comum.tb_fech_civel_provisao")

# COMMAND ----------

# Displaying the schema of the table
spark.table("databox.juridico_comum.tb_fechamento_civel_202406").printSchema()

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
df_fech_civel_provisao66 = df_fech_civel_provisao.drop('DT_ULT_PGTO', 'ACORDOS', 'CONDENACAO', 'PENHORA', 
                                                     'OUTROS_PAGAMENTOS', 'IMPOSTO', 'GARANTIA', 'TOTAL_PAGAMENTOS') \
                                                     .sort(asc('ID_PROCESSO'), desc('MES_FECH')) \
                                                     .where("ENCERRADOS = 1") \
                                                     .dropDuplicates(['ID_PROCESSO'])
df_fech_civel_provisao66.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_66')

# COMMAND ----------

# CARRREGA OS VALORES DA BASE DE PAGAMENTOS
df_fech_civel_provisao77 = spark.sql("""
SELECT A.*
    ,B.DT_ULT_PGTO
    ,B.ACORDO AS ACORDOS
    ,B.CONDENACAO
    ,B.PENHORA
    ,B.OUTROS_PAGAMENTOS
    ,B.IMPOSTO
    ,B.GARANTIA
FROM TB_FECH_FIN_CIVEL_PROVISAO_66 AS A
LEFT JOIN databox.juridico_comum.hist_pagamentos_garantias_civel_f B ON A.ID_PROCESSO = B.PROCESSO_ID
"""
)
df_fech_civel_provisao77.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_77')

# COMMAND ----------

# CRIA O CAMPO COM A SOMA DE TODOS OS PAGAMENTOS
df_fech_civel_provisao88 = spark.sql("""
SELECT A.*,
    ACORDOS + CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA  AS TOTAL_PAGAMENTOS
FROM TB_FECH_FIN_CIVEL_PROVISAO_77 A
"""
)
df_fech_civel_provisao88.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_88')

# COMMAND ----------

df_fech_civel_provisao99 = merge_dfs(df_fech_civel_provisao, df_fech_civel_provisao88,['ID_PROCESSO', 'MES_FECH', 'LINHA'])
df_fech_civel_provisao99.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_99')

# COMMAND ----------

df_fech_civel_provisao_e = spark.sql("""
SELECT *,
CASE WHEN ACORDOS > 0 THEN 'ACORDO'
WHEN CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA > 0 THEN 'CONDENACAO'
ELSE 'SEM ONUS'
END MOTIVO_ENC_AGRP
FROM 
TB_FECH_FIN_CIVEL_PROVISAO_99 A
"""
)

df_fech_civel_provisao_e.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_E')

# COMMAND ----------

df_fech_civel_provisao_2 = spark.sql("""
SELECT *,
'N/A' AS DP_FASE
FROM TB_FECH_FIN_CIVEL_PROVISAO_E
""")
df_fech_civel_provisao_2.createOrReplaceTempView("TB_FECH_FIN_CIVEL_PROVISAO_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Provisão

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Preparação

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
df_saldo_tot_provisao_f = spark.createDataFrame([], schema_f)

# COMMAND ----------

df_saldo_tot_provisao_f = df_saldo_tot_provisao_f.withColumn("NATUREZA_OPERACIONAL_M", col("NATUREZA_OPERACIONAL_M").cast("String"))

# COMMAND ----------

df_saldo_tot_provisao_f.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"databox.juridico_comum.temp_provisao_civel_f")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Loop

# COMMAND ----------

for mes_fech in list_mes_fech:

  MESFECH = "'"+mes_fech+"'"
  
  # CRIA BASE COM OS VALORES NEGATIVOS DE PROVISÃO
  df_saldo_tot_prov = spark.sql(f"""
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
    MES_FECH > '2021-09-01'
  AND MES_FECH <= {MESFECH}
  AND PROVISAO_MOV_TOTAL_M < 0
  ORDER BY
    ID_PROCESSO, MES_FECH
  """
  )

  df_saldo_tot_prov.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO")
  # print(df_saldo_tot_prov.count())

  # Filtra a estratégia mais recente
  df_estrategia = spark.sql(f"""
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

  df_estrategia.createOrReplaceTempView("TB_ESTRATEGIA")
  # print(df_estrategia.count())


  # CARREGA OS DADOS DA ÚLTIMA ESTRATÉGIA
  df_saldo_tot_prov_1 = spark.sql(f"""
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
    TB_ESTRATEGIA B ON A.ID_PROCESSO = B.ID_PROCESSO
  """
  )

  df_saldo_tot_prov_1.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_1")
  # print(df_saldo_tot_prov_1.count())


  # CONSIDERA APENAS OS VALORES POSTERIORES A ESTRATÉGIA DEFESA
  df_saldo_tot_prov_2 = spark.sql(f"""
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

  df_saldo_tot_prov_2.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_2")
  # print(df_saldo_tot_prov_2.count())


  # SOMA OS VALORES DE PROVISÃO
  df_saldo_tot_prov_3 = spark.sql(f"""
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
  df_encerrados = spark.sql(f"""
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

  df_encerrados.createOrReplaceTempView("TB_ENCERRADOS")
  # print(df_encerrados.count())


  # BUSCA O VALOR DE PROVISÃO DO MÊS
  df_saldo_tot_prov_4 = spark.sql(f"""
  SELECT
    A.ID_PROCESSO::LONG,
    A.NATUREZA_OPERACIONAL_M,
    A.DP_FASE,
    A.MOTIVO_ENC_AGRP,
    A.TOTAL_PAGAMENTOS::FLOAT,
    A.MES_FECH::DATE,
    B.VALOR_DE_PROVISAO::FLOAT
  FROM
    TB_ENCERRADOS AS A
    LEFT JOIN TB_SALDO_TOT_PROVISAO_3 AS B ON A.ID_PROCESSO = B.ID_PROCESSO
  """
  )
  df_saldo_tot_prov_4.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_4")

df_saldo_tot_prov_4.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("databox.juridico_comum.temp_provisao_civel_f")
  
print(MESFECH + " concluida.")
  # print(df_saldo_tot_provisao_f.count())

# COMMAND ----------

display(df_saldo_tot_prov_4)

# COMMAND ----------

############
# Utilize esse SELECT caso já tenha rodado o Loop acima.
###########

# df_saldo_tot_provisao_f = spark.sql("SELECT * FROM databox.juridico_comum.temp_provisao_trab_f")

# COMMAND ----------

# Exportada como SHEET=PROVISAO_TRAB
df_saldo_tot_provisao_f.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_F")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Outras tabelas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 TB_PROVISAO_TOTAL

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
  TB_FECH_FIN_CIVEL_PROVISAO_2 A
WHERE
  ENCERRADOS = 1
"""
)

# COMMAND ----------

display(df_provisao_total)

# COMMAND ----------

df_provisao_total.count()

# COMMAND ----------

# Exportada como SHEET=TB_PROVISAO_TOTAL
df_saldo_tot_provisao_f.createOrReplaceTempView("TB_PROVISAO_TOTAL")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 TB_SALDO_TOT_PROVISAO_FF

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   A.ID_PROCESSO,
# MAGIC   A.NATUREZA_OPERACIONAL_M,
# MAGIC   A.DP_FASE,
# MAGIC   A.ESTRATEGIA,
# MAGIC   A.PROVISAO_MOV_M,
# MAGIC   A.PROVISAO_MOV_TOTAL_M,
# MAGIC   A.PROVISAO_TOTAL_M,
# MAGIC   A.PROVISAO_TOTAL_PASSIVO_M,
# MAGIC   A.TOTAL_PAGAMENTOS,
# MAGIC   A.MES_FECH
# MAGIC FROM
# MAGIC   TB_FECH_FIN_CIVEL_PROVISAO_2 A
# MAGIC WHERE
# MAGIC   ENCERRADOS = 1

# COMMAND ----------

df_fech_civel_provisao_3 = spark.sql("""
SELECT
    ID_PROCESSO,
    MES_FECH,
    AREA_DO_DIREITO,
    SUB_AREA_DO_DIREITO,
    ESTRATEGIA,
    ENCERRADOS
FROM TB_FECH_FIN_CIVEL_PROVISAO_2
WHERE ENCERRADOS = 1
"""
)
df_fech_civel_provisao_3.createOrReplaceTempView("TB_FECH_FIN_CIVEL_PROVISAO_3")

# COMMAND ----------

ddf_fech_civel_provisao_ff = spark.sql("""
    SELECT A.*,
           B.AREA_DO_DIREITO,
           B.SUB_AREA_DO_DIREITO,
           'N/A' AS `Fase (M)`,
           B.ESTRATEGIA,
           'N/A' AS `Indicação Processo Estratégico`,
           'N/A' AS `PARCELAMENTO CONDENAÇÃO`,
           'N/A' AS `PARCELAMENTO ACORDO`,
           B.ENCERRADOS
    FROM TB_SALDO_TOT_PROVISAO_F AS A
    LEFT JOIN TB_FECH_FIN_CIVEL_PROVISAO_3 AS B 
    ON A.ID_PROCESSO = B.ID_PROCESSO AND A.MES_FECH=B.MES_FECH
""")

# COMMAND ----------

display(ddf_fech_civel_provisao_ff)

# COMMAND ----------

display(df_fech_trab_provisao_ff)

# COMMAND ----------

# Exportada como SHEET=TB_ENCERR_X_PROVISAO_TRAB
df_fech_trab_provisao_ff.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_FF")

# COMMAND ----------

display(df_fech_trab_provisao_ff.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 TB_ENCERR_X_PROVISAO_TRAB

# COMMAND ----------

# TB_ENCERR_X_PROVISAO_TRAB_
df_encerr_x_provisao_trab = spark.sql("""
SELECT
  ID_PROCESSO,
  AREA_DO_DIREITO,
  SUB_AREA_DO_DIREITO,
  NATUREZA_OPERACIONAL_M,
  FASE_M,
  ESTRATEGIA,
  INDICACAO_PROCESSO_ESTRATEGICO,
  PARCELAMENTO_CONDENACAO,
  PARCELAMENTO_ACORDO,
--   PROVISAO_TOTAL_M,
--   PROVISAO_TOTAL_PASSIVO_M,
  TOTAL_PAGAMENTOS,
--   DT_ULT_PGTO,
  MOTIVO_ENC_AGRP,
  ENCERRADOS,
  MES_FECH
FROM
  TB_SALDO_TOT_PROVISAO_FF
"""
)



# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDAÇÃO

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from databox.juridico_comum.tmp_provisao_f

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MES_FECH,
# MAGIC   DP_FASE,
# MAGIC   count(ID_PROCESSO),
# MAGIC   sum(TOTAL_PAGAMENTOS)
# MAGIC FROM
# MAGIC   databox.juridico_comum.tmp_provisao_f
# MAGIC GROUP BY
# MAGIC   ALL
# MAGIC ORDER BY 1, 2

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
