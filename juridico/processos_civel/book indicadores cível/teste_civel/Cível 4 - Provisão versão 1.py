# Databricks notebook source
from pyspark.sql.functions import col, sum as _sum, when, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ####Junção Tabelas Fechamento Financeiro

# COMMAND ----------

tb_fech_civel_provisao = spark.table("databox.juridico_comum.tb_fech_civel_provisao")

# COMMAND ----------

tb_fech_civel_provisao.createOrReplaceTempView("TB_FECH_CIVEL_PROVISAO")

# COMMAND ----------

# Trata coluna ESTRATEGIA
tb_fech_civel_provisao_1 = tb_fech_civel_provisao.withColumn(
    "ESTRATEGIA",
    when(col("ESTRATEGIA").isin("ACO", "ACOR", "ACORD", "ACORDO"), "ACORDO")
    .when(col("ESTRATEGIA").isin("DEF", "DEFE", "DEFES", "DEFESA"), "DEFESA")
    .otherwise(col("ESTRATEGIA"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Base com o último encerramento do processo

# COMMAND ----------

# Criação da base com o último encerramento do processo
tb_fech_civel_provisao_2 = tb_fech_civel_provisao_1.drop('DT_ULT_PGTO', 'ACORDOS', 'CONDENACAO', 'PENHORA', 
                                                     'OUTROS_PAGAMENTOS', 'IMPOSTO', 'GARANTIA', 'TOTAL_PAGAMENTOS') \
                                                     .sort(asc('ID_PROCESSO'), desc('MES_FECH')) \
                                                     .where("ENCERRADOS = 1") \
                                                     .dropDuplicates(['ID_PROCESSO'])
tb_fech_civel_provisao_2.createOrReplaceTempView('TB_FECH_FIN_CIVEL_PROVISAO_66')

# COMMAND ----------

# Renomeando a coluna 'nome_antigo' para 'nome_novo' no DataFrame df
tb_fech_civel_provisao_a = tb_fech_civel_provisao_a.withColumnRenamed("ACORDOS", "ACORDOS_CATEG")
tb_fech_civel_provisao_a = tb_fech_civel_provisao_a.withColumnRenamed("GARANTIA", "GARANTIA_CATEG")
tb_fech_civel_provisao_a = tb_fech_civel_provisao_a.withColumnRenamed("PENHORA", "PENHORA_CATEG")
tb_fech_civel_provisao_a = tb_fech_civel_provisao_a.withColumnRenamed("IMPOSTO", "IMPOSTO_CATEG")
tb_fech_civel_provisao_a = tb_fech_civel_provisao_a.withColumnRenamed("CONDENACAO", "CONDENACAO_CATEG")
tb_fech_civel_provisao_a = tb_fech_civel_provisao_a.withColumnRenamed("OUTROS_PAGAMENTOS", "OUTROS_PAGAMENTOS_CATEG")
tb_fech_civel_provisao_a = tb_fech_civel_provisao_a.withColumnRenamed("TOTAL_PAGAMENTOS", "TOTAL_PAGAMENTOS_CATEG")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Junção com a Base de Pagamentos e Garantias

# COMMAND ----------

hist_pagto_civel_consolidado= spark.read.table("databox.juridico_comum.hist_pagamentos_garantias_civel_f")


# COMMAND ----------

#tabelas para junção modo SQL SPARK
tb_fech_civel_provisao_a.createOrReplaceTempView("TB_FECH_FIN_CIVEL_PROVISAO_A")
hist_pagto_civel_consolidado.createOrReplaceTempView("HIST_PGTO_CIVEL")

# COMMAND ----------

tb_fech_civel_provisao_b = spark.sql(f"""
/* CARREGA OS PAGAMENTOS DE ACORDOS, CONDENAÇÕES E GARANTIAS NA BASE */
SELECT A.ID_PROCESSO
		,A.MES_FECH
		,B.ACORDO
		,B.CONDENACAO
		,B.PENHORA
		,B.GARANTIAS
		,B.IMPOSTO
		,B.OUTROS_PAGAMENTOS
		,A.ENCERRADOS
		,B.OUTROS_PAGAMENTOS
		,B.DATA_EFETIVA_PAGAMENTO

	FROM TB_FECH_FIN_CIVEL_PROVISAO_A AS A
	LEFT JOIN HIST_PGTO_CIVEL AS B ON A.ID_PROCESSO = B.PROCESSO_ID
	WHERE A.ID_PROCESSO IS NOT NULL AND A.ENCERRADOS = 1
""")

# COMMAND ----------

# MAGIC
# MAGIC %run "./mes_contabil"

# COMMAND ----------

tb_fech_civel_provisao_b.createOrReplaceTempView("TB_FECH_CIVEL_PROVISAO_BB")
df_mes_contabil.createOrReplaceTempView("TB_MES_CONTABIL_CIVEL_PROVISAO")

# COMMAND ----------

tb_fech_civel_provisao_b

# COMMAND ----------

tb_fech_civel_provisao_cc = spark.sql("""
    SELECT
    A.ID_PROCESSO,
    A.MES_FECH,
    A.ACORDO,
    A.CONDENACAO,
    A.PENHORA,
    A.GARANTIAS,
    A.IMPOSTO,
    A.OUTROS_PAGAMENTOS,
    A.ENCERRADOS,
    A.DATA_EFETIVA_PAGAMENTO,
    B.mes_contabil AS MES_CONTABIL
FROM
    TB_FECH_CIVEL_PROVISAO_BB AS A
LEFT JOIN
    TB_MES_CONTABIL_CIVEL_PROVISAO AS B
ON
    A.DATA_EFETIVA_PAGAMENTO BETWEEN B.dt_contabil_inicio AND B.dt_contabil_fim
""")

# COMMAND ----------

tb_fech_civel_provisao_cc.createOrReplaceTempView("TB_FECH_FIN_CIVEL_PROVISAO_C")

tb_fech_civel_provisao_c = spark.sql(f"""
	SELECT ID_PROCESSO
			,MES_FECH
			,SUM(ACORDO) AS ACORDOS
			,SUM(CONDENACAO) AS `CONDENAÇÃO`
			,SUM(PENHORA) AS PENHORA
			,SUM(GARANTIAS) AS GARANTIA
			,SUM(IMPOSTO) AS IMPOSTO
			,SUM(OUTROS_PAGAMENTOS) AS OUTROS_PAGAMENTOS
			

	FROM TB_FECH_FIN_CIVEL_PROVISAO_C
	
	GROUP BY 1, 2
	ORDER BY ID_PROCESSO
 """)

# COMMAND ----------

# Ordena TB_FECH_FIN_CIVEL_PROVISAO por ID_PROCESSO e MES_FECH
tb_fech_civel_provisao = tb_fech_civel_provisao.orderBy("ID_PROCESSO", "MES_FECH")

# Ordena TB_FECH_FIN_CIVEL_PROVISAO_C por ID_PROCESSO e MES_FECH
tb_fech_civel_provisao_c = tb_fech_civel_provisao_c.orderBy("ID_PROCESSO", "MES_FECH")

# COMMAND ----------

tb_fech_civel_provisao_a.createOrReplaceTempView("TB_FECH_FIN_CIVEL_PROVISAO_A")
tb_fech_civel_provisao_c.createOrReplaceTempView("TB_FECH_FIN_CIVEL_PROVISAO_C")

# COMMAND ----------


tb_fech_civel_provisao_d = spark.sql(f"""
SELECT A.*
      
      ,B.ACORDOS
      ,B.`CONDENAÇÃO`
      ,B.PENHORA
      ,B.GARANTIA
      ,B.IMPOSTO
      ,B.OUTROS_PAGAMENTOS
    
FROM  TB_FECH_FIN_CIVEL_PROVISAO_A A
LEFT JOIN TB_FECH_FIN_CIVEL_PROVISAO_C B
ON A.ID_PROCESSO = B.ID_PROCESSO
   
ORDER BY ID_PROCESSO
""")

# COMMAND ----------

from pyspark.sql import functions as F

# Assuming tb_fech_fin_civel_provisao_d is the DataFrame you want to transform
# First, we create the TOTAL_PAGAMENTOS column by summing the specified columns
tb_fech_fin_civel_provisao_e = tb_fech_civel_provisao_d.withColumn(
    "TOTAL_PAGAMENTOS",
    F.col("ACORDOS") + F.col("CONDENAÇÃO") + F.col("PENHORA") + 
    F.col("OUTROS_PAGAMENTOS") + F.col("IMPOSTO") + F.col("GARANTIA")
)

# Then, we create the MOTIVO_ENC_AGRP column based on the provided conditions
tb_fech_fin_civel_provisao_e = tb_fech_fin_civel_provisao_e.withColumn(
    "MOTIVO_ENC_AGRP",
    F.when(
        (F.col("ENCERRADOS") == 1) & (F.col("ACORDOS") > 1), "ACORDO"
    ).when(
        (F.col("ENCERRADOS") == 1) & 
        ((F.col("CONDENAÇÃO") + F.col("PENHORA") + F.col("OUTROS_PAGAMENTOS") + F.col("IMPOSTO") + F.col("GARANTIA")) > 1), "CONDENACAO"
    ).when(
        (F.col("ENCERRADOS") == 1) & 
        ((F.col("ACORDOS") + F.col("CONDENAÇÃO") + F.col("PENHORA") + F.col("OUTROS_PAGAMENTOS") + F.col("IMPOSTO") + F.col("GARANTIA")) <= 1), "SEM ONUS"
    ).otherwise(None)
)

# Removing the DP_FASE column as requested
tb_fech_fin_civel_provisao_e = tb_fech_fin_civel_provisao_e.drop("DP_FASE")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Base Histórica de Provisão Cível

# COMMAND ----------

# dates = df_arquivos.select('mes_fech').where("mes_fech >= '2021-07-01'").collect()

df_dates = df_arquivos.select('mes_fech').where("mes_fech >= '2021-10-01'")
dates = [row['mes_fech'] for row in df_dates.collect()]
list_mes_fech = [d.strftime("%Y-%m-%d") for d in dates]

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

for mes_fech in list_mes_fech:
    MESFECH = "'"+mes_fech+"'"
    print(MESFECH)
    break

# COMMAND ----------

MESFECH = "'"+list_mes_fech[-1]+"'"

print(MESFECH)

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
MES_FECH > '2021-07-01'
AND MES_FECH <= {MESFECH}
AND PROVISAO_MOV_TOTAL_M < 0
ORDER BY
ID_PROCESSO, MES_FECH
"""
)

df_saldo_tot_prov.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Loop

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
    MES_FECH > '2021-07-01'
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
    A.ID_PROCESSO,
    A.NATUREZA_OPERACIONAL_M,
    A.DP_FASE,
    A.MOTIVO_ENC_AGRP,
    A.TOTAL_PAGAMENTOS,
    A.MES_FECH::DATE,
    B.VALOR_DE_PROVISAO
  FROM
    TB_ENCERRADOS AS A
    LEFT JOIN TB_SALDO_TOT_PROVISAO_3 AS B ON A.ID_PROCESSO = B.ID_PROCESSO
  """
  )

  df_saldo_tot_prov_4.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_4")

  df_saldo_tot_provisao_f = df_saldo_tot_provisao_f.unionAll(df_saldo_tot_prov_4)
  # print(df_saldo_tot_provisao_f.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cria a tabela com processos encerrados

# COMMAND ----------

# MAGIC %md
# MAGIC ####Exporta a base com valores de provisão histórica

# COMMAND ----------



# Registrar os DataFrames como tabelas temporárias
TB_SALDO_TOT_PROVISAO_F.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_F")
TB_FECH_FIN_CIVEL_PROVISAO_A.createOrReplaceTempView("TB_FECH_FIN_CIVEL_PROVISAO_A")

# Criar o DataFrame TB_SALDO_TOT_PROVISAO_FF com os demais campos na base de provisão
TB_SALDO_TOT_PROVISAO_FF = spark.sql("""
    SELECT 
        A.`ID PROCESSO`,
        A.`Natureza Operacional (M)`,
        A.MOTIVO_ENC_AGRP,
        A.MES_FECH,
        B.`Área do Direito`,
        B.`Sub-área do Direito`,
        'N/A' AS `FASE (M)`,
        B.ESTRATEGIA AS ESTRATEGIA,
        'N/A' AS `Indicação Processo Estratégico`,
        'N/A' AS `PARCELAMENTO CONDENAÇÃO`,
        'N/A' AS `PARCELAMENTO ACORDO`,
        A.`Valor de Provisão` AS `Provisão Total (M)`,
        A.`Valor de Provisão` AS `Provisão Total Passivo (M)`,
        A.`Total de Pagamento` AS TOTAL_PAGAMENTOS,
        B.DT_ULT_PGTO,
        B.ENCERRADOS
    FROM TB_SALDO_TOT_PROVISAO_F AS A
    LEFT JOIN TB_FECH_FIN_CIVEL_PROVISAO_A AS B 
    ON A.`ID PROCESSO` = B.`ID PROCESSO`
""")

# Registrar o DataFrame TB_SALDO_TOT_PROVISAO_FF como uma tabela temporária
TB_SALDO_TOT_PROVISAO_FF.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_FF")

# Criar o DataFrame TB_ENCERR_X_PROVISAO_CIV_&NMMES com os campos classificados na ordem correta
TB_ENCERR_X_PROVISAO_CIV_NMMES = spark.sql("""
    SELECT 
        `ID PROCESSO`,
        `Área do Direito`,
        `Sub-área do Direito`,
        `Natureza Operacional (M)`,
        `FASE (M)`,
        ESTRATEGIA,
        `Indicação Processo Estratégico`,
        `PARCELAMENTO CONDENAÇÃO`,
        `PARCELAMENTO ACORDO`,
        `Provisão Total (M)`,
        `Provisão Total Passivo (M)`,
        TOTAL_PAGAMENTOS,
        DT_ULT_PGTO,
        MOTIVO_ENC_AGRP,
        ENCERRADOS,
        MES_FECH
    FROM TB_SALDO_TOT_PROVISAO_FF
""")

# Exibir os resultados (opcional)
TB_SALDO_TOT_PROVISAO_FF.show()
TB_ENCERR_X_PROVISAO_CIV_NMMES.show()




# COMMAND ----------

# MAGIC %md
# MAGIC #####Exporta a base final da provisão

# COMMAND ----------


