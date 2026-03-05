# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento do Fechamento Financeiro
# MAGIC
# MAGIC **Inputs**\
# MAGIC Planilha Fechamento Financeiro (ff) fornecida pelo departamento financeiro
# MAGIC
# MAGIC **Outputs**\
# MAGIC Base Fechamento Finaceiro tratado tb_fechamento_civel

# COMMAND ----------

# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo financeiro. Ex: 22.02.2024
dbutils.widgets.text("nmtabela_finan", "")

# Parametro de data da tabela MESFECH. Ex: 01/04/2024
# Será utilizado como valor de uma coluna no join com o passo anterior
dbutils.widgets.text("mes_fechamento", "")

dbutils.widgets.text("nm_tabela", "")

nm_tabela = dbutils.widgets.get("nm_tabela")

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 - Carga e tratamentos iniciais do Fechamento Financeiro

# COMMAND ----------

# Caminho das pastas e arquivos
nmtabela_finan = dbutils.widgets.get("nmtabela_finan")

files_civ = dbutils.fs.ls("/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/civel_base_financeiro/")

for f in files_civ:
    if nmtabela_finan in f.name:
        if f.name.endswith(".xlsx"):
            path_ff = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/civel_base_financeiro/{nmtabela_finan} Fechamento Cível.xlsx'
        if f.name.endswith(".xlsm"):
            path_ff = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_fechamento_financeiro/civel_base_financeiro/{nmtabela_finan} Fechamento Cível.xlsm'

path_ff


# COMMAND ----------

validador = False
x = 0

while validador  == False:
    x += 1
    # Carrega a planilha em Spark Data Frame
    df_ff = read_excel(path_ff, f"'Base'!A{x}")
    if 'LINHA' in df_ff.columns:
        validador = True
    else:
        pass

# Remove linhas sem dados
df_ff = df_ff.where("`ID PROCESSO` IS NOT NULL")

# COMMAND ----------

df_ff.count() #29999

# COMMAND ----------

df_ff.printSchema()

# COMMAND ----------

display(df_ff)

# COMMAND ----------

df_ff  = df_ff .withColumnRenamed("ESTRATÉGIA68", "ESTRATÉGIA").withColumnRenamed("ESTRATÉGIA84", "ESTRATÉGIA_M-1")

# COMMAND ----------

# Ajusta os nomes das colunas. Pontos "." em especial geram muitos erros
df_ff = remove_acentos(df_ff)
df_ff = adjust_column_names(df_ff)

# COMMAND ----------

# Converte as colunas listadas para o tipo data
colunas_data = ['DATACADASTRO', 'REABERTURA', 'DISTRIBUICAO']

df_ff = convert_to_date_format(df_ff, colunas_data)
# display(df_ff)

# COMMAND ----------

colunas_numeros = ['Centro_de_Custo_M_1', 'Centro_de_Custo_M']

df_ff = convert_to_float(df_ff, colunas_numeros)
# display(df_ff)

# COMMAND ----------

# Add o mes_fechamento
mes_fechamento = dbutils.widgets.get("mes_fechamento")
df_fechamento_civel = df_ff.withColumn("MES_FECH", to_date(lit(mes_fechamento), 'dd/MM/yyyy'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 - Carga e join dos Pagamentos e Garantias

# COMMAND ----------

df_pgto_garantias = spark.sql("select * from databox.juridico_comum.hist_pagamentos_garantias_civel_f")

# COMMAND ----------

df_fechamento_civel.createOrReplaceTempView("FECHAMENTO_CIVEL_4")

# COMMAND ----------


df_fechamento_civel_5 = spark.sql(f"""
/* CARREGA OS PAGAMENTOS DE ACORDOS, CONDENAÇÕES E GARANTIAS NA BASE */
SELECT A.ID_PROCESSO
		,A.MES_FECH
		,B.ACORDO
		,B.CONDENACAO
		,B.PENHORA
		,B.GARANTIA
		,B.IMPOSTO
		,B.OUTROS_PAGAMENTOS
		,A.ENCERRADOS
		,B.TOTAL_PAGAMENTOS
		,B.DATA_EFETIVA_DO_PAGAMENTO

	FROM FECHAMENTO_CIVEL_4 AS A
	LEFT JOIN global_temp.HIST_PAGAMENTOS_GARANTIAS_CIVEL_F AS B ON A.ID_PROCESSO = B.PROCESSO_ID
	WHERE A.ID_PROCESSO IS NOT NULL AND A.ENCERRADOS = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 - Adição do Mes Contábil

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/processos cível/book indicadores cível/mes_contabil"

# COMMAND ----------

df_fechamento_civel_5.createOrReplaceTempView("FECHAMENTO_CIVEL_51")
df_mes_contabil.createOrReplaceTempView("TB_MES_CONTABIL_CIVEL")

df_fechamento_civel_5 = spark.sql("""
    SELECT
    A.ID_PROCESSO,
    A.MES_FECH,
    A.ACORDO,
    A.CONDENACAO,
    A.PENHORA,
    A.GARANTIA,
    A.IMPOSTO,
    A.OUTROS_PAGAMENTOS,
    A.ENCERRADOS,
    A.TOTAL_PAGAMENTOS,
    A.DATA_EFETIVA_DO_PAGAMENTO,
    B.mes_contabil AS MES_CONTABIL
FROM
    FECHAMENTO_CIVEL_51 AS A
LEFT JOIN
    TB_MES_CONTABIL_CIVEL AS B
ON
    -- A.DATA_EFETIVA_DO_PAGAMENTO BETWEEN B.dt_contabil_inicio AND B.dt_contabil_fim
    A.DATA_EFETIVA_DO_PAGAMENTO > B.dt_contabil_inicio AND A.DATA_EFETIVA_DO_PAGAMENTO <= B.dt_contabil_fim
""")

# COMMAND ----------

df_fechamento_civel_5.createOrReplaceTempView("FECHAMENTO_CIVEL_5")

df_fechamento_civel_6 = spark.sql(f"""
	SELECT ID_PROCESSO
			,MES_FECH
			,ENCERRADOS
			,MAX(MES_CONTABIL) AS DT_ULT_PGTO
			,SUM(ACORDO) AS ACORDOS
			,SUM(CONDENACAO) AS `CONDENAÇÃO`
			,SUM(PENHORA) AS PENHORA
			,SUM(GARANTIA) AS GARANTIA
			,SUM(IMPOSTO) AS IMPOSTO
			,SUM(OUTROS_PAGAMENTOS) AS OUTROS_PAGAMENTOS
			,SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS

	FROM FECHAMENTO_CIVEL_5
	WHERE MES_CONTABIL <= MES_FECH
	GROUP BY 1, 2, 3
	ORDER BY ID_PROCESSO
 """)

# COMMAND ----------

df_fechamento_civel_6.createOrReplaceTempView("FECHAMENTO_CIVEL_6")

df_fechamento_civel_final = spark.sql(f"""
SELECT A.*
      ,B.DT_ULT_PGTO
      ,B.ACORDOS
      ,B.`CONDENAÇÃO`
      ,B.PENHORA
      ,B.GARANTIA
      ,B.IMPOSTO
      ,B.OUTROS_PAGAMENTOS
      ,B.TOTAL_PAGAMENTOS
FROM FECHAMENTO_CIVEL_4 A
LEFT JOIN FECHAMENTO_CIVEL_6 B
ON A.ID_PROCESSO = B.ID_PROCESSO
   AND A.MES_FECH = B.MES_FECH
   AND A.ENCERRADOS = B.ENCERRADOS
ORDER BY ID_PROCESSO
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 - Tratamentos finais

# COMMAND ----------

df_fechamento_civel_final = df_fechamento_civel_final.drop("DATACADASTRO", "STATUS", "FASE", "FASE_NOVO", "FASE_M", "STATUS_M")

# COMMAND ----------

df_fechamento_civel_final = df_fechamento_civel_final.withColumnRenamed('CADASTRO_NOVO', 'DATACADASTRO') \
                                                   .withColumnRenamed('STATUS_NOVO', 'STATUS_M') \
                                                   .withColumnRenamed('FASE_FINAL', 'FASE_M')

# COMMAND ----------

df_fechamento_civel_final = df_fechamento_civel_final.drop("correcao_m_1_new_renamed")

# COMMAND ----------

# Drop the column "correcao_m1_new" if it exists
df_fechamento_civel_final= df_fechamento_civel_final.drop("estrategia_m_1")


# COMMAND ----------

from pyspark.sql.functions import col

# Lista para armazenar os novos nomes de colunas
new_columns = []

# Dicionário para contar as ocorrências dos nomes de colunas
col_counts = {}

# Iterar sobre as colunas do DataFrame
for c in df_fechamento_civel_final.columns:
    if c in col_counts:
        # Incrementar o contador para colunas duplicadas
        col_counts[c] += 1
        # Adicionar sufixo com o contador ao nome da coluna duplicada
        new_name = f"{c}_{col_counts[c]}"
    else:
        # Inicializar o contador para o nome da coluna
        col_counts[c] = 1
        new_name = c
    new_columns.append(new_name)

# Renomear as colunas do DataFrame
df_fechamento_civel_final = df_fechamento_civel_final.toDF(*new_columns)

# COMMAND ----------

dbutils.widgets.text("nm_tabela", "")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Salva o arquivo final do Fechamento Consolidado

# COMMAND ----------

# %sql
# DROP TABLE databox.juridico_comum.tb_fechamento_civel_202405

# COMMAND ----------


df_fechamento_civel_final.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"databox.juridico_comum.tb_fechamento_civel_{nm_tabela}")

# COMMAND ----------

df = spark.table(f"databox.juridico_comum.tb_fechamento_civel_{nm_tabela}")
display(df)

# COMMAND ----------

df.groupBy("ESTRATEGIA").agg(countDistinct("ID_PROCESSO").alias("COUNT_ID_PROCESSO")).show()

# COMMAND ----------

df_fechamento_civel_final.printSchema()

# COMMAND ----------

df.createOrReplaceTempView('TB_FECH_FIN_CIVEL_F')

# COMMAND ----------

civel_total = spark.sql("""
		SELECT  MES_FECH
	 			,SUM(NOVOS) AS NOVOS
               	,SUM(ENCERRADOS) AS ENCERRADOS 
				,SUM(ESTOQUE) AS ESTOQUE 
				,SUM(ACORDOS) AS ACORDO 
				,SUM('CONDENAÇÃO') AS CONDENACAO 
				,SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS 
				,SUM(PROVISAO_TOTAL_PASSIVO_M) AS PROVISAO_TOTAL_PASSIVO_M 
				,SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M 
				
			FROM TB_FECH_FIN_CIVEL_F
			GROUP BY MES_FECH
	ORDER BY 1
 """)

display(civel_total)
