# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento dos dados de Pagamentos e Garantias
# MAGIC
# MAGIC **Inputs**\
# MAGIC Planilhas fornecidas pelo ELAW e tratadas pela área finaceira. Pagamentos e Garantias.
# MAGIC
# MAGIC **Outputs**\
# MAGIC Uma Global View com os dados de Pagamentos e Garantias tratados e unidos para utilização pelo passo 3.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1- Importação e tratamento inicial bases de Pagamentos e Garantias

# COMMAND ----------

from pyspark.sql.functions import coalesce, row_number, lit

# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo. Ex: 20240423
dbutils.widgets.text("nmtabela_pgto", "")
dbutils.widgets.text("nmtabela_garantias", "")

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Caminho das pastas e arquivos
nmtabela_pgto = dbutils.widgets.get("nmtabela_pgto")
nmtabela_garantias = dbutils.widgets.get("nmtabela_garantias")

path_pagamentos = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{nmtabela_pgto}.xlsx'
path_garantias = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_{nmtabela_garantias}.xlsx'

# COMMAND ----------

# Carrega as planilhas em Spark Data Frames
df_pgto_civel = read_excel(path_pagamentos, "A6",)
df_garantias_civel = read_excel(path_garantias, "A6")

# COMMAND ----------

# Lista as colunas com datas
pgto_data_cols = find_columns_with_word(df_pgto_civel, 'DATA ')
garantias_data_cols = find_columns_with_word(df_garantias_civel, 'DATA ')

print("pgto_data_cols")
print(pgto_data_cols)
print("\n")
print("garantias_data_cols")
print(garantias_data_cols)

# COMMAND ----------

# Converte as datas das colunas listadas
df_pgto_civel = convert_to_date_format(df_pgto_civel, pgto_data_cols)
df_garantias_civel = convert_to_date_format(df_garantias_civel, garantias_data_cols)

# COMMAND ----------

df_pgto_civel = df_pgto_civel.withColumnRenamed("PROCESSO - ID", "PROCESSO_ID")


# COMMAND ----------


# Or, if 'databox.juridico_comum.tb_responsabilidade_civel' is a Delta table registered in the metastore
tb_responsabilidade_final = spark.sql("SELECT * FROM databox.juridico_comum.TB_RESPONSABILIDADE_CIVEL")


# COMMAND ----------

from pyspark.sql.functions import col

# Realiza o left join entre as duas tabelas e adiciona a coluna 'CÍVEL_MASSA_-_RESPONSABILIDADE'
historico_de_pagamentos_civel_1 = df_pgto_civel.join(
    tb_responsabilidade_final.select('PROCESSO_ID', 'CÍVEL_MASSA_RESPONSABILIDADE'),
    on='PROCESSO_ID',
    how='left'
)

# COMMAND ----------

# df_garantias_civel.count() #171646

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Tratamento base de Pagamentos

# COMMAND ----------

historico_de_pagamentos_civel_1.createOrReplaceTempView("HISTORICO_DE_PAGAMENTOS_CIVEL_1")

# COMMAND ----------

df_pgto_civel = spark.sql("""
SELECT 
    HISTORICO_DE_PAGAMENTOS_CIVEL_1.*, 
    CASE 
        WHEN `SUB TIPO` IN ('ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA', 'ACORDO - TRABALHISTA', 'PAGAMENTO DE ACORDO - TRABALHISTA (FK)', 'RNO - ACORDO - TRABALHISTA', 'ACORDO - CÍVEL MASSA - CARTÕES', 'ACORDO - CÍVEL MASSA - FORNECEDOR', 'ACORDO - CÍVEL MASSA - MKTPLACE', 'ACORDO - CÍVEL MASSA - SEGURO') 
        THEN VALOR 
    END AS ACORDO,
    CASE 
        WHEN `SUB TIPO` IN ('CONDENAÇÃO - CÍVEL', 'CONDENAÇÃO - CÍVEL ESTRATÉGICO', 'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL', 'MULTA PROCESSUAL - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'RNO - MULTA PROCESSUAL - TRABALHISTA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO', 'CONDENAÇÃO - CÍVEL MASSA - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA - SEGURO') 
        THEN VALOR 
    END AS CONDENACAO,
    CASE 
        WHEN `SUB TIPO` IN ('HONORÁRIOS CONCILIADOR - CIVEL MASSA', 'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA', 'HONORÁRIOS PERICIAIS - IMOBILIÁRIO', 'HONORÁRIOS PERICIAIS - TRABALHISTA', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA', 'CUSTAS FK - TRABALHISTA', 'CUSTAS PERITOS - CÍVEL', 'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL', 'CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO', 'CUSTAS PROCESSUAIS - TRABALHISTA', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO', 'PERITOS - REGULATÓRIO', 'RNO - CUSTAS PROCESSUAIS - TRABALHISTA', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE') 
        THEN VALOR 
    END AS CUSTAS,
    CASE 
        WHEN `SUB TIPO` IN ('INSS - TRABALHISTA', 'IR - TRABALHISTA', 'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)', 'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA', 'RNO - FGTS', 'FGTS') 
        THEN VALOR 
    END AS IMPOSTO,
    CASE 
        WHEN `SUB TIPO` IN ('LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA') 
        THEN VALOR 
    END AS PENHORA,
    CASE 
        WHEN `SUB TIPO` IN ('PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO', 'PENSÃO - CÍVEL MASSA') 
        THEN VALOR 
    END AS PENSAO,
    CASE 
        WHEN `SUB TIPO` IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)', 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO', 'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO', 'LIMINAR (CÍV. MASSA)', 'PAGAMENTO', 'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA', 'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK') 
        THEN VALOR 
    END AS OUTROS_PAGAMENTOS
FROM HISTORICO_DE_PAGAMENTOS_CIVEL_1
WHERE `SUB TIPO` IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)','CONDENAÇÃO - CÍVEL ESTRATÉGICO'
, 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO', 'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL', 'ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA', 'ACORDO - TRABALHISTA', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO', 'ATIVO', 'CONDENAÇÃO - CÍVEL',  'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CUSTAS FK - TRABALHISTA', 'CUSTAS PERITOS - CÍVEL', 'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL', 'CUSTAS PROCESSUAIS -CÍVEL ESTRATÉGICO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO', 'CUSTAS PROCESSUAIS - TRABALHISTA', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA', 'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA', 'HONORÁRIOS PERICIAIS - IMOBILIÁRIO', 'HONORÁRIOS PERICIAIS - TRABALHISTA', 'LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA', 'LIMINAR (CÍV. MASSA)', 'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL', 'MULTA PROCESSUAL - IMOBILIÁRIO', 'MULTA PROCESSUAL - TRABALHISTA', 'PAGAMENTO', 'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA', 'PAGAMENTO DE ACORDO - TRABALHISTA (FK)', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO', 'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)', 'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK', 'PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO', 'PENSÃO - CÍVEL MASSA', 'PERITOS - REGULATÓRIO', 'RNO - ACORDO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'RNO - CUSTAS PROCESSUAIS - TRABALHISTA', 'RNO - FGTS', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA', 'RNO - MULTA PROCESSUAL - TRABALHISTA', 'INSS - TRABALHISTA', 'IR - TRABALHISTA', 'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)', 'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO', 'CONDENAÇÃO - CÍVEL MASSA - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA - SEGURO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE')
AND UPPER(`STATUS DO PAGAMENTO`) NOT IN ('REMOVIDO', 'CANCELADO', 'REJEITADO', 'EM CORREÇÃO', 'PENDENTE')
AND UPPER(`STATUS DO PAGAMENTO`) NOT LIKE ('%PAGAMENTO DEVOLVIDO%')
""")

# COMMAND ----------

# Correção da DATA EFETIVA PAGAMENTO e Preenche com a data existente
df_pgto_civel = df_pgto_civel.withColumn('DATA EFETIVA DO PAGAMENTO', coalesce('DATA EFETIVA DO PAGAMENTO', 'DATA SOLICITAÇÃO (ESCRITÓRIO)'))


# COMMAND ----------

df_pgto_civel = adjust_column_names(df_pgto_civel)

# COMMAND ----------

df_pgto_civel.createOrReplaceTempView("HISTORICO_DE_PAGAMENTOS_JUD_CIVEL_2")
# df_pgto.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.stg_historico_pgto")

# COMMAND ----------

df_pgto_civel_1 = spark.sql("""
/* AGRUPA TABELA DE PAGAMENTOS DE ACORDOS E CONDENAÇÕES */
WITH CTE AS (
    SELECT PROCESSO_ID
           ,DATA_EFETIVA_DO_PAGAMENTO
           ,COALESCE(SUM(ACORDO), 0) AS ACORDO
           ,COALESCE(SUM(CONDENACAO), 0) AS CONDENACAO
           ,COALESCE(SUM(PENHORA), 0) AS PENHORA
           ,COALESCE(SUM(PENSAO), 0) AS PENSAO
           ,COALESCE(SUM(OUTROS_PAGAMENTOS), 0) AS OUTROS_PAGAMENTOS
           ,COALESCE(SUM(IMPOSTO), 0) AS IMPOSTO
            ,COALESCE(SUM(CUSTAS), 0) AS CUSTAS
    FROM HISTORICO_DE_PAGAMENTOS_JUD_CIVEL_2
    GROUP BY 1, 2)
    SELECT CTE.*
    FROM CTE
    WHERE (ACORDO + CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + PENSAO + CUSTAS) > 0 -- + PENSAO + CUSTAS
"""
)

# COMMAND ----------

# display(df_pgto.sort("PROCESSO_ID", ascending=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3- Tratamento base de Garantias
# MAGIC

# COMMAND ----------

# Ajusta os nomes das colunas com '_' no lugar de ' '
df_garantias_civel = adjust_column_names(df_garantias_civel)

# COMMAND ----------

df_garantias_civel.createOrReplaceTempView("TB_GARANTIAS_CIVEL_F")

# COMMAND ----------

df_garantias_civel.createOrReplaceTempView("TB_GARANTIAS_MES_CIVEL_A")

# COMMAND ----------

df_garantias_civel_1 = spark.sql("""
SELECT PROCESSO_ID
        ,`DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA`
		,`VALOR_LEVANTADO_PARTE_CONTRÁRIA`
FROM TB_GARANTIAS_MES_CIVEL_A
WHERE STATUS_DA_GARANTIA <> 'CANCELADO'
		AND STATUS_DA_GARANTIA NOT LIKE ('%PAGAMENTO DEVOLVIDO%')
		AND STATUS <> 'REMOVIDO'
		AND TIPO_GARANTIA IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL',
						'DEPÓSITO',
						'DEPÓSITO GARANTIA DE JUIZO - CÍVEL',
						'DEPÓSITO JUDICIAL',
						'DEPÓSITO JUDICIAL - CÍVEL',
						'DEPÓSITO JUDICIAL - CÍVEL ESTRATÉGICO',
						'DEPÓSITO JUDICIAL - CÍVEL MASSA',
						'DEPÓSITO JUDICIAL - REGULATÓRIO',
						'DEPÓSITO JUDICAL REGULATÓRIO',
						'DEPÓSITO JUDICIAL - TRABALHISTA',
						'DEPOSITO JUDICIAL TRABALHISTA',
						'DEPÓSITO JUDICIAL TRABALHISTA - BLOQUEIO',
						'DEPÓSITO JUDICIAL TRABALHISTA BLOQUEIO - TRABALHISTA',
						'DEPÓSITO JUDICIAL TRIBUTÁRIO',
						'DEPÓSITO RECURSAL - AIRO - TRABALHISTA',
						'DEPÓSITO RECURSAL - AIRR',
						'DEPÓSITO RECURSAL - AIRR - TRABALHISTA',
						'DEPÓSITO RECURSAL - CÍVEL MASSA',
						'DEPÓSITO RECURSAL - EMBARGOS TST',
						'DEPÓSITO RECURSAL - RO - TRABALHISTA',
						'DEPÓSITO RECURSAL - RR - TRABALHISTA',
						'DEPÓSITO RECURSAL AIRR',
						'DEPÓSITO RECURSAL RO',
						'PENHORA - GARANTIA',
						'PENHORA - REGULATÓRIO' );
""")
# df_garantias.count() #75479

# COMMAND ----------

df_garantias_civel_1.createOrReplaceTempView("TB_GARANTIAS_MES_CIVEL_A")

# COMMAND ----------

df_garantias_civel_2 = spark.sql("""
	SELECT PROCESSO_ID
        ,`DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA` AS DATA_EFETIVA_DO_PAGAMENTO
        ,SUM(`VALOR_LEVANTADO_PARTE_CONTRÁRIA`) AS GARANTIA
	FROM TB_GARANTIAS_CIVEL_F
  WHERE `DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA` IS NOT NULL
	GROUP BY 1, 2
	ORDER BY 1, 2
 """)

# COMMAND ----------

df_garantias_civel_2.createOrReplaceTempView("TB_GARANTIAS_CIVEL_FF")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 - Junta as bases de Pagamentos e Garantias

# COMMAND ----------

df_pgto_civel_1.createOrReplaceTempView("HISTORICO_DE_PAGAMENTOS_JUD_CIVEL_4")
df_garantias_civel_2.createOrReplaceTempView("TB_GARANTIAS_CIVEL_F")

# COMMAND ----------

# Faz o Join das duas tabelas
df_pgto_garantias_civel = spark.sql("""
SELECT 
COALESCE(A.PROCESSO_ID, B.PROCESSO_ID) AS PROCESSO_ID
,COALESCE(A.DATA_EFETIVA_DO_PAGAMENTO, B.DATA_EFETIVA_DO_PAGAMENTO) AS DATA_EFETIVA_DO_PAGAMENTO
,ACORDO
,CONDENACAO
,PENHORA
,PENSAO
,OUTROS_PAGAMENTOS
,IMPOSTO
,CUSTAS
,COALESCE(GARANTIA, 0) AS GARANTIA
FROM HISTORICO_DE_PAGAMENTOS_JUD_CIVEL_4 A
FULL OUTER JOIN TB_GARANTIAS_CIVEL_F B
ON A.PROCESSO_ID = B.PROCESSO_ID AND A.DATA_EFETIVA_DO_PAGAMENTO = B.DATA_EFETIVA_DO_PAGAMENTO;
""")

# COMMAND ----------

df_pgto_garantias_civel_1 = df_pgto_garantias_civel.withColumn("TOTAL_PAGAMENTOS",col("ACORDO") + 
                                                                    col("CONDENACAO") +
                                                                    col("PENHORA") +
                                                                    col("PENSAO") +
                                                                    col("CUSTAS") +
                                                                    col("OUTROS_PAGAMENTOS") +
                                                                    col("IMPOSTO") +
                                                                    col("GARANTIA"))

# COMMAND ----------

df_pgto_garantias_civel_1.createOrReplaceGlobalTempView("HIST_PAGAMENTOS_GARANTIAS_CIVEL_F")

# COMMAND ----------

# Agrupa as infromações
df_pgto_garantias_civel_final = spark.sql("""
    SELECT PROCESSO_ID
        ,MAX(DATA_EFETIVA_DO_PAGAMENTO) AS DT_ULT_PGTO
        ,SUM(coalesce(ACORDO, 0)) AS ACORDO
        ,SUM(coalesce(CONDENACAO, 0)) AS CONDENACAO
        ,SUM(coalesce(PENHORA, 0)) AS PENHORA
        ,SUM(coalesce(PENSAO, 0)) AS PENSAO
        ,SUM(coalesce(OUTROS_PAGAMENTOS, 0)) AS OUTROS_PAGAMENTOS
        ,SUM(coalesce(IMPOSTO, 0)) AS IMPOSTO
        ,SUM(coalesce(CUSTAS, 0)) AS CUSTAS
        ,SUM(coalesce(GARANTIA, 0)) AS GARANTIA
        ,SUM(coalesce(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
    FROM global_temp.HIST_PAGAMENTOS_GARANTIAS_CIVEL_F
    GROUP BY 1 
    ORDER BY 1;
 """)

# COMMAND ----------

df_pgto_garantias_civel_final.createOrReplaceGlobalTempView('HIST_PAGAMENTOS_GARANTIA_CIVEL_FF')


# COMMAND ----------

df_pgto_garantias_civel_final.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("databox.juridico_comum.hist_pagamentos_garantias_civel_com_custas")

# COMMAND ----------

dbutils.notebook.exit("Success")
