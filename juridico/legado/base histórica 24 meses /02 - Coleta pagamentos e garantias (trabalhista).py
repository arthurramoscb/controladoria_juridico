# Databricks notebook source
# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo gerencial trabalhista consolidado.
#  Ex: 20240419
dbutils.widgets.text("data_arq_pagamentos", "")

dbutils.widgets.text("data_arq_garantias", "")


# COMMAND ----------

# MAGIC %run /Workspace/Jurídico/funcao_tratamento_fechamento/common_functions

# COMMAND ----------

# Carrega o excel gerencial trabalhista, trata o nome das colunas e ajusta valores
from pyspark.sql.functions import *

# Armazena valor inputado no widget pelo usuário em variavel
data_arq_pagamentos = dbutils.widgets.get("data_arq_pagamentos")

data_arq_garantias = dbutils.widgets.get("data_arq_garantias")

# Definine caminho de arquivo com base na variavel inputada no widget
path_arq_pagamentos = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{data_arq_pagamentos}.xlsx'

path_arq_garantias = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_20240325.xlsx'

# Cria dataframe utilizando função carregada do notebook commons functions
df_pagamentos_original = read_excel(path_arq_pagamentos,"'PAGAMENTOS'!A6:AF1048576")

df_garantias_original = read_excel(path_arq_garantias,"'gerencial_garantias'!A6:BG1048576")

# Ajusta colunas criando o dataframe modificado (util para futuras comparações)
df_pagamentos = adjust_column_names(df_pagamentos_original)

df_garantias = adjust_column_names(df_garantias_original)

# Remove acentos das colunas (para facilitar o uso de SQL futuramente)
df_pagamentos = remove_acentos(df_pagamentos)

df_garantias = remove_acentos(df_garantias)

# # Define colunas que terão a alteração para o formato de data
pagamentos_columns_to_date = ['DATA_REGISTRADO','DATA_SOLICITACAO_ESCRITORIO','DATA_LIMITE_DE_PAGAMENTO','DATA_DE_VENCIMENTO',
'DATA_EFETIVA_DO_PAGAMENTO']

garantias_columns_to_date = ['DATA_REGISTRADO_PROCESSO','DATA_DE_LEVANTAMENTO_EMPRESA','DATA_DE_LEVANTAMENTO_PARTE_CONTRARIA','DATA_DA_SOLICITACAO_DO_PAGAMENTO','DATA_LIMITE_DE_PAGAMENTO','DATA_DE_VENCIMENTO','DATA_EFETIVA_DO_PAGAMENTO','DATA_DO_ALVARA','DATA_DA_CI','DATA_REGISTRADO_JUDICIAL','DATA_DA_EMISSAO_JUDICIAL','DATA_DA_VIGENCIA_JUDICIAL']

# # Chama função que converte colunas definidas para formato de data
df_pagamentos = convert_to_date_format(df_pagamentos, pagamentos_columns_to_date)

df_garantias = convert_to_date_format(df_garantias, garantias_columns_to_date)

# Converter valores para seus formatos correspondentes 
df_pagamentos = df_pagamentos.withColumn("PROCESSO_ID", col("PROCESSO_ID").cast("int"))
df_pagamentos = df_pagamentos.withColumn("ID_DO_PAGAMENTO", col("ID_DO_PAGAMENTO").cast("int"))
df_pagamentos = df_pagamentos.dropna(subset=['PROCESSO_ID'])

df_garantias = df_garantias.withColumn("PROCESSO_ID", col("PROCESSO_ID").cast("int"))
df_garantias = df_garantias.dropna(subset=['PROCESSO_ID'])


# COMMAND ----------

# PAGAMENTOS: cria colunas com VALOR por SUB-ASSUNTO

assunto_acordo = ['ACORDO - CÍVEL','ACORDO - CÍVEL ESTRATÉGICO','ACORDO - CÍVEL MASSA',								'ACORDO - TRABALHISTA','PAGAMENTO DE ACORDO - TRABALHISTA (FK)','RNO - ACORDO - TRABALHISTA',						'ACORDO - CÍVEL MASSA - CARTÕES','ACORDO - CÍVEL MASSA - FORNECEDOR','ACORDO - CÍVEL MASSA - MKTPLACE'
'ACORDO - CÍVEL MASSA - SEGURO','ACORDO - IMOBILIÁRIO','ACORDO - MEDIAÇÃO','ACORDO - REGULATÓRIO - PROCON COM AUDIÊNCIA','ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO']

assunto_condenacao = ['CONDENAÇÃO - CÍVEL','CONDENAÇÃO - CÍVEL ESTRATÉGICO','CONDENAÇÃO - CÍVEL MASSA','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)','CONDENAÇÃO - REGULATÓRIO','CONDENAÇÃO - TRABALHISTA','CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO','CONDENAÇÃO - CÍVEL MASSA - CARTÕES','CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR','CONDENAÇÃO - CÍVEL MASSA - MKTPLACE','CONDENAÇÃO - CÍVEL MASSA - SEGURO','CONDENAÇÃO - IMOBILIÁRIO','CONDENAÇÃO - IMOBILIÁRIO (INCONTROVERSO)','CONDENAÇÃO - REGULATÓRIO','MULTA - REGULATÓRIO','MULTA PROCESSUAL - CÍVEL','MULTA PROCESSUAL - TRABALHISTA','MULTA PROCESSUAL - CÍVEL ESTRATÉGICO','MULTA PROCESSUAL - IMOBILIÁRIO','RNO - CONDENAÇÃO - TRABALHISTA','RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)','RNO - MULTA PROCESSUAL - TRABALHISTA']

assunto_custas = ['HONORÁRIOS CONCILIADOR - CIVEL MASSA','HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO','HONORÁRIOS PERICIAIS - CÍVEL MASSA','HONORÁRIOS PERICIAIS - IMOBILIÁRIO','HONORÁRIOS PERICIAIS - TRABALHISTA','HONORÁRIOS CONCILIADOR - CIVEL MASSA - CARTÕES','HONORÁRIOS CONCILIADOR - CIVEL MASSA - FORNECEDOR','HONORÁRIOS CONCILIADOR - CIVEL MASSA - MKTPLACE','HONORÁRIOS CONCILIADOR - CIVEL MASSA - SEGURO','HONORÁRIOS PERICIAIS - CÍVEL MASSA - CARTÕES','HONORÁRIOS PERICIAIS - CÍVEL MASSA - FORNECEDOR','HONORÁRIOS PERICIAIS - CÍVEL MASSA - MKTPLACE','HONORÁRIOS PERICIAIS - CÍVEL MASSA - SEGURO','HONORÁRIOS SUCUMBENCIAIS - REGULATÓRIO','PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)','RNO - HONORÁRIOS PERICIAIS -TRABALHISTA','CUSTAS FK - TRABALHISTA','CUSTAS PERITOS - CÍVEL','CUSTAS PERITOS - IMOBILIÁRIO','CUSTAS PROCESSUAIS - CÍVEL','CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO','CUSTAS PROCESSUAIS - CÍVEL MASSA','CUSTAS PROCESSUAIS - IMOBILIÁRIO','CUSTAS PROCESSUAIS - REGULATÓRIO','CUSTAS PROCESSUAIS - TRABALHISTA','CUSTAS PROCESSUAIS - CÍVEL MASSA - CARTÕES','CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR','CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE','CUSTAS PROCESSUAIS - CÍVEL MASSA - SEGURO','PAGAMENTO DE CUSTAS - TRIBUTÁRIO','PERITOS - REGULATÓRIO','RNO - CUSTAS PROCESSUAIS - TRABALHISTA']

assunto_impostos = ['INSS - TRABALHISTA','IR - TRABALHISTA','PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)','PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)','RNO - INSS - TRABALHISTA','RNO - IR - TRABALHISTA ','RNO - FGTS','FGTS']

assunto_penhora = ['LIBERAÇÃO DE PENHORA - MASSA','LIBERAÇÃO DE PENHORA MASSA','LIBERAÇÃO DE PENHORA - REGULATÓRIO']

assunto_pensao = ['PENSÃO - CÍVEL','PENSÃO - CÍVEL ESTRATÉGICO','PENSÃO - CÍVEL MASSA']

assunto_outros_pagamentos = ['ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL,(PAG)','ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO','ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL','LIMINAR (CÍV. MASSA)','PAGAMENTO','PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA','PAGAMENTO DE EXECUÇÃO - TRABALHISTA','PAGAMENTOS - ALL - VV','PAGAMENTOS FK']

df_pagamentos = df_pagamentos.withColumn(
    'ACORDO',
    when(col('SUB_TIPO').isin(assunto_acordo), col('VALOR')).otherwise(0)
)

df_pagamentos = df_pagamentos.withColumn(
    'CONDENACAO',
    when(col('SUB_TIPO').isin(assunto_acordo), col('VALOR')).otherwise(0)
)

df_pagamentos = df_pagamentos.withColumn(
    'CUSTAS',
    when(col('SUB_TIPO').isin(assunto_custas), col('VALOR')).otherwise(0)
)

df_pagamentos = df_pagamentos.withColumn(
    'IMPOSTO',
    when(col('SUB_TIPO').isin(assunto_impostos), col('VALOR')).otherwise(0)
)

df_pagamentos = df_pagamentos.withColumn(
    'PENHORA',
    when(col('SUB_TIPO').isin(assunto_penhora), col('VALOR')).otherwise(0)
)

df_pagamentos = df_pagamentos.withColumn(
    'PENSAO',
    when(col('SUB_TIPO').isin(assunto_pensao), col('VALOR')).otherwise(0)
)

df_pagamentos = df_pagamentos.withColumn(
    'OUTROS_PAGAMENTOS',
    when(col('SUB_TIPO').isin(assunto_outros_pagamentos), col('VALOR')).otherwise(0)
)

# COMMAND ----------

df_pagamentos.createOrReplaceTempView("temp_pagamentos")

# COMMAND ----------

# Agrupar Acordos, Condenação, Penhora, Pensão, Outros pagamentos por PROCESSO_ID e DATA_EFETIVA_DO_PAGAMENTO
df_agg_pgt_aco_cond = spark.sql("""
    SELECT PROCESSO_ID
			,DATA_EFETIVA_DO_PAGAMENTO AS DATA_EFETIVA_PAGAMENTO
			,SUM(ACORDO) AS ACORDO
			,SUM(CONDENACAO) AS CONDENACAO
			,SUM(PENHORA) AS PENHORA
			,SUM(PENSAO) AS PENSAO
			,SUM(OUTROS_PAGAMENTOS) AS OUTROS_PAGAMENTOS
	FROM temp_pagamentos
	GROUP BY 1, 2
""")

# Agrupar Impostos por PROCESSO_ID e DATA_EFETIVA_DO_PAGAMENTO
df_agg_impostos = spark.sql("""
    SELECT PROCESSO_ID
			,DATA_EFETIVA_DO_PAGAMENTO AS DATA_EFETIVA_PAGAMENTO
			,SUM(IMPOSTO) AS IMPOSTO
	FROM temp_pagamentos
	GROUP BY 1, 2
""")

# Agrupar Custas por PROCESSO_ID e DATA_EFETIVA_DO_PAGAMENTO
df_agg_custas = spark.sql("""
    SELECT PROCESSO_ID
			,DATA_EFETIVA_DO_PAGAMENTO AS DATA_EFETIVA_PAGAMENTO
			,SUM(CUSTAS) AS CUSTAS
	FROM temp_pagamentos
	GROUP BY 1, 2
""")

# Agrupando pagamentos, impostos e custas
df_agg_pagamentos = df_agg_pgt_aco_cond.join(df_agg_impostos, on=['PROCESSO_ID','DATA_EFETIVA_PAGAMENTO'], how='left')
df_agg_pagamentos = df_agg_pagamentos.join(df_agg_custas, on=['PROCESSO_ID','DATA_EFETIVA_PAGAMENTO'], how='left')
df_agg_pagamentos = df_agg_pagamentos.orderBy(desc('DATA_EFETIVA_PAGAMENTO'))

# display(df_agg_pagamentos)

# COMMAND ----------

df_garantias.createOrReplaceTempView("temp_garantias")

# COMMAND ----------

# Prepara base de garantias com filtros, o que está comentado estava comentado no código SAS
df_garantias_fil = spark.sql("""
    SELECT *
	FROM temp_garantias
    WHERE STATUS_DA_GARANTIA NOT LIKE ('%PAGAMENTO DEVOLVIDO%')
        AND STATUS <> 'REMOVIDO'
        AND TIPO_GARANTIA IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL',
            -- 'BEM MÓVEL - CÍVEL',
            -- 'BEM MÓVEL - CÍVEL MASSA',
            -- 'BEM MÓVEL - REGULATÓRIO',
            -- 'BENS IMÓVEIS - TRABALHISTA',
            -- 'BENS MÓVEIS - TRABALHISTA',
            -- 'CARTA DE FIANÇA - CÍVEL',
            -- 'CARTA DE FIANÇA - CÍVEL MASSA',
            -- 'CARTA DE FIANÇA - REGULATÓRIO',
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
            -- 'FIANÇA - TRABALHISTA',
            -- 'IMÓVEL - CÍVEL MASSA',
            -- 'IMÓVEL - REGULATÓRIO',
            -- 'INATIVO',
            -- 'LEVANTAMENTO DE CRÉDITO',
            -- 'LEVANTAMENTO DE CRÉDITO - CÍVEL MASSA',
            'PENHORA - GARANTIA',
			'PENHORA - REGULATÓRIO'
            )
""")

df_garantias_fil.createOrReplaceTempView("temp_garantias")

# COMMAND ----------

# Agrupa tabela de garantias e soma o valor atualizado

df_agg_garantias_mes_aa = spark.sql("""
    SELECT PROCESSO_ID
			,DATA_DE_LEVANTAMENTO_PARTE_CONTRARIA
            ,STATUS_DA_GARANTIA
			,SUM(VALOR_LEVANTADO_PARTE_CONTRARIA) AS VALOR_LEV_PARTE_CONTR
	FROM temp_garantias
    WHERE DATA_DE_LEVANTAMENTO_PARTE_CONTRARIA IS NOT NULL
	GROUP BY 1, 2, 3
""")

df_agg_garantias_mes_aa.createOrReplaceTempView("temp_garantias_mes_aa")

df_agg_garantias_sem_status = spark.sql("""
    SELECT PROCESSO_ID
			,DATA_DE_LEVANTAMENTO_PARTE_CONTRARIA AS DATA_EFETIVA_PAGAMENTO
			,SUM(VALOR_LEV_PARTE_CONTR) AS GARANTIA
	FROM temp_garantias_mes_aa
	GROUP BY 1, 2
""")

df_agg_garantias_sem_status.createOrReplaceTempView("df_agg_garantias_sem_status")



# COMMAND ----------

# juntando pagamentos e garantias
from pyspark.sql.functions import *

df_pagamentos_garantias = df_agg_pagamentos.join(df_agg_garantias_sem_status, on=['PROCESSO_ID','DATA_EFETIVA_PAGAMENTO'], how='left')

df_pagamentos_garantias = df_pagamentos_garantias.withColumn("GARANTIA", when(col("GARANTIA").isNull(), 0).otherwise(col("GARANTIA")))

# Cria tabela temporaria da junção
df_pagamentos_garantias.createOrReplaceTempView('temp_pagamentos_garantias')

# Agrupar tabela de pagamentos sem a data
df_agg_pagamentos_garantias = spark.sql("""
    SELECT PROCESSO_ID
			,MAX(DATA_EFETIVA_PAGAMENTO) AS DT_ULT_PGTO
			,SUM(ACORDO) AS ACORDO
            ,SUM(CONDENACAO) AS CONDENACAO
            ,SUM(PENHORA) AS PENHORA
            ,SUM(OUTROS_PAGAMENTOS) AS OUTROS_PAGAMENTOS
            ,SUM(IMPOSTO) AS IMPOSTO
            ,SUM(GARANTIA) AS GARANTIA
	FROM temp_pagamentos_garantias
	GROUP BY 1
""")

# Cria ou substitui tabela com informações para consulta
df_pagamentos_garantias.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.trab_pagamentos_24M_{data_arq_pagamentos}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Abaixo conversão para excel novamente**

# COMMAND ----------

# import pandas as pd
# # Cria o arquivo excel com o dataframe tratado (pyspark)

# data_arq_pagamentos = dbutils.widgets.get("data_arq_pagamentos")

# local_path = f'dbfs:/local_disk0/tmp/HISTORICO_DE-PAGAMENTOS_{data_arq_pagamentos}_24M.xlsx'

# # write_excel(df_pagamentos,local_path)

# volume_path = f'/Volumes/databox/juridico_comum/arquivos/modelo_provisao/output/HISTORICO_DE-PAGAMENTOS_{data_arq_pagamentos}_24M.xlsx'

# # Transformando em df pandas
# df_pagamentos_pandas = df_pagamentos.toPandas()

# df_pagamentos_pandas.to_excel('/local_disk0/tmp/PAGAMENTOS_TESTE.xlsx', engine='xlsxwriter')

# # df_pagamentos_pandas.to_excel(local_path, index=False, engine="openpyxl")

# # Movendo arquivo do local_path para o output
# # dbutils.fs.cp(local_path, volume_path)


# COMMAND ----------

#df_consolidado.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.trab_ger_consolida_{nmtabela}")
