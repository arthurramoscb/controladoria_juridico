# Databricks notebook source
# Cria widget 
dbutils.widgets.text("data_arq_fechamento", "DD.MM.AAAA")

dbutils.widgets.text("mes_fech", "01SET2024")

# COMMAND ----------

# MAGIC %run /Workspace/Jurídico/funcao_tratamento_fechamento/common_functions

# COMMAND ----------

# %run /Workspace/Jurídico/processos trabalhista/base histórica 24 meses /01 - ETL Base de dados juridico (trabalhista)

# COMMAND ----------

# %run /Workspace/Jurídico/processos trabalhista/base histórica 24 meses /02 - Coleta pagamentos e garantias (trabalhista)

# COMMAND ----------

# Carrega o excel fechamento trabalhista, trata o nome das colunas e ajusta valores
from pyspark.sql.functions import *

# Armazena valor inputado no widget pelo usuário em variavel
data_arq_fechamento = dbutils.widgets.get("data_arq_fechamento")
mes_fech = dbutils.widgets.get("mes_fech")

# Definine caminho de arquivo com base na variavel inputada no widget
path_arq_fechamento = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/trabalhista_base_financeiro/{data_arq_fechamento} Fechamento Trabalhista Ajustada.xlsx'

df_fechamento_original =  read_excel(path_arq_fechamento,"'Base Fechamento'!A4:CK1048576")

# Ajusta colunas criando o dataframe modificado (util para futuras comparações)
df_fechamento = adjust_column_names(df_fechamento_original)

# Remove acentos das colunas (para facilitar o uso de SQL futuramente)
df_fechamento = remove_acentos(df_fechamento)

# Define colunas para converter para data
fechamento_columns_to_date = ['DATACADASTRO','REABERTURA','DISTRIBUICAO']

# Converte columas para data
df_fechamento = convert_to_date_format(df_fechamento, fechamento_columns_to_date)

# Converter valores para seus formatos correspondentes 
df_fechamento = df_fechamento.withColumn("ID_PROCESSO", col("ID_PROCESSO").cast("int"))

# Adiciona a informação de mes_fech
df_fechamento = df_fechamento.withColumn("MES_FECH", lit(mes_fech))

# Cria uma tabela temporaria com o df tratado
df_fechamento.createOrReplaceTempView("temp_fechamento")

# COMMAND ----------

# Cria campos adicionais na tabela de fechamento
df_fechamento_trab_ger_3 = spark.sql("""
    SELECT A.*
    ,(CASE WHEN A.DATACADASTRO IS NULL THEN B.DATA_REGISTRADO 
                        ELSE A.DATACADASTRO END) AS CADASTRO_NOVO
    ,(CASE WHEN A.STATUS_M IS NULL THEN B.STATUS 
                        ELSE A.STATUS_M END) AS STATUS_NOVO
    ,B.FASE AS FASE_NOVO
    ,B.CARTEIRA
    ,(CASE WHEN A.INDICACAO_PROCESSO_ESTRATEGICO IS NULL THEN B.ULTIMA_POSICAO_ESTRATEGIA_JUSTIFICATIVA_ACORDO_DEFESA
				ELSE A.INDICACAO_PROCESSO_ESTRATEGICO END ) AS ESTRATEGIA
    FROM temp_fechamento A
    LEFT JOIN databox.juridico_comum.trab_ger_24m_20240621 B
""")

# Cria tabela temporaria com a nova visão criada acima
df_fechamento_trab_ger_3.createOrReplaceTempView('temp_fechamento_2')

# COMMAND ----------

# Realiza um tratamento para o campo FASE
df_fechamento_trab_ger_4 = spark.sql('''
    SELECT *
    ,(CASE WHEN FASE_NOVO IN ('','N/A','INATIVO','ENCERRAMENTO','ADMINISTRATIVO') THEN 'DEMAIS'
                        WHEN FASE_NOVO IN ('EXECUÇÃO' 
                                        ,'EXECUÇÃO - TRT' 
                                        ,'EXECUÇÃO - TST' 
                                        ,'EXECUÇÃO DEFINITIVA' 
                                        ,'EXECUÇÃO DEFINITIVA (TRT)' 
                                        ,'EXECUÇÃO DEFINITIVA (TST)'
                                        ,'EXECUÇÃO DEFINITIVA PROSSEGUIMENTO' 
                                        ,'EXECUÇÃO PROVISORIA (TRT)'
                                        ,'EXECUÇÃO PROVISORIA (TST)'
                                        ,'EXECUÇÃO PROVISÓRIA'
                                        ,'EXECUÇÃO PROVISORIA'
                                        ,'EXECUÇÃO PROVISÓRIA PROSSEGUIMENTO') THEN 'EXECUÇÃO'
                        WHEN FASE_NOVO IN ('RECURSAL'
                                        ,'RECURSAL TRT'
                                        ,'RECURSAL TST') THEN 'RECURSAL' 
                        ELSE FASE_NOVO END) AS DP_FASE
    FROM temp_fechamento_2
''')

# Cria tabela temporaria com a nova visão criada acima
df_fechamento_trab_ger_4.createOrReplaceTempView('temp_fechamento_3')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   A.ID_PROCESSO
# MAGIC   ,A.MES_FECH
# MAGIC   ,B.ACORDO
# MAGIC   ,B.CONDENACAO
# MAGIC   ,B.PENHORA
# MAGIC   ,B.GARANTIA
# MAGIC   ,B.IMPOSTO
# MAGIC   ,B.OUTROS_PAGAMENTOS
# MAGIC   ,A.ENCERRADOS
# MAGIC   ,(B.ACORDO + B.CONDENACAO + B.PENHORA + B.GARANTIA + B.IMPOSTO + B.OUTROS_PAGAMENTOS) AS TOTAL_PAGAMENTOS
# MAGIC   ,B.DATA_EFETIVA_PAGAMENTO AS DT_ULT_PGTO
# MAGIC   -- ,TO_DATE(CONCAT(SUBSTRING(B.DATA_EFETIVA_PAGAMENTO, 5, 2), 
# MAGIC   --                  SUBSTRING(B.DATA_EFETIVA_PAGAMENTO, 3, 2), 
# MAGIC   --                  SUBSTRING(B.DATA_EFETIVA_PAGAMENTO, 1, 2)), 'yyMMdd') AS MES_CONTABIL
# MAGIC FROM temp_fechamento_3 A
# MAGIC LEFT JOIN databox.juridico_comum.trab_pagamentos_24m_20240624 B
# MAGIC ON A.ID_PROCESSO = B.PROCESSO_ID 
# MAGIC   GROUP BY A.ID_PROCESSO, A.MES_FECH, B.ACORDO, B.CONDENACAO, B.PENHORA, B.GARANTIA, B.IMPOSTO, B.OUTROS_PAGAMENTOS, A.ENCERRADOS, B.DATA_EFETIVA_PAGAMENTO
# MAGIC

# COMMAND ----------

print('Concluido!')
