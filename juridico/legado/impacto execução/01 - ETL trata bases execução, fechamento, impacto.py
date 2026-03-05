# Databricks notebook source
dbutils.widgets.text("mes_execucao", "", "Mês da Execução")

dbutils.widgets.text("data_impacto", "", "Data do cálculo do impacto")

dbutils.widgets.text("nmtabela", "", "Data do arquivo gerencial")

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # 01.01 Trata base de execução recebida

# COMMAND ----------

from pyspark.sql.functions import *

mes_execucao = dbutils.widgets.get("mes_execucao")

path_excecucao = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_execucao/RELATÓRIO DE EXECUÇÃO - {mes_execucao}.xlsx'

df_execucao = read_excel(path_excecucao)

df_execucao = adjust_column_names(df_execucao)

columns = df_execucao.columns
colunas_datas = []

colunas_datas = find_columns_with_word(df_execucao, 'DATA')

# Lista de colunas a selecionar
colunas = ["ID", "ÚLTIMO_CÁLCULO_CONSIDERADO_CRITÉRIO_GCB","VALOR_DO_ÚLTIMO_CALCULO_CONSIDERADO_CRITÉRIO_GCB","DATA_DA_ATUALIZAÇÃO_DO_ÚLTIMO_CÁLCULO_CONSIDERADO_CRITÉRIO_GCB"]

# Selecionar colunas usando a lista
df_execucao_f = df_execucao.select(*colunas)

df_execucao_f = df_execucao_f.withColumn("ÚLTIMO_CÁLCULO_CONSIDERADO_CRITÉRIO_GCB", upper(df_execucao_f["ÚLTIMO_CÁLCULO_CONSIDERADO_CRITÉRIO_GCB"]))

display(df_execucao_f)

# COMMAND ----------

# MAGIC %md
# MAGIC # 01.02 Trata arquivo de impacto

# COMMAND ----------

data_impacto = dbutils.widgets.get("data_impacto")

path_impacto = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_execucao/Base Execução - Atualizada em {data_impacto}.xlsx'

df_impacto = read_excel(path_impacto,"'Impacto'!A4")

colunas_impcato_datas = []

colunas_impcato_datas = find_columns_with_word(df_impacto, 'DATA')

df_impacto = convert_to_date_format(df_impacto,colunas_impcato_datas)

display(df_impacto)

# COMMAND ----------

# MAGIC %md
# MAGIC # 02.02 Trata arquivo Gerencial

# COMMAND ----------

nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_gerenciais/external/'

arquivo_consolidado = f'TRABALHISTA_GERENCIAL_(CONSOLIDADO)-{nmtabela}.xlsx'
arquivo_ativos = 'Trabalhista_Gerencial_(Ativos)-20191220.xlsx'
arquivo_encerrados = 'Trabalhista_Gerencial_(Encerrados)-20191230.xlsx'

path_consolidado = diretorio_origem + arquivo_consolidado
path_ativos = diretorio_origem + arquivo_ativos
path_encerrados = diretorio_origem + arquivo_encerrados

path_consolidado

# COMMAND ----------

df_consolidado = read_excel(path_consolidado, "'TRABALHISTA'!A6")
df_ativos = read_excel(path_ativos, "A1")
df_encerrados = read_excel(path_encerrados, "A1")

df_consolidado.createOrReplaceTempView('df_consolidado')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC  SELECT 
# MAGIC     EMPRESA,
# MAGIC     count(`(PROCESSO) ID`) AS quantidade_ids,
# MAGIC     ROUND((count(`(PROCESSO) ID`) * 100.0 / (SELECT COUNT(*) FROM df_consolidado)), 2) AS porcentagem
# MAGIC FROM 
# MAGIC     df_consolidado
# MAGIC GROUP BY 
# MAGIC     EMPRESA
# MAGIC ORDER BY 
# MAGIC     quantidade_ids DESC;

# COMMAND ----------

import time

x = 60

y = 60

z = x * y

for i in range(z):
  print(i)
  




