# Databricks notebook source
# MAGIC %md
# MAGIC # Carrega base gerencial trabalhista

# COMMAND ----------

# DBTITLE 1,widgets
# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo gerencial trabalhista consolidado.
#  Ex: 20240419
dbutils.widgets.text("data_trab_ger", "")


# COMMAND ----------

# MAGIC %md
# MAGIC **Comando run abaixo para chamar funções uteis**

# COMMAND ----------

# DBTITLE 1,common_functions
# MAGIC %run /Workspace/Jurídico/funcao_tratamento_fechamento/common_functions

# COMMAND ----------

# Carrega o excel gerencial trabalhista, trata o nome das colunas e ajusta valores
from pyspark.sql.functions import *

# Armazena valor inputado no widget pelo usuário em variavel
data_trab_ger = dbutils.widgets.get("data_trab_ger")

# Definine caminho de arquivo com base na variavel inputada no widget
path_trab_ger = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_gerenciais/external/TRABALHISTA_GERENCIAL_(CONSOLIDADO)-{data_trab_ger}.xlsx'

# Cria dataframe utilizando função carregada do notebook commons functions
df_trab_ger_original = read_excel(path_trab_ger,"'TRABALHISTA'!A6:DX1048576")

# Ajusta colunas criando o dataframe modificado (util para futuras comparações)
df_trab_ger = adjust_column_names(df_trab_ger_original)

# Define colunas que terão a alteração para o formato de data
columns_to_date = ['DATA_REGISTRADO','DISTRIBUIÇÃO','PARTE_CONTRÁRIA_DATA_ADMISSÃO', 'PARTE_CONTRÁRIA_DATA_DISPENSA', 'DATA_DE_ENCERRAMENTO_NO_TRIBUNAL', 'DATA_DE_REGISTRO_DO_ENCERRAMENTO', 'DATA_DE_SOLICITAÇÃO_DE_ENCERRAMENTO', 'DATA_DA_BAIXA_PROVISÓRIA', 'DATA_DA_REATIVAÇÃO', 'DATA_AUDIENCIA_INICIAL', 'DATA_DE_REABERTURA', 'DATA_DE_RECEBIMENTO', 'DATA_DE_DESLIGAMENTO_DO_FUNCIONÁRIO_ATIVO', 'DATA_DO_TRÂNSITO_EM_JULGADO_ESOCIAL', 'DATA_DO_RESULTADO_DO_TRÂNSITO_EM_JULGADO_CONHECIMENTO', 'DATA_DE_ADMISSÃO_DO_TERCEIRO', 'DATA_DE_DEMISSÃO_DO_TERCEIRO']

# Chama função que converte colunas definidas para formato de data
df_trab_ger = convert_to_date_format(df_trab_ger, columns_to_date)

df_trab_ger = df_trab_ger.withColumn("PROCESSO_ID", col("PROCESSO_ID").cast("int"))

# Remove acentos das colunas
df_trab_ger = remove_acentos(df_trab_ger)

# Trata coluna FILIAL
df_trab_columns = df_trab_ger.columns
columns_filial = df_trab_ger.select([col_name for col_name in df_trab_ger.columns if 'FILIAL' in col_name.upper()]).columns
n_columns_filial = len(columns_filial)

for x in range(n_columns_filial):
    if x == 0:
        df_trab_ger = df_trab_ger.withColumnRenamed(columns_filial[x],'FILIAL')
    else:
        df_trab_ger = df_trab_ger.withColumnRenamed(columns_filial[x],f'FILIAL{x}')



# COMMAND ----------

# Cria tabela delta com dados para serem utilizados em outros processos

# Variavel inputada no widget
data_trab_ger = dbutils.widgets.get("data_trab_ger")

# Cria ou substitui tabela com informações para consulta
df_trab_ger.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.trab_ger_24M_{data_trab_ger}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Abaixo conversão para excel**

# COMMAND ----------


# # Variavel inputada no widget
# data_trab_ger = dbutils.widgets.get("data_trab_ger")

# # Define local para salvar arquivo excel
# local_path = f'dbfs:/local_disk0/tmp/TRABALHISTA_GERENCIAL_(CONSOLIDADO)-{data_trab_ger}_24M.xlsx'

# # Cria o arquivo excel com o dataframe tratado (pyspark)
# write_excel(df_trab_ger,local_path)

# volume_path = f'/Volumes/databox/juridico_comum/arquivos/modelo_provisao/output/TRABALHISTA_GERENCIAL_(CONSOLIDADO)-{data_trab_ger}_24M.xlsx'

# dbutils.fs.cp(local_path, volume_path)
