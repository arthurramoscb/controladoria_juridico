# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela de Provisão Cível
# MAGIC
# MAGIC
# MAGIC ####Tabela de Pagamentos Histórico Cível
# MAGIC - **Outputs**\
# MAGIC Tabela Delta:
# MAGIC     df_fecham_civel_pbi

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Ler a tabela Delta
df_fecham_civel = spark.read.format("delta").table("databox.juridico_comum.tb_fech_civel_provisao")

df_fecham_civel = df_fecham_civel.fillna({
    "NOVOS": 0,
    "ENCERRADOS": 0,
    "ESTOQUE": 0
})

# Imprimir o schema
df_fecham_civel.write.format("delta") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"databox.juridico_comum.df_fecham_civel_pbi")

