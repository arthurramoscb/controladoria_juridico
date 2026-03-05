# Databricks notebook source
from pyspark.sql.functions import col

def getOwner(catalog, schema, table_name):
    result = spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.{table_name}")
    owner = result.filter(col("col_name") == "Owner").select("data_type").first()[0]
    return (owner)

# COMMAND ----------

def setOwner(catalog, schema, prefixo, sufixo): 
    
    owner_final = "BDAC_V_AZ_PRD_C_JURIDICO"

    df_tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    df_result = df_tables.where(col("tableName").like(f"{prefixo}%{sufixo}")).select("tableName")

    total_tabelas = df_result.count()
    display(f"Total de tabelas: {total_tabelas}")
    tabela_atual = 1
    
    for row in df_result.collect():

        table_name = row.tableName
        owner = getOwner(catalog, schema, table_name)

        print(f"Tabela: {tabela_atual}/{total_tabelas}: " + table_name + f" ({owner})")

        if (owner != owner_final):
            try:
                spark.sql(f"ALTER TABLE {catalog}.{schema}.{table_name} OWNER TO `BDAC_V_AZ_PRD_C_JURIDICO`")
                print("Tabela: " + table_name + f" ({owner_final})")
            except:
                print("Tabela: " + table_name + f" ({owner})")
                pass

# COMMAND ----------

setOwner("databox", "juridico_comum", "", "")
