# Databricks notebook source
# MAGIC %md
# MAGIC # União de tabelas similares
# MAGIC Monta uma única tabela com todas as tablas com o mesmo padrão de nome.  
# MAGIC Inclui todas as colunas de todas as tabelas e as que não tiverem alguma coluna que outra tenha, fica com o valor NULL.

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

from pyspark.sql.types import DoubleType, LongType, TimestampType, StringType

# COMMAND ----------

# Definir o schema e o regex do nome das tabelas
nome_tabelas = "'tb_fechamento_civel%'"
nome_schema = "databox.juridico_comum"
nome_tabela_unida = "tb_fech_civel_consolidado"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 - Definição das funções

# COMMAND ----------

# Carrega as tabelas em DF e retorna uma lista de DFs
def load_dataframes(schema, table_names):
    """
    Carrega as tabelas em DF e retorna uma lista de DFs
    """
    dataframes = []
    for table_name in table_names:
        df = spark.table(f"{schema}.{table_name}")
        dataframes.append(df)
    return dataframes

# COMMAND ----------

# Deduplica as colunas com mesmo nome
def deduplicate_columns(df: DataFrame) -> DataFrame:
    columns = df.columns
    renamed_columns = [f"{col}@{i}" for i, col in enumerate(columns)]
    
    # Rename columns to make them unique
    df_renamed = df.toDF(*renamed_columns)
    
    # Identify and keep the first occurrence of each column
    unique_columns = []
    seen = set()
    for i, col in enumerate(columns):
        if col not in seen:
            unique_columns.append(renamed_columns[i])
            seen.add(col)
    
    # Select only the unique columns
    df_unique = df_renamed.select(*unique_columns)
    
    # Optionally, rename columns back to original names, if needed
    original_names = [col.split('@')[0] for col in unique_columns]
    df_final = df_unique.toDF(*original_names)
    
    return df_final

# COMMAND ----------

# Teste deduplicação de colunas
# Example DataFrame with duplicate column names
data = [(1, 'A', 100), (2, 'B', 200)]
columns = ['id', 'name', 'id']  # Duplicate 'id' column

df = spark.createDataFrame(data, columns)

# Show initial DataFrame
print("Initial DataFrame with duplicate columns:")
df.show()

# Deduplicate columns
df_deduplicated = deduplicate_columns(df)

# Show DataFrame after deduplicating columns
print("DataFrame after deduplicating columns:")
df_deduplicated.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Execução

# COMMAND ----------


# Lista todas as tabelas no catálogo com o nome buscado
tables = spark.sql(f"SHOW TABLES IN {nome_schema}").collect()
tables = spark.createDataFrame(tables)

tables = tables.where(f"tableName LIKE {nome_tabelas}")

table_names = [row.tableName for row in tables.select('tableName').collect()]

table_names.sort(reverse = True)

print(table_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1 Nomes das colunas

# COMMAND ----------

lista_dfs = load_dataframes(nome_schema, table_names)

# Ajusta os nomes de colunas para remover exesso de espaços e "_"
lista_dfs_clean = []
for df in lista_dfs:
    df_clean = adjust_column_names(df)
    df_clean = remove_acentos(df_clean)
    lista_dfs_clean.append(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2 Deduplica

# COMMAND ----------

# Deduplica colunas com mesmo nome
lista_dfs_clean_dd = []
for df in lista_dfs_clean:
    df_dd = deduplicate_columns(df)
    lista_dfs_clean_dd.append(df_dd)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3 Cast string

# COMMAND ----------

# Cast string todas as colunas
lista_dfs_clean_dd_string = []
for df in lista_dfs_clean_dd:
    for col in df.columns:
        df = df.withColumn(col, df[col].cast(StringType()))
    
    lista_dfs_clean_dd_string.append(df)

# lista_dfs_clean_dd_string[0].printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.4 All schema

# COMMAND ----------

def get_all_schema(dfs: list[DataFrame]) -> StructType:
    """
    Pega a lista de dfs e retorna o schema em comum. Ou seja,
    todas as colunas presentes em todas as tabelas.
    """
    # Get the schemas of all DataFrames
    schemas = [df.schema.fields for df in dfs]
    
    # Flatten the list of fields and use a set to find unique fields
    all_fields = {field for schema in schemas for field in schema}
    
    # Create a StructType with all the unique fields
    all_schema = StructType(list(all_fields))
    
    return all_schema

# COMMAND ----------

def create_empty_df_with_all_schema(dfs: list[DataFrame]) -> DataFrame:
    """"
    Usa as colunas em comum para criar um DF vazio.
    """
    # Get the all-inclusive schema
    all_schema = get_all_schema(dfs)
    
    # Create an empty DataFrame with the all-inclusive schema
    empty_df = spark.createDataFrame([], all_schema)
    
    return empty_df

# COMMAND ----------

unified_df = create_empty_df_with_all_schema(lista_dfs_clean_dd_string)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 - Union

# COMMAND ----------

for df in lista_dfs_clean_dd_string:
    unified_df = unified_df.unionByName(df, allowMissingColumns=True)

# unified_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 - Recast

# COMMAND ----------

# Cria um DF com seus tipos originais
df_ref = create_empty_df_with_all_schema(lista_dfs_clean_dd)

# Usa esse schema como referencia para converter novamente os tipos do DF unido
schema_ref = df_ref.schema

for field in schema_ref.fields:
    unified_df = unified_df.withColumn(field.name, unified_df[field.name].cast(field.dataType))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 - Salva a tabela final
# MAGIC

# COMMAND ----------

unified_df.printSchema()

# COMMAND ----------

# Example of renaming a duplicate column (if necessary)
unified_df = unified_df.withColumnRenamed('%_RISCO', '%_RISCO_new')


# COMMAND ----------

unified_df.write.format("delta").mode("overwrite").saveAsTable(f"{nome_schema}.{nome_tabela_unida}")

# COMMAND ----------

df_test = spark.sql(f"SELECT * FROM {nome_schema}.{nome_tabela_unida}")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

tb_fech_civel_consolidado = spark.read.table("databox.juridico_comum.tb_fech_civel_consolidado")

# COMMAND ----------

tb_fech_civel_consolidado = spark.sql("SELECT * FROM databox.juridico_comum.tb_fech_civel_consolidado")

# COMMAND ----------

display(tb_fech_civel_consolidado)

# COMMAND ----------


