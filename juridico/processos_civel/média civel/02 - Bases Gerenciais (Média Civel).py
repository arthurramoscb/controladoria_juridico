# Databricks notebook source
# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

dbutils.widgets.text("nmtabela", "")

# COMMAND ----------

# Caminho das pastas e arquivos

nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/'

arquivo_ativos = f'CIVEL_GERENCIAL-(ATIVOS)-{nmtabela}.xlsx'
arquivo_encerrados = f'CIVEL_GERENCIAL-(ENCERRADO)-{nmtabela}.xlsx'

path_ativos = diretorio_origem + arquivo_ativos
path_encerrados = diretorio_origem + arquivo_encerrados

# COMMAND ----------

path_dexpara_drivers = '/Volumes/databox/juridico_comum/arquivos/cível/bases_media/de_para_drivers.xlsx'

df_depara_drivers = read_excel(path_dexpara_drivers,"'DP_Assuntos'!A1")

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# Carrega as planilhas em Spark Data Frames
gerencial_ativos_civel = read_excel(path_ativos, "'CÍVEL'!A6")
gerencial_encerrados_civel = read_excel(path_encerrados, "'CÍVEL'!A6")

# COMMAND ----------

dataframes = [gerencial_ativos_civel, gerencial_encerrados_civel]  # Lista de DataFrames

# COMMAND ----------

def convert_to_date_format_americano(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Converts columns in a PySpark DataFrame to date format.

    Args:
    - df: PySpark DataFrame
    - columns: List of column names to convert to date format

    Returns:
    - PySpark DataFrame with specified columns converted to date format
    """
    for column in columns:
        column_type = dict(df.dtypes)[column]
        try:
            if "timestamp" in column_type:
                df = df.withColumn(column, to_date(col(column), 'yyyy-MM-dd'))
            else:
                df = df.withColumn(column, when(
                                                length(col(column)) == 10,
                                                to_date(col(column), 'MM/dd/yyyy')
                                            ).otherwise(
                                                to_date(col(column), 'M/d/yy')
                                            )
                )
        except:
            print(f"Column '{column}' does not exist. Skipping...")
    return df

# COMMAND ----------

lista_col_data_americana = ['DATA DO FATO GERADOR79','DATA DO FATO GERADOR81', 'DATA DO FATO GERADOR82', 'DATA FATO GERADOR']
gerencial_ativos_civel = convert_to_date_format_americano(gerencial_ativos_civel,lista_col_data_americana)
gerencial_encerrados_civel = convert_to_date_format_americano(gerencial_encerrados_civel,lista_col_data_americana)


# COMMAND ----------

# Lista as colunas com datas
ativos_data_cols = find_columns_with_word(gerencial_ativos_civel, 'DATA ')
ativos_data_cols.append('DISTRIBUIÇÃO')

encerrados_data_cols = find_columns_with_word(gerencial_encerrados_civel, 'DATA ')
encerrados_data_cols.append('DISTRIBUIÇÃO')

# COMMAND ----------

gerencial_ativos_civel = convert_to_date_format(gerencial_ativos_civel, ativos_data_cols)
gerencial_encerrados_civel = convert_to_date_format(gerencial_encerrados_civel, encerrados_data_cols)

# COMMAND ----------

nmtabela = dbutils.widgets.get("nmtabela")

df_dt_corte_final = spark.sql(f"SELECT TO_DATE('{nmtabela}', 'yyyyMMdd') AS dt_ult_pgto")

# Extraindo o valor da data convertido
dt_corte_final = df_dt_corte_final.collect()[0]["dt_ult_pgto"]

df_24M = spark.createDataFrame([(dt_corte_final,)], ["dt_ult_pgto"])

# Subtraindo 24 meses e obtendo o primeiro dia do mês resultante
df_24M_antes = df_24M.withColumn(
    "data_24_meses_antes",
    expr("add_months(dt_ult_pgto, -24)")
).withColumn(
    "primeiro_dia_mes",
    trunc("data_24_meses_antes", "MM")
)

# Extraindo o valor da data resultante
dt_processos_24M_antes = df_24M_antes.collect()[0]["primeiro_dia_mes"]

dt_processos_24M_antes = f"'{dt_processos_24M_antes}'"
dt_corte_final = f"'{dt_corte_final}'"

# Teste 
# dt_processos_24M_antes = f"'2021-12-01'"
# dt_corte_final = f"'2023-11-30'"

print(f'Filtro de {dt_processos_24M_antes} até {dt_corte_final}')

# COMMAND ----------

gerencial_ativos_civel.createOrReplaceTempView('gerencial_ativos_civel')

gerencial_encerrados_civel.createOrReplaceTempView('gerencial_encerrados_civel')

# COMMAND ----------

gerencial_ativos_civel_validar_assunto = gerencial_ativos_civel.join(df_depara_drivers, gerencial_ativos_civel["ASSUNTO (CÍVEL) - ASSUNTO"] == df_depara_drivers["DE"], how="left")

gerencial_ativos_civel_validar_assunto.createOrReplaceTempView('gerencial_ativos_civel_validar_assunto')

# COMMAND ----------

# MAGIC %sql
# MAGIC     select       
# MAGIC     `ASSUNTO (CÍVEL) - ASSUNTO`
# MAGIC     ,COUNT(`PROCESSO - ID`) AS QTD
# MAGIC     from gerencial_ativos_civel_validar_assunto 
# MAGIC GROUP BY `ASSUNTO (CÍVEL) - ASSUNTO`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comando abaixo filtra ultimos 24 meses de baixa provisoria

# COMMAND ----------

gerencial_ativos_civel_f = spark.sql(f'''
    SELECT * FROM gerencial_ativos_civel_validar_assunto
    WHERE `DATA DA BAIXA PROVISÓRIA` IS NOT NULL 
    AND `DATA DA BAIXA PROVISÓRIA` BETWEEN {dt_processos_24M_antes} AND {dt_corte_final}
    AND STATUS = 'BAIXA PROVISORIA'
''')

gerencial_ativos_civel_f.createOrReplaceTempView('gerencial_ativos_civel_f')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Faz tratamento da gerancial encerrados

# COMMAND ----------

gerencial_encerrados_civel_validar_assunto = gerencial_encerrados_civel.join(df_depara_drivers, gerencial_encerrados_civel["ASSUNTO (CÍVEL) - ASSUNTO"] == df_depara_drivers["DE"], how="left")

gerencial_encerrados_civel_validar_assunto.createOrReplaceTempView('gerencial_encerrados_civel_validar_assunto')

# COMMAND ----------

gerencial_encerrados_civel_f = spark.sql(f'''
    SELECT * FROM gerencial_encerrados_civel_validar_assunto
    WHERE `DATA DE REGISTRO DO ENCERRAMENTO` IS NOT NULL 
    AND `DATA DE REGISTRO DO ENCERRAMENTO` BETWEEN {dt_processos_24M_antes} AND {dt_corte_final}
''')

gerencial_encerrados_civel_f.createOrReplaceTempView('gerencial_encerrados_civel_f')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria uma tabela unificando Ativos (baixa provisoria) e encerrados

# COMMAND ----------

gerencial_union = spark.sql('''
    SELECT * FROM gerencial_ativos_civel_f
    UNION ALL
    SELECT * FROM gerencial_encerrados_civel_f
''')

gerencial_union.createOrReplaceTempView('gerencial_union')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria uma tabela contendo o total de pagamentos e garantias

# COMMAND ----------


from pyspark.sql.functions import *

gerencial_union_com_pagamentos = spark.sql('''
  select * from 
  gerencial_union A
  left join databox.juridico_comum.TB_pagamentos_garantias_media_final B
  on A.`PROCESSO - ID` = B.PROCESSO_ID
''')

for colu in gerencial_union_com_pagamentos.columns:
  if 'PARA' in str(colu):
    colu_ = colu.replace(':','')
    gerencial_union_com_pagamentos = gerencial_union_com_pagamentos.withColumnRenamed(colu, colu_)

gerencial_union_com_pagamentos_ = gerencial_union_com_pagamentos.withColumnRenamed("PARA", "ASSUNTO")

gerencial_union_com_pagamentos = gerencial_union_com_pagamentos_.withColumn("VALOR_TOTAL",
    col("TOTAL_PAGAMENTOS") + 
    col("TOTAL_GARANTIAS") 
    )


gerencial_union_com_pagamentos.createOrReplaceTempView('gerencial_union_com_pagamentos')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `ASSUNTO (CÍVEL) - ASSUNTO`
# MAGIC     ,COUNT(`PROCESSO - ID`) AS QTD_PROCESSOS
# MAGIC     ,COUNT(TOTAL_PAGAMENTOS) AS QTD_PAGAMENTOS
# MAGIC     FROM gerencial_union_com_pagamentos
# MAGIC     GROUP BY 1

# COMMAND ----------

df_media_civel_global = spark.sql('''
    SELECT
        'GLOBAL' AS ASSUNTO
        ,SUM(coalesce(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
        ,SUM(coalesce(TOTAL_GARANTIAS, 0)) AS TOTAL_GARANTIAS
        ,SUM(coalesce(VALOR_TOTAL, 0)) AS VALOR_TOTAL
        ,STDDEV_POP(coalesce(VALOR_TOTAL, 0)) AS DESVIO_PADRAO
        ,(AVG(coalesce(VALOR_TOTAL, 0)) + STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2) AS LIMITE_SUPERIOR
        ,(AVG(coalesce(VALOR_TOTAL, 0)) - STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2) AS LIMITE_INFERIOR
        ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
        ,COUNT(CASE WHEN `PROCESSO - ID` IS NOT NULL THEN 1 END) AS TOTAL_PROCESSOS
        ,1 - (TOTAL_PAGOS / TOTAL_PROCESSOS) AS EXITO
        ,(SUM(coalesce(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS MEDIA
    FROM gerencial_union_com_pagamentos
  ''')

df_media_civel_global.createOrReplaceTempView('df_media_civel_global')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct * from gerencial_union_com_pagamentos 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gerencial_union_com_pagamentos WHERE `PROCESSO - ID` = 1258599

# COMMAND ----------

# MAGIC %sql
# MAGIC select `PROCESSO - ID`, COUNT(`PROCESSO - ID`), COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) from gerencial_union_com_pagamentos GROUP BY `PROCESSO - ID` having COUNT(`PROCESSO - ID`) > 1

# COMMAND ----------

df_media_civel_por_assunto = spark.sql('''
    SELECT 
    ASSUNTO
    ,SUM(coalesce(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
    ,SUM(coalesce(TOTAL_GARANTIAS, 0)) AS TOTAL_GARANTIAS
    ,SUM(coalesce(VALOR_TOTAL, 0)) AS VALOR_TOTAL
    ,STDDEV_POP(coalesce(VALOR_TOTAL, 0)) AS DESVIO_PADRAO
    ,(SUM(coalesce(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS MEDIA
    ,(AVG(coalesce(VALOR_TOTAL, 0)) + STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2)  AS LIMITE_SUPERIOR
    ,(AVG(coalesce(VALOR_TOTAL, 0)) - STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2) AS LIMITE_INFERIOR
    ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
    ,COUNT(CASE WHEN `PROCESSO - ID` IS NOT NULL THEN 1 END) AS TOTAL_PROCESSOS
    ,1 - (TOTAL_PAGOS / TOTAL_PROCESSOS) AS EXITO
    FROM gerencial_union_com_pagamentos
    GROUP BY ASSUNTO
''')

df_media_civel_por_assunto.createOrReplaceTempView('df_media_civel_por_assunto')

# COMMAND ----------

df_media_civel = spark.sql('''
    SELECT 
        ASSUNTO
        ,TOTAL_PAGAMENTOS
        ,TOTAL_GARANTIAS
        ,VALOR_TOTAL
        ,DESVIO_PADRAO
        ,MEDIA
        ,LIMITE_SUPERIOR
        ,LIMITE_INFERIOR
        ,TOTAL_PAGOS
        ,TOTAL_PROCESSOS
        ,EXITO
    FROM df_media_civel_global
    UNION
    SELECT
        ASSUNTO
        ,TOTAL_PAGAMENTOS
        ,TOTAL_GARANTIAS
        ,VALOR_TOTAL
        ,DESVIO_PADRAO
        ,MEDIA
        ,LIMITE_SUPERIOR
        ,LIMITE_INFERIOR
        ,TOTAL_PAGOS
        ,TOTAL_PROCESSOS
        ,EXITO
    FROM df_media_civel_por_assunto

''')

df_media_civel.createOrReplaceTempView('df_media_civel')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria df global utilizando limites inferior e superior

# COMMAND ----------

df_pagamentos_global = spark.sql('''
    SELECT 
    'GLOBAL' AS ASSUNTO
    ,VALOR_TOTAL
    FROM gerencial_union_com_pagamentos 
''')

df_pagamentos_global.createOrReplaceTempView('df_pagamentos_global')

df_pagamentos_global_com_limite = spark.sql('''
    SELECT A.*
    ,B.LIMITE_SUPERIOR
    ,B.LIMITE_INFERIOR
    
     FROM 
    df_pagamentos_global A
    LEFT JOIN 
    df_media_civel_global B
    ON A.ASSUNTO = B.ASSUNTO

''')

df_pagamentos_global_com_limite.createOrReplaceTempView('df_pagamentos_global_com_limite')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria nova média GLOBAL dentro dos limites

# COMMAND ----------

df_pagamentos_global_nova_media = spark.sql('''
    SELECT 
        ASSUNTO
        ,SUM(COALESCE(VALOR_TOTAL, 0)) AS VALOR_TOTAL
        ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
        ,(SUM(COALESCE(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS NOVA_MEDIA
    FROM df_pagamentos_global_com_limite
    WHERE VALOR_TOTAL >= LIMITE_INFERIOR AND VALOR_TOTAL <= LIMITE_SUPERIOR
    GROUP BY ASSUNTO
''')

df_pagamentos_global_nova_media.createOrReplaceTempView('df_pagamentos_global_nova_media')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria df por assunto utilizando limites inferior e superior

# COMMAND ----------

df_pagamentos_assunto = spark.sql('''
    SELECT 
    ASSUNTO
    ,VALOR_TOTAL
    FROM gerencial_union_com_pagamentos 
''')

df_pagamentos_assunto.createOrReplaceTempView('df_pagamentos_assunto')

df_pagamentos_assunto_com_limite = spark.sql('''
    SELECT A.*
    ,B.LIMITE_SUPERIOR
    ,B.LIMITE_INFERIOR
    
     FROM 
    df_pagamentos_assunto A
    LEFT JOIN 
    df_media_civel_por_assunto B
    ON A.ASSUNTO = B.ASSUNTO

''')

df_pagamentos_assunto_com_limite.createOrReplaceTempView('df_pagamentos_assunto_com_limite')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria nova média por ASSUNTO dentro dos limites

# COMMAND ----------

df_pagamentos_assunto_nova_media = spark.sql('''
    SELECT 
        ASSUNTO
        ,SUM(COALESCE(VALOR_TOTAL, 0)) AS VALOR_TOTAL
        ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
        ,(SUM(COALESCE(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS NOVA_MEDIA
    FROM df_pagamentos_assunto_com_limite
    WHERE VALOR_TOTAL >= LIMITE_INFERIOR AND VALOR_TOTAL <= LIMITE_SUPERIOR
    GROUP BY ASSUNTO
''')

df_pagamentos_assunto_nova_media.createOrReplaceTempView('df_pagamentos_assunto_nova_media')

# COMMAND ----------

df_nova_media_civel = spark.sql(''' 
    SELECT 
    ASSUNTO
    ,VALOR_TOTAL
    ,TOTAL_PAGOS
    ,NOVA_MEDIA
    FROM 
    df_pagamentos_global_nova_media 
    UNION
    SELECT 
    ASSUNTO
    ,VALOR_TOTAL
    ,TOTAL_PAGOS
    ,NOVA_MEDIA
    FROM
    df_pagamentos_assunto_nova_media
''')

df_nova_media_civel.createOrReplaceTempView('df_nova_media_civel')

# COMMAND ----------

df_media_civel_nova_media_considerada = spark.sql('''
    select 
    A.*,
    B.NOVA_MEDIA
    ,CASE 
        WHEN A.TOTAL_PROCESSOS < 5 THEN (SELECT NOVA_MEDIA FROM df_nova_media_civel WHERE ASSUNTO = 'GLOBAL')
        ELSE B.NOVA_MEDIA
    END AS MEDIA_CONSIDERADA
    from df_media_civel A 
    left join df_nova_media_civel B
    ON A.ASSUNTO = B.ASSUNTO 
''')

df_media_civel_nova_media_considerada.createOrReplaceTempView('df_media_civel_nova_media_considerada')

# COMMAND ----------

df_media_civel_f = spark.sql('''
    select
    ASSUNTO
    ,TOTAL_PAGAMENTOS
    ,TOTAL_GARANTIAS
    ,VALOR_TOTAL
    ,DESVIO_PADRAO
    ,MEDIA
    ,LIMITE_SUPERIOR
    ,LIMITE_INFERIOR
    ,TOTAL_PAGOS
    ,TOTAL_PROCESSOS
    ,EXITO
    ,NOVA_MEDIA
    ,MEDIA_CONSIDERADA
    ,CASE WHEN TOTAL_PROCESSOS <= 5 THEN (SELECT EXITO FROM df_media_civel_nova_media_considerada WHERE ASSUNTO = 'GLOBAL') 
        ELSE EXITO 
    END AS EXITO_CONSIDERADO
    from df_media_civel_nova_media_considerada
    ORDER BY 
    CASE 
        WHEN ASSUNTO = 'GLOBAL' THEN 0
        ELSE 1
    END,
    ASSUNTO asc;
''')

df_media_civel_f.createOrReplaceTempView('df_media_civel_f')

# COMMAND ----------

# # Caso queira criar uma tabela delta descomente aqui

# df_media_civel_f.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.tb_media_civel_{nmtabela}")

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

table_name = f'df_media_civel_f_{nmtabela}'

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_media_civel_f.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_media_civel_f.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='CIV_MEDIAS_FINAL')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_media/output/{table_name}_old.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

df_gerencial_union = spark.table('gerencial_union_com_pagamentos').toPandas()


# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

table_name = f'df_gerencial_union_civel_f_{nmtabela}'

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_gerencial_union

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_media_civel_f.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='UNION')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_media/output/{table_name}.xlsx'

copyfile(local_path, volume_path)
