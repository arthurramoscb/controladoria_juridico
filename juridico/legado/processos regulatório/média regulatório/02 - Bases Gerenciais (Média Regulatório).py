# Databricks notebook source
# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

dbutils.widgets.text("nmtabela", "")

# COMMAND ----------

# Caminho das pastas e arquivos

nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/regulatório/media_regulatorio/'

gerencial_reg = f'REGULATORIO_GERENCIAL_(CONSOLIDADO)_{nmtabela}.xlsx'

path_gerencial_reg = diretorio_origem + gerencial_reg

# COMMAND ----------

path_dexpara_drivers = '/Volumes/databox/juridico_comum/arquivos/cível/bases_media/de_para_drivers.xlsx'

df_depara_drivers = read_excel(path_dexpara_drivers,"'DP_Regulatorio'!A1")

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# Carrega as planilhas em Spark Data Frames
gerencial_regulatorio = read_excel(path_gerencial_reg, "'Regulatório'!A6")

# COMMAND ----------

dataframes = [gerencial_regulatorio]  # Lista de DataFrames

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

lista_col_data_americana = ['DATA DO FATO GERADOR']
gerencial_regulatorio = convert_to_date_format_americano(gerencial_regulatorio,lista_col_data_americana)

# COMMAND ----------

reg_data_cols = find_columns_with_word(gerencial_regulatorio, 'DATA ')
reg_data_cols.append('DATA DE DISTRIBUIÇÃO')

# COMMAND ----------

gerencial_regulatorio = convert_to_date_format(gerencial_regulatorio, reg_data_cols)

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

print(f'Filtro de {dt_processos_24M_antes} até {dt_corte_final}')

# COMMAND ----------

gerencial_regulatorio.createOrReplaceTempView('gerencial_regulatorio')

# COMMAND ----------

gerencial_regulatorio_validar_assunto = gerencial_regulatorio.join(df_depara_drivers, gerencial_regulatorio["Ação"] == df_depara_drivers["DE"], how="left")

gerencial_regulatorio_validar_assunto.createOrReplaceTempView('gerencial_regulatorio_validar_assunto')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(`Ação`)
# MAGIC     ,DE
# MAGIC     ,PARA
# MAGIC  FROM gerencial_regulatorio_validar_assunto
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comando abaixo filtra ultimos 24 meses de baixa provisoria e encerrados

# COMMAND ----------

gerencial_regulatorio_f = spark.sql(f'''
    SELECT * FROM gerencial_regulatorio_validar_assunto
    WHERE `Status` IN ('ENCERRADO', 'BAIXA PROVISORIA')
    AND (`Data da Baixa Provisória` BETWEEN {dt_processos_24M_antes} AND {dt_corte_final}
        OR `DATA DE REGISTRO DO ENCERRAMENTO (ELAW)` BETWEEN {dt_processos_24M_antes} AND {dt_corte_final})

''')

gerencial_regulatorio_f.createOrReplaceTempView('gerencial_regulatorio_f')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria uma tabela contendo o total de pagamentos e garantias

# COMMAND ----------


from pyspark.sql.functions import *

gerencial_regulatorio_com_pagamentos = spark.sql('''
  select * from 
  gerencial_regulatorio_f A
  left join databox.juridico_comum.TB_pagamentos_garantias_media_final B
  on A.`(Processo) ID` = B.PROCESSO_ID
''')


gerencial_regulatorio_com_pagamentos = gerencial_regulatorio_com_pagamentos.withColumnRenamed("PARA", "TIPO_ACAO")

gerencial_regulatorio_com_pagamentos = gerencial_regulatorio_com_pagamentos.withColumn("VALOR_TOTAL",
    col("TOTAL_PAGAMENTOS") + 
    col("TOTAL_GARANTIAS") 
    )


gerencial_regulatorio_com_pagamentos.createOrReplaceTempView('gerencial_regulatorio_com_pagamentos')

# COMMAND ----------

display(gerencial_regulatorio_com_pagamentos)

# COMMAND ----------

base_gerencial_com_valor = gerencial_regulatorio_com_pagamentos.toPandas()

# COMMAND ----------

import openpyxl
from shutil import copyfile

nome_arquivo =f"base_gerencial_com_valor.xlsx"
caminho_local = f"/local_disk0/tmp/{nome_arquivo}"
base_gerencial_com_valor.to_excel(caminho_local, index=False, sheet_name='Detalhado_IDs')

# 4. EXPORTAÇÃO: Mover o arquivo para o Volume do Unity Catalog (onde você pode baixar)
caminho_volume = f"/Volumes/databox/juridico_comum/arquivos/regulatório/media_regulatorio/output/{nome_arquivo}"

try:
    copyfile(caminho_local, caminho_volume)
    print(f"Sucesso! Arquivo disponível em: {caminho_volume}")
except Exception as e:
    print(f"Erro ao copiar para o volume: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Faz media GLOBAL e por ASSUNTO - JUD

# COMMAND ----------

df_media_regulatorio_global_jud = spark.sql('''
    SELECT
        'GLOBAL' AS ORGAO
        ,SUM(coalesce(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
        ,SUM(coalesce(TOTAL_GARANTIAS, 0)) AS TOTAL_GARANTIAS
        ,SUM(coalesce(VALOR_TOTAL, 0)) AS VALOR_TOTAL
        ,STDDEV_POP(coalesce(VALOR_TOTAL, 0)) AS DESVIO_PADRAO
        ,(AVG(coalesce(VALOR_TOTAL, 0)) + STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2) AS LIMITE_SUPERIOR
        ,(AVG(coalesce(VALOR_TOTAL, 0)) - STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2) AS LIMITE_INFERIOR
        ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
        ,COUNT(CASE WHEN `(Processo) ID` IS NOT NULL THEN 1 END) AS TOTAL_PROCESSOS
        ,1 - (TOTAL_PAGOS / TOTAL_PROCESSOS) AS EXITO
        ,(SUM(coalesce(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS MEDIA
    FROM gerencial_regulatorio_com_pagamentos
    WHERE TIPO_ACAO = 'JUD'
  ''')

df_media_regulatorio_global_jud.createOrReplaceTempView('df_media_regulatorio_global_jud')

# COMMAND ----------

df_media_regulatorio_por_assunto_jud = spark.sql('''
    SELECT 
    `Órgão ofensor` AS ORGAO
    ,SUM(coalesce(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
    ,SUM(coalesce(TOTAL_GARANTIAS, 0)) AS TOTAL_GARANTIAS
    ,SUM(coalesce(VALOR_TOTAL, 0)) AS VALOR_TOTAL
    ,STDDEV_POP(coalesce(VALOR_TOTAL, 0)) AS DESVIO_PADRAO
    ,(SUM(coalesce(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS MEDIA
    ,(AVG(coalesce(VALOR_TOTAL, 0)) + STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2)  AS LIMITE_SUPERIOR
    ,(AVG(coalesce(VALOR_TOTAL, 0)) - STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2) AS LIMITE_INFERIOR
    ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
    ,COUNT(CASE WHEN `(Processo) ID` IS NOT NULL THEN 1 END) AS TOTAL_PROCESSOS
    ,1 - (TOTAL_PAGOS / TOTAL_PROCESSOS) AS EXITO
    FROM gerencial_regulatorio_com_pagamentos
    WHERE TIPO_ACAO = 'JUD'
    GROUP BY `Órgão ofensor`
''')

df_media_regulatorio_por_assunto_jud.createOrReplaceTempView('df_media_regulatorio_por_assunto_jud')

# COMMAND ----------

df_media_regulatorio_jud = spark.sql('''
    SELECT 
        ORGAO
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
    FROM df_media_regulatorio_global_jud
    UNION
    SELECT
        ORGAO
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
    FROM df_media_regulatorio_por_assunto_jud

''')

df_media_regulatorio_jud.createOrReplaceTempView('df_media_regulatorio_jud')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM df_media_regulatorio_jud

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria df global utilizando limites inferior e superior

# COMMAND ----------

df_pagamentos_global = spark.sql('''
    SELECT 
    'GLOBAL' AS ORGAO
    ,VALOR_TOTAL
    FROM gerencial_regulatorio_com_pagamentos 
''')

df_pagamentos_global.createOrReplaceTempView('df_pagamentos_global')

df_pagamentos_global_com_limite = spark.sql('''
    SELECT A.*
    ,B.LIMITE_SUPERIOR
    ,B.LIMITE_INFERIOR
    
     FROM 
    df_pagamentos_global A
    LEFT JOIN 
    df_media_regulatorio_global_jud B
    ON A.ORGAO = B.ORGAO

''')

df_pagamentos_global_com_limite.createOrReplaceTempView('df_pagamentos_global_com_limite')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria nova média GLOBAL dentro dos limites

# COMMAND ----------

df_pagamentos_global_nova_media = spark.sql('''
    SELECT 
        ORGAO
        ,SUM(COALESCE(VALOR_TOTAL, 0)) AS VALOR_TOTAL
        ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
        ,(SUM(COALESCE(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS NOVA_MEDIA
    FROM df_pagamentos_global_com_limite
    WHERE VALOR_TOTAL >= LIMITE_INFERIOR AND VALOR_TOTAL <= LIMITE_SUPERIOR
    GROUP BY ORGAO
''')

df_pagamentos_global_nova_media.createOrReplaceTempView('df_pagamentos_global_nova_media')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria df por assunto utilizando limites inferior e superior

# COMMAND ----------

df_pagamentos_assunto = spark.sql('''
    SELECT 
    `Órgão ofensor` AS ORGAO
    ,VALOR_TOTAL
    FROM gerencial_regulatorio_com_pagamentos 
''')

df_pagamentos_assunto.createOrReplaceTempView('df_pagamentos_assunto')

df_pagamentos_assunto_com_limite = spark.sql('''
    SELECT A.*
    ,B.LIMITE_SUPERIOR
    ,B.LIMITE_INFERIOR
    
     FROM 
    df_pagamentos_assunto A
    LEFT JOIN 
    df_media_regulatorio_por_assunto_jud B
    ON A.ORGAO = B.ORGAO

''')

df_pagamentos_assunto_com_limite.createOrReplaceTempView('df_pagamentos_assunto_com_limite')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria nova média por ASSUNTO dentro dos limites

# COMMAND ----------

df_pagamentos_assunto_nova_media = spark.sql('''
    SELECT 
        ORGAO
        ,SUM(COALESCE(VALOR_TOTAL, 0)) AS VALOR_TOTAL
        ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
        ,(SUM(COALESCE(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS NOVA_MEDIA
    FROM df_pagamentos_assunto_com_limite
    WHERE VALOR_TOTAL >= LIMITE_INFERIOR AND VALOR_TOTAL <= LIMITE_SUPERIOR
    GROUP BY ORGAO
''')

df_pagamentos_assunto_nova_media.createOrReplaceTempView('df_pagamentos_assunto_nova_media')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unificando base de média com nova média dentro dos limites - JUD

# COMMAND ----------

df_nova_media_regulatorio = spark.sql(''' 
    SELECT 
    ORGAO
    ,VALOR_TOTAL
    ,TOTAL_PAGOS
    ,NOVA_MEDIA
    FROM 
    df_pagamentos_global_nova_media 
    UNION
    SELECT 
    ORGAO
    ,VALOR_TOTAL
    ,TOTAL_PAGOS
    ,NOVA_MEDIA
    FROM
    df_pagamentos_assunto_nova_media
''')

df_nova_media_regulatorio.createOrReplaceTempView('df_nova_media_regulatorio')

# COMMAND ----------

df_media_regulatorio_nova_media_considerada = spark.sql('''
    select 
    A.*,
    B.NOVA_MEDIA
    ,CASE 
        WHEN A.TOTAL_PROCESSOS < 5 THEN (SELECT NOVA_MEDIA FROM df_nova_media_regulatorio WHERE ORGAO = 'GLOBAL')
        ELSE B.NOVA_MEDIA
    END AS MEDIA_CONSIDERADA
    from df_media_regulatorio_jud A 
    left join df_nova_media_regulatorio B
    ON A.ORGAO = B.ORGAO 
''')

df_media_regulatorio_nova_media_considerada.createOrReplaceTempView('df_media_regulatorio_nova_media_considerada')

# COMMAND ----------

df_media_regulatorio_jud_f = spark.sql('''
    select
    ORGAO
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
    ,CASE WHEN TOTAL_PROCESSOS <= 5 THEN (SELECT EXITO FROM df_media_regulatorio_nova_media_considerada WHERE ORGAO = 'GLOBAL') 
        ELSE EXITO 
    END AS EXITO_CONSIDERADO
    from df_media_regulatorio_nova_media_considerada
    ORDER BY 
    CASE 
        WHEN ORGAO = 'GLOBAL' THEN 0
        ELSE 1
    END,
    ORGAO asc;
''')

df_media_regulatorio_jud_f.createOrReplaceTempView('df_media_regulatorio_jud_f')

# COMMAND ----------

# %sql
# select * from df_media_regulatorio_jud_f

# COMMAND ----------

# Caso queira criar uma tabela delta descomente aqui

# df_media_regulatorio_jud_f.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.tb_media_regulatorio_jud{nmtabela}")

# COMMAND ----------

# REG_MEDIAS_FINAL_JUD

import pandas as pd
import re
from shutil import copyfile

table_name = f'df_media_regulatorio_jud_f_{nmtabela}'

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_media_regulatorio_jud_f.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_media_regulatorio_jud_f.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='REG_MEDIAS_FINAL_JUD')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/regulatório/media_regulatorio/output/{table_name}.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Faz média GLOBAL e POR ORGAO - ADM

# COMMAND ----------

df_media_regulatorio_global_adm = spark.sql('''
    SELECT
        'GLOBAL' AS ORGAO
        ,SUM(coalesce(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
        ,SUM(coalesce(TOTAL_GARANTIAS, 0)) AS TOTAL_GARANTIAS
        ,SUM(coalesce(VALOR_TOTAL, 0)) AS VALOR_TOTAL
        ,STDDEV_POP(coalesce(VALOR_TOTAL, 0)) AS DESVIO_PADRAO
        ,(AVG(coalesce(VALOR_TOTAL, 0)) + STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2) AS LIMITE_SUPERIOR
        ,(AVG(coalesce(VALOR_TOTAL, 0)) - STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2) AS LIMITE_INFERIOR
        ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
        ,COUNT(CASE WHEN `(Processo) ID` IS NOT NULL THEN 1 END) AS TOTAL_PROCESSOS
        ,1 - (TOTAL_PAGOS / TOTAL_PROCESSOS) AS EXITO
        ,(SUM(coalesce(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS MEDIA
    FROM gerencial_regulatorio_com_pagamentos
    WHERE TIPO_ACAO = 'ADM'
  ''')

df_media_regulatorio_global_adm.createOrReplaceTempView('df_media_regulatorio_global_adm')

# COMMAND ----------

df_media_regulatorio_por_assunto_adm = spark.sql('''
    SELECT 
    `Órgão ofensor` AS ORGAO
    ,SUM(coalesce(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
    ,SUM(coalesce(TOTAL_GARANTIAS, 0)) AS TOTAL_GARANTIAS
    ,SUM(coalesce(VALOR_TOTAL, 0)) AS VALOR_TOTAL
    ,STDDEV_POP(coalesce(VALOR_TOTAL, 0)) AS DESVIO_PADRAO
    ,(SUM(coalesce(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS MEDIA
    ,(AVG(coalesce(VALOR_TOTAL, 0)) + STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2)  AS LIMITE_SUPERIOR
    ,(AVG(coalesce(VALOR_TOTAL, 0)) - STDDEV_POP(coalesce(VALOR_TOTAL, 0)) * 2) AS LIMITE_INFERIOR
    ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
    ,COUNT(CASE WHEN `(Processo) ID` IS NOT NULL THEN 1 END) AS TOTAL_PROCESSOS
    ,1 - (TOTAL_PAGOS / TOTAL_PROCESSOS) AS EXITO
    FROM gerencial_regulatorio_com_pagamentos
    WHERE TIPO_ACAO = 'ADM'
    GROUP BY `Órgão ofensor`
''')

df_media_regulatorio_por_assunto_adm.createOrReplaceTempView('df_media_regulatorio_por_assunto_adm')

# COMMAND ----------

df_media_regulatorio_adm = spark.sql('''
    SELECT 
        ORGAO
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
    FROM df_media_regulatorio_global_adm
    UNION
    SELECT
        ORGAO
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
    FROM df_media_regulatorio_por_assunto_adm

''')

df_media_regulatorio_adm.createOrReplaceTempView('df_media_regulatorio_adm')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria df global utilizando limites inferior e superior

# COMMAND ----------

df_pagamentos_global_adm = spark.sql('''
    SELECT 
    'GLOBAL' AS ORGAO
    ,VALOR_TOTAL
    FROM gerencial_regulatorio_com_pagamentos 
''')

df_pagamentos_global_adm.createOrReplaceTempView('df_pagamentos_global_adm')

df_pagamentos_global_adm_com_limite = spark.sql('''
    SELECT A.*
    ,B.LIMITE_SUPERIOR
    ,B.LIMITE_INFERIOR
    
     FROM 
    df_pagamentos_global_adm A
    LEFT JOIN 
    df_media_regulatorio_global_adm B
    ON A.ORGAO = B.ORGAO

''')

df_pagamentos_global_adm_com_limite.createOrReplaceTempView('df_pagamentos_global_adm_com_limite')


# COMMAND ----------

df_pagamentos_global_adm_nova_media = spark.sql('''
    SELECT 
        ORGAO
        ,SUM(COALESCE(VALOR_TOTAL, 0)) AS VALOR_TOTAL
        ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
        ,(SUM(COALESCE(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS NOVA_MEDIA
    FROM df_pagamentos_global_adm_com_limite
    WHERE VALOR_TOTAL >= LIMITE_INFERIOR AND VALOR_TOTAL <= LIMITE_SUPERIOR
    GROUP BY ORGAO
''')

df_pagamentos_global_adm_nova_media.createOrReplaceTempView('df_pagamentos_global_adm_nova_media')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cria df por assunto utilizando limites - ADM

# COMMAND ----------

df_pagamentos_assunto_adm = spark.sql('''
    SELECT 
    `Órgão ofensor` AS ORGAO
    ,VALOR_TOTAL
    FROM gerencial_regulatorio_com_pagamentos 
''')

df_pagamentos_assunto_adm.createOrReplaceTempView('df_pagamentos_assunto_adm')

df_pagamentos_assunto_adm_com_limite = spark.sql('''
    SELECT A.*
    ,B.LIMITE_SUPERIOR
    ,B.LIMITE_INFERIOR
    
     FROM 
    df_pagamentos_assunto_adm A
    LEFT JOIN 
    df_media_regulatorio_por_assunto_adm B
    ON A.ORGAO = B.ORGAO

''')

df_pagamentos_assunto_adm_com_limite.createOrReplaceTempView('df_pagamentos_assunto_adm_com_limite')


# COMMAND ----------

df_pagamentos_assunto_adm_nova_media = spark.sql('''
    SELECT 
        ORGAO
        ,SUM(COALESCE(VALOR_TOTAL, 0)) AS VALOR_TOTAL
        ,COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END) AS TOTAL_PAGOS
        ,(SUM(COALESCE(VALOR_TOTAL, 0)) / COUNT(CASE WHEN VALOR_TOTAL > 0 THEN 1 END)) AS NOVA_MEDIA
    FROM df_pagamentos_assunto_adm_com_limite
    WHERE VALOR_TOTAL >= LIMITE_INFERIOR AND VALOR_TOTAL <= LIMITE_SUPERIOR
    GROUP BY ORGAO
''')

df_pagamentos_assunto_adm_nova_media.createOrReplaceTempView('df_pagamentos_assunto_adm_nova_media')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unificando base de média com nova média dentro dos limites - ADM

# COMMAND ----------

df_nova_media_adm_regulatorio = spark.sql(''' 
    SELECT 
    ORGAO
    ,VALOR_TOTAL
    ,TOTAL_PAGOS
    ,NOVA_MEDIA
    FROM 
    df_pagamentos_global_adm_nova_media 
    UNION
    SELECT 
    ORGAO
    ,VALOR_TOTAL
    ,TOTAL_PAGOS
    ,NOVA_MEDIA
    FROM
    df_pagamentos_assunto_adm_nova_media
''')

df_nova_media_adm_regulatorio.createOrReplaceTempView('df_nova_media_adm_regulatorio')

# COMMAND ----------

df_media_regulatorio_adm_nova_media_considerada = spark.sql('''
    select 
    A.*,
    B.NOVA_MEDIA
    ,CASE 
        WHEN A.TOTAL_PROCESSOS < 5 THEN (SELECT NOVA_MEDIA FROM df_nova_media_adm_regulatorio WHERE ORGAO = 'GLOBAL')
        ELSE B.NOVA_MEDIA
    END AS MEDIA_CONSIDERADA
    from df_media_regulatorio_adm A 
    left join df_nova_media_adm_regulatorio B
    ON A.ORGAO = B.ORGAO 
''')

df_media_regulatorio_adm_nova_media_considerada.createOrReplaceTempView('df_media_regulatorio_adm_nova_media_considerada')

# COMMAND ----------

df_media_regulatorio_adm_f = spark.sql('''
    select
    ORGAO
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
    ,CASE WHEN TOTAL_PROCESSOS <= 5 THEN (SELECT EXITO FROM df_media_regulatorio_nova_media_considerada WHERE ORGAO = 'GLOBAL') 
        ELSE EXITO 
    END AS EXITO_CONSIDERADO
    from df_media_regulatorio_adm_nova_media_considerada
    ORDER BY 
    CASE 
        WHEN ORGAO = 'GLOBAL' THEN 0
        ELSE 1
    END,
    ORGAO asc;
''')

df_media_regulatorio_adm_f.createOrReplaceTempView('df_media_regulatorio_adm_f')

# COMMAND ----------

# REG_MEDIAS_FINAL_JUD

import pandas as pd
import re
from shutil import copyfile

table_name = f'df_media_regulatorio_adm_f_{nmtabela}'

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_media_regulatorio_adm_f.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_media_regulatorio_adm_f.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='REG_MEDIAS_FINAL_ADM')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/regulatório/media_regulatorio/output/{table_name}.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM databox.juridico_comum.TB_pagamentos_garantias_media_final

# COMMAND ----------

df_media_pagamentos_garantias = spark.table('databox.juridico_comum.TB_pagamentos_garantias_media_final').toPandas()

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

# REG_MEDIAS_FINAL_JUD

import pandas as pd
import re
from shutil import copyfile
import openpyxl

table_name = f'df_media_pagamentos_garantias'

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_media_pagamentos_garantias

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
%pip install openpyxllocal_path = f'/local_disk0/tmp/df_media_pagamentos_garantias.xlsx'

pandas_df.to_excel(local_path, index=False, sheet_name='PAG_GARANTIAS')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/regulatório/media_regulatorio/output/{table_name}.xlsx'

copyfile(local_path, volume_path)
