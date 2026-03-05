# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento dos dados de Pagamentos e Garantias
# MAGIC
# MAGIC **Inputs**\
# MAGIC Planilhas fornecidas pelo ELAW e tratadas pela área finaceira. Pagamentos e Garantias.
# MAGIC
# MAGIC **Outputs**\
# MAGIC Uma Global View com os dados de Pagamentos e Garantias tratados e unidos para utilização nos próximos passos
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import coalesce, row_number, lit, col

# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo. Ex: 20240423
dbutils.widgets.text("nmtabela_pgto", "")
dbutils.widgets.text("nmtabela_garantias", "")
dbutils.widgets.text("nmtabela", "")
dbutils.widgets.text("nmencerrados_historico", "")
dbutils.widgets.text("gerencial_regulatorio", "")

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Caminho das pastas e arquivos
nmtabela_pgto = dbutils.widgets.get("nmtabela_pgto")
nmtabela_garantias = dbutils.widgets.get("nmtabela_garantias")
nm_regulatorio = dbutils.widgets.get("gerencial_regulatorio")

# Padrão
# path_pagamentos = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{nmtabela_pgto}.xlsx'
# path_garantias = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_{nmtabela_garantias}.xlsx'

# Arquivo com pagamentos desindexados no excel (processo a ser feito depois aqui)
path_pagamentos = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{nmtabela_pgto}.xlsx'
path_garantias = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_{nmtabela_garantias}.xlsx'

# Arquivos gerenciais
nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/'

arquivo_ativos = f'CIVEL_GERENCIAL-(ATIVOS)-{nmtabela}.xlsx'
arquivo_encerrados = f'CIVEL_GERENCIAL-(ENCERRADO)-{nmtabela}.xlsx'

path_ativos = diretorio_origem + arquivo_ativos
path_encerrados = diretorio_origem + arquivo_encerrados

arquivo_encerrados_historico = 'CIVEL_ENCERRADOS_HISTORICO - 20241009.xlsx'
path_encerrados_historico = diretorio_origem + arquivo_encerrados_historico

path_ger_regulatorio = f'/Volumes/databox/juridico_comum/arquivos/regulatório/media_regulatorio/REGULATORIO_GERENCIAL_(CONSOLIDADO)_{nm_regulatorio}.xlsx'

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# Carrega as planilhas em Spark Data Frames
gerencial_ativos_civel = read_excel(path_ativos, "'CÍVEL'!A6")
gerencial_encerrados_civel = read_excel(path_encerrados, "'CÍVEL'!A6")
gerencial_regulatorio = read_excel(path_ger_regulatorio, "'Regulatório'!A6")

gerencial_ativos_civel = gerencial_ativos_civel.withColumn("DISTRIBUIÇÃO_F", coalesce(col("DISTRIBUIÇÃO"), col("DATA REGISTRADO")))
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumn("DISTRIBUIÇÃO_F", coalesce(col("DISTRIBUIÇÃO"), col("DATA REGISTRADO")))
gerencial_encerrados_civel_historico = read_excel(path_encerrados_historico, "'CÍVEL ENCERRADOS HISTÓRICO'!A6")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumn("DISTRIBUIÇÃO_F", coalesce(col("DISTRIBUIÇÃO"), col("DATA REGISTRADO")))

gerencial_regulatorio  = gerencial_regulatorio.withColumn("DISTRIBUIÇÃO_F", coalesce(col("DATA DE DISTRIBUIÇÃO"), col("Data Registrado")))

gerencial_regulatorio = gerencial_regulatorio.withColumnRenamed("(Processo) ID", "PROCESSO - ID")
gerencial_regulatorio = gerencial_regulatorio.withColumnRenamed("Status", "STATUS")

gerencial_regulatorio_f = gerencial_regulatorio[['PROCESSO - ID', 'STATUS', 'DISTRIBUIÇÃO_F']]
gerencial_ativos_civel_f = gerencial_ativos_civel[['PROCESSO - ID', 'STATUS', 'DISTRIBUIÇÃO_F']]
gerencial_encerrados_civel_f = gerencial_encerrados_civel[['PROCESSO - ID', 'STATUS', 'DISTRIBUIÇÃO_F']]
gerencial_encerrados_civel_historico_f = gerencial_encerrados_civel_historico[['PROCESSO - ID', 'STATUS', 'DISTRIBUIÇÃO_F']]

gerencial_encerrados_civel_historico_f.createOrReplaceTempView('gerencial_encerrados_civel_historico_f')

gerencial_union_1 =  gerencial_ativos_civel_f.union(gerencial_encerrados_civel_f)

gerencial_union_2 = gerencial_union_1.union(gerencial_encerrados_civel_historico_f)

gerencial_union_3 = gerencial_union_2.union(gerencial_regulatorio_f)

gerencial_union_3.createOrReplaceTempView("gerencial_union_3")

gerencial_union = spark.sql('''
    SELECT
        t1.`PROCESSO - ID`,
        t1.`STATUS`,
        t1.`DISTRIBUIÇÃO_F`
    FROM gerencial_union_3 t1
    INNER JOIN (
        SELECT
            `PROCESSO - ID`,
            MAX(`DISTRIBUIÇÃO_F`) AS `MAIOR_DISTRIBUIÇÃO`
        FROM gerencial_union_3
        GROUP BY `PROCESSO - ID`
    ) t2 ON t1.`PROCESSO - ID` = t2.`PROCESSO - ID`
    AND t1.`DISTRIBUIÇÃO_F` = t2.`MAIOR_DISTRIBUIÇÃO`
''')

gerencial_union.createOrReplaceTempView("gerencial_union")




# COMMAND ----------

# Carrega as planilhas em Spark Data Frames
df_pgto_civel = read_excel(path_pagamentos, "A6")
df_garantias_civel = read_excel(path_garantias, "A6")

df_pgto_civel.createOrReplaceTempView('df_pgto_civel')
df_garantias_civel.createOrReplaceTempView('df_garantias_civel')

# COMMAND ----------

# Lista as colunas com datas
pgto_data_cols = find_columns_with_word(df_pgto_civel, 'DATA ')
garantias_data_cols = find_columns_with_word(df_garantias_civel, 'DATA ')

# COMMAND ----------

# Converte as datas das colunas listadas
df_pgto_civel = convert_to_date_format(df_pgto_civel, pgto_data_cols)
df_garantias_civel = convert_to_date_format(df_garantias_civel, garantias_data_cols)

# COMMAND ----------

# Ajusta nome de colunas
df_pgto_civel = adjust_column_names(df_pgto_civel)
df_garantias_civel = adjust_column_names(df_garantias_civel)

# COMMAND ----------

# Caminho dos Dexparas fixos
path_dexpara_drivers = '/Volumes/databox/juridico_comum/arquivos/cível/bases_media/de_para_drivers.xlsx'
path_dexpara_pagamentos_validos = '/Volumes/databox/juridico_comum/arquivos/cível/bases_media/Pagamentos_e_Garantias_Validos.xlsx'

df_depara_drivers = read_excel(path_dexpara_drivers,"'DP_Assuntos'!A1")
df_tipo_pgto_validos = read_excel(path_dexpara_pagamentos_validos,"'Tipos de Pagto Validos'!A1") 
df_status_pgto_validos = read_excel(path_dexpara_pagamentos_validos,"'Status de Pagto'!A1")
df_tipo_garantias_validos = read_excel(path_dexpara_pagamentos_validos,"'Tipo Garantia'!A1") 
df_status_garantias_validos = read_excel(path_dexpara_pagamentos_validos,"'Status Garantia'!A1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atualiza pagamentos válidos

# COMMAND ----------

df_pgto_civel_validar_tipo = df_pgto_civel.join(df_tipo_pgto_validos, df_pgto_civel["SUB_TIPO"] == df_tipo_pgto_validos["Tipo"], how="left")

df_pgto_civel_validar_tipo.createOrReplaceTempView("df_pgto_civel_validar_tipo")


# COMMAND ----------

df_pgto_civel_tipos_fora_depara = spark.sql('''
    SELECT DISTINCT(`SUB_TIPO`) FROM df_pgto_civel_validar_tipo
    where `Entra_Tipo_Pgto?` is null
    ORDER BY `SUB_TIPO` ASC
''')

df_pgto_civel_tipos_fora_depara.createOrReplaceTempView('df_pgto_civel_tipos_fora_depara')

# COMMAND ----------

df_pgto_civel_validar_status = df_pgto_civel_validar_tipo.join(df_status_pgto_validos, df_pgto_civel_validar_tipo["STATUS_DO_PAGAMENTO"] == df_status_pgto_validos["Status"], how="left")

df_pgto_civel_validar_status.createOrReplaceTempView('df_pgto_civel_validar_status')

# COMMAND ----------

df_pgto_civel_status_fora_depara = spark.sql('''
    SELECT DISTINCT(`STATUS_DO_PAGAMENTO`) FROM df_pgto_civel_validar_status
    where `Entra_Status_Pgto?` is null
    ORDER BY `STATUS_DO_PAGAMENTO` ASC
''')

df_pgto_civel_status_fora_depara.createOrReplaceTempView('df_pgto_civel_status_fora_depara')

# COMMAND ----------

df_pgto_civel_validado = df_pgto_civel_validar_status.filter(
        (df_pgto_civel_validar_status["Entra_Tipo_Pgto?"] == "Sim") 
    &   (df_pgto_civel_validar_status["Entra_Status_Pgto?"] == "Sim")
)

df_pgto_civel_validado.createOrReplaceTempView('df_pgto_civel_validado')

# COMMAND ----------

nmtabela_pgto = dbutils.widgets.get("nmtabela_pgto")

df_dt_ult_pgto = spark.sql(f"SELECT TO_DATE('{nmtabela_pgto}', 'yyyyMMdd') AS dt_ult_pgto")

# Extraindo o valor da data convertido
dt_ult_pgto = df_dt_ult_pgto.collect()[0]["dt_ult_pgto"]

df_24M = spark.createDataFrame([(dt_ult_pgto,)], ["dt_ult_pgto"])

# Subtraindo 24 meses e obtendo o primeiro dia do mês resultante
df_24M_antes = df_24M.withColumn(
    "data_24_meses_antes",
    expr("add_months(dt_ult_pgto, -24)")
).withColumn(
    "primeiro_dia_mes",
    trunc("data_24_meses_antes", "MM")
)

# Extraindo o valor da data resultante
dt_pgto_24M_antes = df_24M_antes.collect()[0]["primeiro_dia_mes"]

dt_pgto_24M_antes = f"'{dt_pgto_24M_antes}'"
dt_ult_pgto = f"'{dt_ult_pgto}'"

print(f'Filtro de {dt_pgto_24M_antes} até {dt_ult_pgto}')

# COMMAND ----------

df_pgto_civel_validado_f = spark.sql(f''' 
    SELECT * FROM df_pgto_civel_validado
    WHERE COALESCE(`DATA_EFETIVA_DO_PAGAMENTO`, `DATA_SOLICITAÇÃO_ESCRITÓRIO`) BETWEEN '1900-01-01'
    AND {dt_ult_pgto}
    AND `ÁREA_DO_DIREITO` IN ('CÍVEL ESTRATÉGICO','CÍVEL MASSA','REGULATÓRIO')
   ''')

df_pgto_civel_validado_f.createOrReplaceTempView('df_pgto_civel_validado_f')

# COMMAND ----------

df_pgto_civel_validado_agg_f = spark.sql('''
    SELECT PROCESSO_ID
    ,MAX(DATA_EFETIVA_DO_PAGAMENTO) AS DT_ULT_PGTO
    ,SUM(coalesce(VALOR, 0)) AS TOTAL_PAGAMENTOS
    FROM df_pgto_civel_validado_f
    GROUP BY 
    PROCESSO_ID
''')

df_pgto_civel_validado_agg_f.createOrReplaceTempView('df_pgto_civel_validado_agg_f')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Desindexação de total de pagamentos

# COMMAND ----------

from pyspark.sql.functions import trunc, col

df_pgto_civel_com_distribuicao = spark.sql('''
    SELECT A.*
    ,B.STATUS
    ,B.`DISTRIBUIÇÃO_F`
    FROM df_pgto_civel_validado_agg_f A 
    LEFT JOIN gerencial_union B
    on A.PROCESSO_ID = B.`PROCESSO - ID`
''')


df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn("DISTRIBUIÇÃO_F", to_date(df_pgto_civel_com_distribuicao["DISTRIBUIÇÃO_F"]))
df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn("DISTRIBUIÇÃO_F", trunc(df_pgto_civel_com_distribuicao["DISTRIBUIÇÃO_F"], "MM"))
df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn("DT_ULT_PGTO", trunc(df_pgto_civel_com_distribuicao["DT_ULT_PGTO"], "MM"))
df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn("ANO_DISTRIBUICAO", year(df_pgto_civel_com_distribuicao["DISTRIBUIÇÃO_F"]))
df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn("ANO_ULT_PGTO", year(df_pgto_civel_com_distribuicao["DT_ULT_PGTO"]))
df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn("MES_DISTRIBUICAO", month(df_pgto_civel_com_distribuicao["DISTRIBUIÇÃO_F"]))
df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn("MES_ULT_PGTO", month(df_pgto_civel_com_distribuicao["DT_ULT_PGTO"]))
df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn("MESES_DIFERENÇA", (col("ANO_ULT_PGTO") - col("ANO_DISTRIBUICAO")) * 12 + (col("MES_ULT_PGTO") - col("MES_DISTRIBUICAO") ))


# COMMAND ----------

df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn(
    "MESES_DIF",
    when(col("MESES_DIFERENÇA") <= 0, 1).otherwise(col("MESES_DIFERENÇA"))
)

df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.fillna({"MESES_DIF": 1})

df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn(
    "DESINDEXADOR",
    (col("MESES_DIF") * 0.01) + 1.01
)

df_pgto_civel_com_distribuicao = df_pgto_civel_com_distribuicao.withColumn(
    "VALOR_DESINDEXADO",
    (col("TOTAL_PAGAMENTOS") / col("DESINDEXADOR" ))
)

df_pgto_civel_com_distribuicao.createOrReplaceTempView('df_pgto_civel_com_distribuicao')

# COMMAND ----------

df_pgto_civel_com_distribuicao.show()

# Para ver mais colunas sem truncar o conteúdo:
#df_pgto_civel_com_distribuicao.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atualiza Garantias Válidas

# COMMAND ----------

df_garantias_civel_validar_tipo = df_garantias_civel.join(df_tipo_garantias_validos, df_garantias_civel["TIPO_GARANTIA"] == df_tipo_garantias_validos["Tipo"], how="left")

df_garantias_civel_validar_tipo.createOrReplaceTempView('df_garantias_civel_validar_tipo')

# COMMAND ----------

df_garantias_civel_validar_tipos_fora_depara = spark.sql('''
    SELECT DISTINCT(`TIPO_GARANTIA`) FROM df_garantias_civel_validar_tipo
    where `Entra_Tipo_Garantia?` is null
    ORDER BY `TIPO_GARANTIA` ASC
''')

df_garantias_civel_validar_tipos_fora_depara.createOrReplaceTempView('df_garantias_civel_validar_tipos_fora_depara')

# COMMAND ----------

df_garantias_civel_validar_status = df_garantias_civel_validar_tipo.join(df_status_garantias_validos, df_garantias_civel_validar_tipo["STATUS_DA_GARANTIA"] == df_status_garantias_validos["Tipo"], how="left")

df_garantias_civel_validar_status.createOrReplaceTempView('df_garantias_civel_validar_status')

# COMMAND ----------

df_garantias_civel_status_fora_depara = spark.sql('''
    SELECT DISTINCT(`STATUS_DA_GARANTIA`) FROM df_garantias_civel_validar_status
    where `Entra_Status_Garantia?` is null
    ORDER BY `STATUS_DA_GARANTIA` ASC
''')

df_pgto_civel_status_fora_depara.createOrReplaceTempView('df_pgto_civel_status_fora_depara')

# COMMAND ----------

df_garantias_civel_f = df_garantias_civel_validar_status.filter(
        (df_garantias_civel_validar_status["Entra_Tipo_Garantia?"] == "Sim") 
    &   (df_garantias_civel_validar_status["Entra_Status_Garantia?"] == "Sim")
    &   (df_garantias_civel_validar_status["ÁREA_DO_DIREITO"].isin("CÍVEL MASSA", "CÍVEL ESTRATÉGICO", "REGULATÓRIO"))
)

df_garantias_civel_f.createOrReplaceTempView('df_garantias_civel_f')

# COMMAND ----------

df_garantias_civel_validado_agg_f = spark.sql('''
    SELECT PROCESSO_ID
    ,MAX(DATA_EFETIVA_DO_PAGAMENTO) AS DT_ULT_PGTO
    ,SUM(coalesce(`VALOR_LEVANTADO_PARTE_CONTRÁRIA`, 0)) AS TOTAL_GARANTIAS
    FROM df_garantias_civel_f
    GROUP BY PROCESSO_ID
''')

df_garantias_civel_validado_agg_f.createOrReplaceTempView('df_garantias_civel_validado_agg_f')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Desindexação de total de garantias

# COMMAND ----------

from pyspark.sql.functions import trunc, col

df_garantias_civel_com_distribuicao = spark.sql('''
    SELECT A.*
    ,B.STATUS
    ,B.`DISTRIBUIÇÃO_F`
    FROM df_garantias_civel_validado_agg_f A 
    LEFT JOIN gerencial_union B
    on A.PROCESSO_ID = B.`PROCESSO - ID`
''')


df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn("DISTRIBUIÇÃO_F", to_date(df_garantias_civel_com_distribuicao["DISTRIBUIÇÃO_F"]))
df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn("DISTRIBUIÇÃO_F", trunc(df_garantias_civel_com_distribuicao["DISTRIBUIÇÃO_F"], "MM"))
df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn("DT_ULT_PGTO", trunc(df_garantias_civel_com_distribuicao["DT_ULT_PGTO"], "MM"))
df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn("ANO_DISTRIBUICAO", year(df_garantias_civel_com_distribuicao["DISTRIBUIÇÃO_F"]))
df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn("ANO_ULT_PGTO", year(df_garantias_civel_com_distribuicao["DT_ULT_PGTO"]))
df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn("MES_DISTRIBUICAO", month(df_garantias_civel_com_distribuicao["DISTRIBUIÇÃO_F"]))
df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn("MES_ULT_PGTO", month(df_garantias_civel_com_distribuicao["DT_ULT_PGTO"]))
df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn("MESES_DIFERENÇA", (col("ANO_ULT_PGTO") - col("ANO_DISTRIBUICAO")) * 12 + (col("MES_ULT_PGTO") - col("MES_DISTRIBUICAO") ))

df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn(
    "MESES_DIF",
    when(col("MESES_DIFERENÇA") <= 0, 1).otherwise(col("MESES_DIFERENÇA"))
)

df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn(
    "DESINDEXADOR",
    (col("MESES_DIF") * 0.01) + 1.01
)

df_garantias_civel_com_distribuicao = df_garantias_civel_com_distribuicao.withColumn(
    "TOTAL_GARANTIAS_DESINDEXADO",
    (col("TOTAL_GARANTIAS") / col("DESINDEXADOR" ))
)

df_garantias_civel_com_distribuicao.createOrReplaceTempView('df_garantias_civel_com_distribuicao')

df_garantias_civel_com_distribuicao_f = spark.sql('''
    select distinct * from df_garantias_civel_com_distribuicao                                              
 ''')

df_garantias_civel_com_distribuicao_f.createOrReplaceTempView('df_garantias_civel_com_distribuicao_f')


# COMMAND ----------

df_pagamentos_garantias_media_final = spark.sql('''
    SELECT 
        coalesce(A.PROCESSO_ID, B.PROCESSO_ID) AS PROCESSO_ID
        ,coalesce(A.DT_ULT_PGTO, B.DT_ULT_PGTO) AS DATA_EFETIVA_DO_PAGAMENTO
        ,coalesce(A.VALOR_DESINDEXADO, 0) AS TOTAL_PAGAMENTOS
        ,coalesce(B.TOTAL_GARANTIAS_DESINDEXADO, 0) AS TOTAL_GARANTIAS
    FROM df_pgto_civel_com_distribuicao A 
    LEFT JOIN df_garantias_civel_com_distribuicao_f B 
    ON A.PROCESSO_ID = B.PROCESSO_ID
''')

df_pagamentos_garantias_media_final.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("databox.juridico_comum.TB_pagamentos_garantias_media_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databox.juridico_comum.TB_pagamentos_garantias_media_final

# COMMAND ----------

media_spark = spark.read.table("databox.juridico_comum.TB_pagamentos_garantias_media_final")

df_media = media_spark.toPandas()

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

#table_name = f'df_media_{nmtabela}'


# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_media_civel_f.xlsx'
df_media.to_excel(local_path, index=False, sheet_name='CIV_MEDIAS_FINAL')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_media/output/media_tratadas.xlsx'

copyfile(local_path, volume_path)
