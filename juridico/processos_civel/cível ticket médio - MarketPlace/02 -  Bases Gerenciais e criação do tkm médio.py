# Databricks notebook source
# MAGIC %md
# MAGIC #### Importação Bases Gerenciais Cível

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

dbutils.widgets.text("dt_info_produtos_elaw", "", "dt_info_produtos_elaw")
dbutils.widgets.text("mes_extracao", "", "mes_extracao")
dbutils.widgets.text("nmtabela", "", "nmtabela")

# COMMAND ----------

dt_info_produtos_elaw = dbutils.widgets.get("dt_info_produtos_elaw")
mes_extracao = dbutils.widgets.get("mes_extracao")


# COMMAND ----------

# Caminho das pastas e arquivos

nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/'
dir_info_produtos = '/Volumes/databox/juridico_comum/arquivos/cível/bases_info_produtos/'

#arquivo_ativos = f'CIVEL_GERENCIAL-(ATIVOS)-{nmtabela}.xlsx'
#arquivo_encerrados = f'CIVEL_GERENCIAL-(ENCERRADO)-{nmtabela}.xlsx'
#arquivo_encerrados_historico = f'CIVEL_ENCERRADOS_HISTORICO - {dt_historico}.xlsx'
arquivo_info_produtos = f'INFORMACOES_PRODUTOS_ATE_{dt_info_produtos_elaw}.xlsx'

#path_ativos = diretorio_origem + arquivo_ativos
#path_encerrados = diretorio_origem + arquivo_encerrados
#path_encerrados_historico = diretorio_origem + arquivo_encerrados_historico
path_info_produtos = dir_info_produtos + arquivo_info_produtos



# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# Carrega as planilhas em Spark Data Frames
gerencial_info_produtos = read_excel(path_info_produtos, "'INFO_PRODUTOS'!A6")


# COMMAND ----------

dataframes = [gerencial_info_produtos]  # Lista de DataFrames

# COMMAND ----------

# def convert_to_date_format_americano(df: DataFrame, columns: List[str]) -> DataFrame:
#     """
#     Converts columns in a PySpark DataFrame to date format.

#     Args:
#     - df: PySpark DataFrame
#     - columns: List of column names to convert to date format

#     Returns:
#     - PySpark DataFrame with specified columns converted to date format
#     """
#     for column in columns:
#         column_type = dict(df.dtypes)[column]
#         try:
#             if "timestamp" in column_type:
#                 df = df.withColumn(column, to_date(col(column), 'yyyy-MM-dd'))
#             else:
#                 df = df.withColumn(column, when(
#                                                 length(col(column)) == 10,
#                                                 to_date(col(column), 'MM/dd/yyyy')
#                                             ).otherwise(
#                                                 to_date(col(column), 'M/d/yy')
#                                             )
#                 )
#         except:
#             print(f"Column '{column}' does not exist. Skipping...")
#     return df

# COMMAND ----------

#gerencial_encerrados_civel_historico.printSchema()

# COMMAND ----------

# lista_col_data_americana = ['DATA DO FATO GERADOR79','DATA DO FATO GERADOR81', 'DATA DO FATO GERADOR82', 'DATA FATO GERADOR']
# gerencial_ativos_civel = convert_to_date_format_americano(gerencial_ativos_civel,lista_col_data_americana)
# gerencial_encerrados_civel = convert_to_date_format_americano(gerencial_encerrados_civel,lista_col_data_americana)


# COMMAND ----------

# lista_col_data_americana = ['DATA DO FATO GERADOR79', 'DATA DO FATO GERADOR81', 'DATA DO FATO GERADOR82', 'DATA FATO GERADOR']
# gerencial_encerrados_civel_historico = convert_to_date_format_americano(gerencial_encerrados_civel_historico,lista_col_data_americana)

# COMMAND ----------

# # Lista as colunas com datas
# ativos_data_cols = find_columns_with_word(gerencial_ativos_civel, 'DATA ')
# ativos_data_cols.append('DISTRIBUIÇÃO')

# encerrados_data_cols = find_columns_with_word(gerencial_encerrados_civel, 'DATA ')
# encerrados_data_cols.append('DISTRIBUIÇÃO')

# encerrados_historico_data_cols = find_columns_with_word(gerencial_encerrados_civel_historico, 'DATA ')
# encerrados_historico_data_cols.append('DISTRIBUIÇÃO')

# print("ativos_data_cols")
# print(ativos_data_cols)
# print("\n")
# print("encerrados_data_cols")
# print(encerrados_data_cols)
# print("\n")
# print("encerrados_historico_data_cols")
# print(encerrados_historico_data_cols)

# COMMAND ----------

# gerencial_ativos_civel = convert_to_date_format(gerencial_ativos_civel, ativos_data_cols)
# gerencial_encerrados_civel = convert_to_date_format(gerencial_encerrados_civel, encerrados_data_cols)
# gerencial_encerrados_civel_historico = convert_to_date_format(gerencial_encerrados_civel_historico, encerrados_historico_data_cols)

# COMMAND ----------

# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DA COMPRA", "DATA_DA_COMPRA")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DO PEDIDO", "DATA_DO_PEDIDO")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DO FATO GERADOR79", "DATA_FATO_GERADOR_1")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA FATO GERADOR", "DATA_FATO_GERADOR_2")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DO FATO GERADOR81", "DATA_FATO_GERADOR_3")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DO FATO GERADOR82", "DATA_FATO_GERADOR_4")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("CENTRO DE CUSTO / ÁREA DEMANDANTE - CÓDIGO", "CENTRO_DE_CUSTO_AREA_DEMANDANTE")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("COMPRA DIRETA OU MARKETPLACE? - NEW ", "DIRETA_OU_MKT")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("SUB-ÁREA DO DIREITO", "SUB_AREA_DO_DIREITO")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("ASSUNTO (CÍVEL) - PRINCIPAL", "ASSUNTO_CIVEL_PRI")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("ASSUNTO (CÍVEL) - ASSUNTO", "ASSUNTO_CIVEL_ASS")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("PROCESSO - Nº. PEDIDO", "NUM_PEDIDO")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("AÇÃO", "ACAO")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("ESCRITÓRIO EXTERNO", "ESCRITORIO")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("RECLAMAÇÃO BLACK FRIDAY?", "RECLAMACAO_BLACK_FRIDAY")
# gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("FABRICANTE DO PRODUTO", "FABRICANTE")

# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DA COMPRA", "DATA_DA_COMPRA")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO PEDIDO", "DATA_DO_PEDIDO")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO FATO GERADOR79", "DATA_FATO_GERADOR_1")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA FATO GERADOR", "DATA_FATO_GERADOR_2")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO FATO GERADOR81", "DATA_FATO_GERADOR_3")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO FATO GERADOR82", "DATA_FATO_GERADOR_4")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("CENTRO DE CUSTO / ÁREA DEMANDANTE - CÓDIGO", "CENTRO_DE_CUSTO_AREA_DEMANDANTE")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("COMPRA DIRETA OU MARKETPLACE? - NEW ", "DIRETA_OU_MKT")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("SUB-ÁREA DO DIREITO", "SUB_AREA_DO_DIREITO")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("ASSUNTO (CÍVEL) - PRINCIPAL", "ASSUNTO_CIVEL_PRI")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("ASSUNTO (CÍVEL) - PRINCIPAL", "ASSUNTO_CIVEL_PRI")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("ASSUNTO (CÍVEL) - ASSUNTO", "ASSUNTO_CIVEL_ASS")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("PROCESSO - Nº. PEDIDO", "NUM_PEDIDO")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("AÇÃO", "ACAO")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("ESCRITÓRIO EXTERNO", "ESCRITORIO")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("RECLAMAÇÃO BLACK FRIDAY?", "RECLAMACAO_BLACK_FRIDAY")
# gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("FABRICANTE DO PRODUTO", "FABRICANTE")

# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DA COMPRA", "DATA_DA_COMPRA")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO PEDIDO", "DATA_DO_PEDIDO")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO FATO GERADOR79", "DATA_FATO_GERADOR_1")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA FATO GERADOR", "DATA_FATO_GERADOR_2")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO FATO GERADOR81", "DATA_FATO_GERADOR_3")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO FATO GERADOR82", "DATA_FATO_GERADOR_4")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("CENTRO DE CUSTO / ÁREA DEMANDANTE - CÓDIGO", "CENTRO_DE_CUSTO_AREA_DEMANDANTE")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("COMPRA DIRETA OU MARKETPLACE? - NEW ", "DIRETA_OU_MKT")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("SUB-ÁREA DO DIREITO", "SUB_AREA_DO_DIREITO")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("ASSUNTO (CÍVEL) - PRINCIPAL", "ASSUNTO_CIVEL_PRI")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("ASSUNTO (CÍVEL) - ASSUNTO", "ASSUNTO_CIVEL_ASS")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("PROCESSO - Nº. PEDIDO", "NUM_PEDIDO")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("AÇÃO", "ACAO")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("ESCRITÓRIO EXTERNO", "ESCRITORIO")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("RECLAMAÇÃO BLACK FRIDAY?", "RECLAMACAO_BLACK_FRIDAY")
# gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("FABRICANTE DO PRODUTO", "FABRICANTE")

gerencial_info_produtos = gerencial_info_produtos.withColumnRenamed("Estado", "Estado2")


# COMMAND ----------

gerencial_info_produtos = adjust_column_names(gerencial_info_produtos)

gerencial_info_produtos.createOrReplaceTempView('gerencial_info_produtos_original')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criação da base de alçadas para ACORDO do Cível

# COMMAND ----------

gerencial_info_produtos_alcadas = gerencial_info_produtos.select(
    'PROCESSO_ID',
    'ÁREA_DO_DIREITO',
    'SUB_ÁREA_DO_DIREITO',
    'ASSUNTO_CÍVEL_PRINCIPAL',
    'ASSUNTO_CÍVEL_ASSUNTO',
    'STATUS',
    'VALOR_DA_CAUSA',
    'ESTADO',
    'CÍVEL_MASSA_RESPONSABILIDADE',
    'SKU_DO_PRODUTO',
    'PRODUTO',
    'FABRICANTE_DO_PRODUTO',
    'VALOR_DO_PRODUTO',
    'DATA_REGISTRADO',
    'EMPRESA'
)



# COMMAND ----------

from pyspark.sql.functions import col

dt_info_produtos_elaw = dbutils.widgets.get("dt_info_produtos_elaw")
ano = int(dt_info_produtos_elaw[:4])
ano_inicio = ano - 2

gerencial_info_produtos_alcadas = gerencial_info_produtos_alcadas.filter(col('DATA_REGISTRADO') >= f'{ano_inicio}-01-01')

gerencial_info_produtos_alcadas.createOrReplaceTempView('gerencial_info_produtos_alcadas')

# COMMAND ----------

df_info_produtos_alcadas_agrupa_produto = spark.sql('''
SELECT 
    PROCESSO_ID
    ,MAX(VALOR_DA_CAUSA) AS VALOR_DA_CAUSA
    ,SUM(coalesce(VALOR_DO_PRODUTO, 0)) AS TOTAL_VALOR_PRODUTO
FROM gerencial_info_produtos_alcadas
GROUP BY PROCESSO_ID
''')

df_info_produtos_alcadas_agrupa_produto.createOrReplaceTempView('df_info_produtos_alcadas_agrupa_produto')


# COMMAND ----------

df_info_produtos_alcadas_agrupa_produto_e_pgto = spark.sql('''
    SELECT 
    A.PROCESSO_ID,
    A.VALOR_DA_CAUSA,
    A.TOTAL_VALOR_PRODUTO,
    B.DT_ULT_PGTO,
    coalesce(B.ACORDO,0) + coalesce(B.CONDENACAO,0) + coalesce(B.PENHORA,0) + coalesce(B.PENSAO,0) + coalesce(B.IMPOSTO,0) + coalesce(B.GARANTIA,0) AS TOTAL_PAGAMENTOS
    FROM df_info_produtos_alcadas_agrupa_produto A 
    LEFT JOIN databox.juridico_comum.hist_pagamentos_garantias_civel_com_custas B
    ON A.PROCESSO_ID = B.PROCESSO_ID
''')

df_info_produtos_alcadas_agrupa_produto_e_pgto.createOrReplaceTempView('df_info_produtos_alcadas_agrupa_produto_e_pgto')

# COMMAND ----------

df_info_produtos_alcadas_agrupada_1 = spark.sql('''
    SELECT 
        A.PROCESSO_ID,
        A.VALOR_DA_CAUSA,
        A.TOTAL_VALOR_PRODUTO,
        A.DT_ULT_PGTO,
        A.TOTAL_PAGAMENTOS,
        B.`ÁREA_DO_DIREITO`,
        B.`SUB_ÁREA_DO_DIREITO`,
        B.`ASSUNTO_CÍVEL_PRINCIPAL`,
        B.`ASSUNTO_CÍVEL_ASSUNTO`,
        B.STATUS,
        B.ESTADO,
        B.`CÍVEL_MASSA_RESPONSABILIDADE`, 
        B.DATA_REGISTRADO, 
        B.EMPRESA
    FROM df_info_produtos_alcadas_agrupa_produto_e_pgto A
    LEFT JOIN gerencial_info_produtos_alcadas B
    ON A.PROCESSO_ID = B.PROCESSO_ID
''')

df_info_produtos_alcadas_agrupada_1.createOrReplaceTempView('df_info_produtos_alcadas_agrupada_1')


df_gerencial_info_produtos_alcadas_agrupada = spark.sql('''
    SELECT DISTINCT * FROM df_info_produtos_alcadas_agrupada_1
''')

df_gerencial_info_produtos_alcadas_agrupada.createOrReplaceTempView('df_gerencial_info_produtos_alcadas_agrupada')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criação da base de alçadas para ACORDO do Cível MASSA

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa = spark.sql('''
    SELECT *
    ,CONCAT(TRIM(`ASSUNTO_CÍVEL_PRINCIPAL`), '_' ,TRIM(`ASSUNTO_CÍVEL_ASSUNTO`), '_' , TRIM(`ESTADO`)) AS ASSUNTO_UNIFICADO
    ,CASE 
    WHEN TOTAL_VALOR_PRODUTO <= 500 THEN '0A500'
    WHEN TOTAL_VALOR_PRODUTO <= 1000 THEN '501A1000'
    WHEN TOTAL_VALOR_PRODUTO <= 1500 THEN '1001A1500'
    WHEN TOTAL_VALOR_PRODUTO <= 2000 THEN '1501A2000'
    WHEN TOTAL_VALOR_PRODUTO <= 3000 THEN '2001A3000'
    ELSE 'ACIMA3000' END AS TIPO_ALCADA
    ,'GLOBAL' AS MARK
    FROM df_gerencial_info_produtos_alcadas_agrupada
    WHERE `ÁREA_DO_DIREITO` = 'CÍVEL MASSA'
''')

gerencial_info_produtos_alcadas_agrupada_massa.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_massa')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkm_produto = spark.sql('''
    select 
        ASSUNTO_UNIFICADO
        ,AVG(TOTAL_VALOR_PRODUTO) AS TOTAL_VALOR_PRODUTO
    from gerencial_info_produtos_alcadas_agrupada_massa
    where STATUS = 'ENCERRADO'
    GROUP BY ALL
''')

gerencial_info_produtos_alcadas_agrupada_massa_tkm_produto.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_massa_tkm_produto')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa = spark.sql('''
    SELECT ASSUNTO_UNIFICADO
    ,AVG(TOTAL_VALOR_PRODUTO) AS TKM_PRODUTO_ASSUNTO_UNI
    ,AVG(TOTAL_PAGAMENTOS) AS TKM_PAGAMENTOS_ASSUNTO_UNI
    ,AVG(VALOR_DA_CAUSA) AS TKM_VALOR_CAUSA_ASSUNTO_UNI
    FROM gerencial_info_produtos_alcadas_agrupada_massa
    WHERE STATUS = 'ENCERRADO'
    GROUP BY ASSUNTO_UNIFICADO
''')

gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa_1 = spark.sql('''
    SELECT `ASSUNTO_CÍVEL_PRINCIPAL`
    ,AVG(TOTAL_VALOR_PRODUTO) AS TKM_PRODUTOS_ASSUNTO
    ,AVG(TOTAL_PAGAMENTOS) AS TKM_PAGAMENTOS_ASSUNTO
    ,AVG(VALOR_DA_CAUSA) AS TKM_VALOR_CAUSA_ASSUNTO
    FROM gerencial_info_produtos_alcadas_agrupada_massa
    WHERE STATUS = 'ENCERRADO'
    GROUP BY `ASSUNTO_CÍVEL_PRINCIPAL`
''')
gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa_1.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa_1')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa_global = spark.sql('''
 SELECT MARK
,SUM(TOTAL_VALOR_PRODUTO) / COUNT(PROCESSO_ID) AS TKM_PRODUTOS_ASSUNTO_GLOBAL
,SUM(TOTAL_PAGAMENTOS) / COUNT(PROCESSO_ID) AS TKM_PAGAMENTOS_ASSUNTO_GLOBAL
,SUM(VALOR_DA_CAUSA) / COUNT(PROCESSO_ID) AS TKM_VALOR_CAUSA_ASSUNTO_GLOBAL
FROM gerencial_info_produtos_alcadas_agrupada_massa
WHERE STATUS = 'ENCERRADO'
GROUP BY ALL
''')

gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa_global.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa_global')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkms_final = spark.sql('''
    SELECT A.*
    ,B.TOTAL_VALOR_PRODUTO AS TKM_VALOR_PRODUTO_UNI
    ,C.TKM_PAGAMENTOS_ASSUNTO_UNI
    ,C.TKM_VALOR_CAUSA_ASSUNTO_UNI
    ,D.TKM_PRODUTOS_ASSUNTO
    ,D.TKM_PAGAMENTOS_ASSUNTO
    ,D.TKM_VALOR_CAUSA_ASSUNTO
    ,E.TKM_PRODUTOS_ASSUNTO_GLOBAL
    ,E.TKM_PAGAMENTOS_ASSUNTO_GLOBAL 
    ,E.TKM_VALOR_CAUSA_ASSUNTO_GLOBAL
    FROM gerencial_info_produtos_alcadas_agrupada_massa A 
    LEFT JOIN gerencial_info_produtos_alcadas_agrupada_massa_tkm_produto B
    ON A.ASSUNTO_UNIFICADO = B.ASSUNTO_UNIFICADO
    LEFT JOIN gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa C 
    ON A.ASSUNTO_UNIFICADO = C.ASSUNTO_UNIFICADO
    LEFT JOIN gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa_1 D
    ON A.`ASSUNTO_CÍVEL_PRINCIPAL` = D.`ASSUNTO_CÍVEL_PRINCIPAL`
    LEFT JOIN gerencial_info_produtos_alcadas_agrupada_massa_tkm_pagamentos_e_causa_global E
    ON A.MARK = E.MARK
''')

gerencial_info_produtos_alcadas_agrupada_massa_tkms_final.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_massa_tkms_final')


# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_f = spark.sql('''

SELECT DISTINCT *
    ,ROW_NUMBER() OVER (PARTITION BY PROCESSO_ID ORDER BY (SELECT NULL)) AS RN
 FROM gerencial_info_produtos_alcadas_agrupada_massa_tkms_final
''')

gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_f.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_massa_tkms_final')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_excel = spark.sql('''
    select distinct * from gerencial_info_produtos_alcadas_agrupada_massa_tkms_final
    WHERE RN = 1
''')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_f = gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_excel.select(
    'PROCESSO_ID'
    ,'VALOR_DA_CAUSA'
    ,'TOTAL_VALOR_PRODUTO'
    ,'DT_ULT_PGTO'
    ,'TOTAL_PAGAMENTOS'
    ,'ÁREA_DO_DIREITO'
    ,'SUB_ÁREA_DO_DIREITO'
    ,'ASSUNTO_CÍVEL_PRINCIPAL'
    ,'ASSUNTO_CÍVEL_ASSUNTO'
    ,'STATUS'
    ,'ESTADO'
    ,'CÍVEL_MASSA_RESPONSABILIDADE'
    ,'DATA_REGISTRADO'
    ,'ASSUNTO_UNIFICADO'
    ,'TIPO_ALCADA'
    ,'TKM_VALOR_PRODUTO_UNI'
    ,'TKM_PAGAMENTOS_ASSUNTO_UNI'
    ,'TKM_VALOR_CAUSA_ASSUNTO_UNI'
    ,'TKM_PRODUTOS_ASSUNTO'
    ,'TKM_PAGAMENTOS_ASSUNTO'
    ,'TKM_VALOR_CAUSA_ASSUNTO'
    ,'TKM_PRODUTOS_ASSUNTO_GLOBAL'
    ,'TKM_PAGAMENTOS_ASSUNTO_GLOBAL'
    ,'TKM_VALOR_CAUSA_ASSUNTO_GLOBAL'
    )

gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_f = gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_f.fillna(0)

gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_f.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_f')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_ff = spark.sql('''

SELECT *,
  CASE WHEN GREATEST(TKM_VALOR_PRODUTO_UNI, TKM_PAGAMENTOS_ASSUNTO_UNI) <=1 THEN GREATEST(TKM_PRODUTOS_ASSUNTO_GLOBAL, TKM_PAGAMENTOS_ASSUNTO_GLOBAL) 
    WHEN GREATEST(TKM_VALOR_PRODUTO_UNI, TKM_PAGAMENTOS_ASSUNTO_UNI) <= GREATEST(TKM_PRODUTOS_ASSUNTO_GLOBAL, TKM_PAGAMENTOS_ASSUNTO_GLOBAL) THEN GREATEST(TKM_PRODUTOS_ASSUNTO_GLOBAL, TKM_PAGAMENTOS_ASSUNTO_GLOBAL)
  ELSE
  GREATEST(TKM_VALOR_PRODUTO_UNI, TKM_PAGAMENTOS_ASSUNTO_UNI) END AS ALCADA

 FROM gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_f
 ''')

 

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_massa_tkms_excel = gerencial_info_produtos_alcadas_agrupada_massa_tkms_final_ff.select(
    'PROCESSO_ID',
    'SUB_ÁREA_DO_DIREITO',
    'ASSUNTO_CÍVEL_PRINCIPAL',
    'ASSUNTO_CÍVEL_ASSUNTO',
    'ESTADO',
    'ALCADA',
    'STATUS',
    'TKM_VALOR_PRODUTO_UNI',
    'TKM_PAGAMENTOS_ASSUNTO_UNI',
    'TKM_PRODUTOS_ASSUNTO_GLOBAL',
    'TKM_PAGAMENTOS_ASSUNTO_GLOBAL'
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criação da base de alçadas para ACORDO do Cível ESTRATÉGICO

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_estrategico = spark.sql('''
    SELECT *
    ,CONCAT(TRIM(`ASSUNTO_CÍVEL_PRINCIPAL`), '_' ,TRIM(`ASSUNTO_CÍVEL_ASSUNTO`), '_' , TRIM('ESTADO')) AS ASSUNTO_UNIFICADO
    ,CASE 
    WHEN VALOR_DA_CAUSA <= 50000 THEN '0A50000'
    WHEN VALOR_DA_CAUSA <= 100000 THEN '50001A100000'
    WHEN VALOR_DA_CAUSA <= 200000 THEN '100001A200000'
    WHEN VALOR_DA_CAUSA <= 300000 THEN '200001A300000'
    WHEN VALOR_DA_CAUSA <= 500000 THEN '300001A500000'
    WHEN VALOR_DA_CAUSA <= 999999 THEN '500001A999999'
    ELSE 'ACIMA999999' END AS TIPO_ALCADA
    ,'GLOBAL' AS MARK
    FROM df_gerencial_info_produtos_alcadas_agrupada
    WHERE `ÁREA_DO_DIREITO` = 'CÍVEL ESTRATÉGICO'
''')

gerencial_info_produtos_alcadas_agrupada_estrategico.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_estrategico')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_produto = spark.sql('''
    select 
        TIPO_ALCADA
        ,ASSUNTO_UNIFICADO
        ,AVG(TOTAL_VALOR_PRODUTO) AS TOTAL_VALOR_PRODUTO
        ,AVG(VALOR_DA_CAUSA) AS TOTAL_VALOR_CAUSA
    from gerencial_info_produtos_alcadas_agrupada_estrategico
    where STATUS = 'ENCERRADO'
    GROUP BY ALL
''')

gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_produto.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_produto')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa = spark.sql('''
    SELECT ASSUNTO_UNIFICADO
    ,AVG(TOTAL_PAGAMENTOS) AS TKM_PAGAMENTOS_ASSUNTO_UNI
    ,AVG(VALOR_DA_CAUSA) AS TKM_VALOR_CAUSA_ASSUNTO_UNI
    FROM gerencial_info_produtos_alcadas_agrupada_estrategico
    WHERE STATUS = 'ENCERRADO'
    GROUP BY ASSUNTO_UNIFICADO
''')

gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa_1 = spark.sql('''
    SELECT `ASSUNTO_CÍVEL_PRINCIPAL`
    ,AVG(TOTAL_PAGAMENTOS) AS TKM_PAGAMENTOS_ASSUNTO
    ,AVG(VALOR_DA_CAUSA) AS TKM_VALOR_CAUSA_ASSUNTO
    FROM gerencial_info_produtos_alcadas_agrupada_estrategico
    WHERE STATUS = 'ENCERRADO'
    GROUP BY `ASSUNTO_CÍVEL_PRINCIPAL`
''')
gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa_1.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa_1')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa_global = spark.sql('''
 SELECT MARK
,SUM(TOTAL_PAGAMENTOS) / COUNT(PROCESSO_ID) AS TKM_PAGAMENTOS_ASSUNTO
,SUM(VALOR_DA_CAUSA) / COUNT(PROCESSO_ID) AS TKM_VALOR_CAUSA_ASSUNTO
FROM gerencial_info_produtos_alcadas_agrupada_estrategico
WHERE STATUS = 'ENCERRADO'
GROUP BY ALL
''')

gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa_global.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa_global')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final = spark.sql('''
    SELECT A.*
    ,B.TOTAL_VALOR_PRODUTO AS TKM_VALOR_PRODUTO
    ,C.TKM_PAGAMENTOS_ASSUNTO_UNI
    ,C.TKM_VALOR_CAUSA_ASSUNTO_UNI
    ,D.TKM_PAGAMENTOS_ASSUNTO
    ,D.TKM_VALOR_CAUSA_ASSUNTO
    ,E.TKM_PAGAMENTOS_ASSUNTO AS TKM_PAGAMENTOS_ASSUNTO_GLOBAL
    ,E.TKM_VALOR_CAUSA_ASSUNTO AS TKM_VALOR_CAUSA_ASSUNTO_GLOBAL
    FROM gerencial_info_produtos_alcadas_agrupada_estrategico A 
    LEFT JOIN gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_produto B
    ON A.TIPO_ALCADA = B.TIPO_ALCADA
    LEFT JOIN gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa C 
    ON A.ASSUNTO_UNIFICADO = C.ASSUNTO_UNIFICADO
    LEFT JOIN gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa_1 D
    ON A.`ASSUNTO_CÍVEL_PRINCIPAL` = D.`ASSUNTO_CÍVEL_PRINCIPAL`
    LEFT JOIN gerencial_info_produtos_alcadas_agrupada_estrategico_tkm_pagamentos_e_causa_global E
    ON A.MARK = E.MARK
''')

gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final')


# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final_f = spark.sql('''

SELECT DISTINCT *
    ,ROW_NUMBER() OVER (PARTITION BY PROCESSO_ID ORDER BY (SELECT NULL)) AS RN
 FROM gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final
''')

gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final_f.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final_excel = spark.sql('''
    select distinct * from gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final
    WHERE RN = 1
''')

# COMMAND ----------

gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final_f = gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final_excel.select(
    'PROCESSO_ID'
    ,'VALOR_DA_CAUSA'
    ,'TOTAL_VALOR_PRODUTO'
    ,'DT_ULT_PGTO'
    ,'TOTAL_PAGAMENTOS'
    ,'ÁREA_DO_DIREITO'
    ,'SUB_ÁREA_DO_DIREITO'
    ,'ASSUNTO_CÍVEL_PRINCIPAL'
    ,'ASSUNTO_CÍVEL_ASSUNTO'
    ,'STATUS'
    ,'ESTADO'
    ,'CÍVEL_MASSA_RESPONSABILIDADE'
    ,'DATA_REGISTRADO'
    ,'ASSUNTO_UNIFICADO'
    ,'TIPO_ALCADA'
    ,'TKM_VALOR_PRODUTO'
    ,'TKM_PAGAMENTOS_ASSUNTO_UNI'
    ,'TKM_VALOR_CAUSA_ASSUNTO_UNI'
    ,'TKM_PAGAMENTOS_ASSUNTO'
    ,'TKM_VALOR_CAUSA_ASSUNTO'
    ,'TKM_PAGAMENTOS_ASSUNTO_GLOBAL'
    ,'TKM_VALOR_CAUSA_ASSUNTO_GLOBAL'
    ,'EMPRESA'
    )


gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final_f.createOrReplaceTempView('gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final_f')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gerencial info produtos para base do Marktplace

# COMMAND ----------

c = gerencial_info_produtos.columns 

for cc in c:
    print(cc)

# COMMAND ----------

gerencial_info_produtos = gerencial_info_produtos.select(
    'PROCESSO_ID'
    ,'STATUS'
    ,'ÁREA_DO_DIREITO'
    ,'ASSUNTO_CÍVEL_ASSUNTO'
    ,'DATA_REGISTRADO'
    ,'CENTRO_DE_CUSTO_ÁREA_DEMANDANTE_CÓDIGO'
    ,'ESTADO'
    ,'NÚMERO_DO_PROCESSO'
    ,'COMPRA_DIRETA_OU_MARKETPLACE_NEW'
    ,'CÍVEL_MASSA_RESPONSABILIDADE'
    ,'NÚMERO_DO_PEDIDO'
    ,'ID_LOJISTA'
    ,'LOJISTA'
    ,'MOTIVO_DE_ENCERRAMENTO'
)

# COMMAND ----------

gerencial_info_produtos = gerencial_info_produtos.fillna('')

# COMMAND ----------

gerencial_info_produtos.createOrReplaceTempView('gerencial_info_produtos')

# COMMAND ----------

gerencial_info_produtos_f = spark.sql('''
    SELECT *
    ,CASE WHEN (`ASSUNTO_CÍVEL_ASSUNTO` <> '' AND LOJISTA <> '' AND ESTADO <> '') THEN CONCAT(`ASSUNTO_CÍVEL_ASSUNTO`,LOJISTA,ESTADO) ELSE '' END AS ASSUNTO_LOJA_UF
    ,CASE WHEN (`ASSUNTO_CÍVEL_ASSUNTO` <> '' AND ESTADO <> '') THEN CONCAT(`ASSUNTO_CÍVEL_ASSUNTO`,ESTADO) ELSE '' END AS ASSUNTO_UF
    ,CONCAT(`ASSUNTO_CÍVEL_ASSUNTO`) AS ASSUNTO
    FROM gerencial_info_produtos
''')

gerencial_info_produtos_f.createOrReplaceTempView('gerencial_info_produtos_f')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gerencial info produtos com pagamentos

# COMMAND ----------

gerencial_info_produtos_com_pgto = spark.sql('''
    SELECT A.*,
    coalesce(B.ACORDO, 0) AS ACORDO
    ,coalesce(B.CONDENACAO, 0) AS CONDENACAO
    ,coalesce(B.CUSTAS, 0) AS CUSTAS
    ,coalesce(B.PENHORA, 0) AS PENHORA
    ,coalesce(B.PENSAO, 0) AS PENSAO
    ,coalesce(B.OUTROS_PAGAMENTOS, 0) AS OUTROS_PAGAMENTOS
    ,coalesce(B.IMPOSTO, 0) AS IMPOSTO
    ,coalesce(B.GARANTIA, 0) AS GARANTIA
    ,coalesce(B.TOTAL_PAGAMENTOS, 0) AS TOTAL_PAGAMENTOS
    ,coalesce(B.CONDENACAO, 0) + coalesce(B.CUSTAS, 0) AS TOTAL_CONDENACAO_CUSTAS
    FROM gerencial_info_produtos_f A
    LEFT JOIN databox.juridico_comum.hist_pagamentos_garantias_civel_com_custas B 
    ON A.PROCESSO_ID = B.PROCESSO_ID
''')

gerencial_info_produtos_com_pgto.createOrReplaceGlobalTempView('gerencial_info_produtos_com_pgto')

# COMMAND ----------

gerencial_info_produtos_com_pgto_ = spark.sql('''
    SELECT *,
    CASE WHEN STATUS = 'ENCERRADO' THEN CASE
        WHEN ACORDO > 1 THEN 'ACORDO'
        WHEN (
            CONDENACAO  +PENHORA  +OUTROS_PAGAMENTOS+IMPOSTO + GARANTIA
        ) > 1 THEN 'CONDENACAO'
        WHEN (
            ACORDO + CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA
        ) <= 1 THEN 'SEM ONUS'
        END
    END AS MOTIVO_ENC_AGRP

    FROM global_temp.gerencial_info_produtos_com_pgto
''') 

gerencial_info_produtos_com_pgto_.createOrReplaceGlobalTempView('gerencial_info_produtos_com_pgto')

# COMMAND ----------

df_assunto_loja_uf = spark.sql('''
    SELECT
    ASSUNTO_LOJA_UF
    ,COUNT(PROCESSO_ID) AS ASSUNTO_LOJA_UF_QTD
    ,SUM(COALESCE(TOTAL_CONDENACAO_CUSTAS, 0)) AS ASSUNTO_LOJA_UF_VALOR
    FROM global_temp.gerencial_info_produtos_com_pgto
    WHERE STATUS = 'ENCERRADO' AND MOTIVO_ENC_AGRP = 'CONDENACAO'
    AND `COMPRA_DIRETA_OU_MARKETPLACE_NEW` = 'MARKETPLACE'
    GROUP BY 1
''')

df_assunto_uf = spark.sql('''
    SELECT
    ASSUNTO_UF
    ,COUNT(PROCESSO_ID) AS ASSUNTO_UF_QTD
    ,SUM(COALESCE(TOTAL_CONDENACAO_CUSTAS, 0)) AS ASSUNTO_UF_VALOR
    FROM global_temp.gerencial_info_produtos_com_pgto
    WHERE STATUS = 'ENCERRADO' AND MOTIVO_ENC_AGRP = 'CONDENACAO'
    AND `COMPRA_DIRETA_OU_MARKETPLACE_NEW` = 'MARKETPLACE'
    GROUP BY 1
''')

df_assunto = spark.sql('''
    SELECT
    ASSUNTO
    ,COUNT(PROCESSO_ID) AS ASSUNTO_QTD
    ,SUM(COALESCE(TOTAL_CONDENACAO_CUSTAS, 0)) AS ASSUNTO_VALOR
    FROM global_temp.gerencial_info_produtos_com_pgto
    WHERE STATUS = 'ENCERRADO' AND MOTIVO_ENC_AGRP = 'CONDENACAO'
    AND `COMPRA_DIRETA_OU_MARKETPLACE_NEW` = 'MARKETPLACE'
    GROUP BY 1
''')

df_assunto_global = spark.sql('''
    SELECT
    ASSUNTO
    ,COUNT(PROCESSO_ID) AS ASSUNTO_QTD
    ,SUM(COALESCE(TOTAL_CONDENACAO_CUSTAS, 0)) AS ASSUNTO_VALOR
    FROM global_temp.gerencial_info_produtos_com_pgto
    WHERE STATUS = 'ENCERRADO' AND MOTIVO_ENC_AGRP = 'CONDENACAO'
    GROUP BY 1
''')

df_tkm_sem_assunto = spark.sql('''
    SELECT
    'GLOBAL' AS ASSUNTO
    ,COUNT(PROCESSO_ID) AS ASSUNTO_GLOBAL_QTD
    ,SUM(COALESCE(TOTAL_CONDENACAO_CUSTAS, 0)) AS ASSUNTO_GLOBAL_VALOR
    FROM global_temp.gerencial_info_produtos_com_pgto
    WHERE STATUS = 'ENCERRADO' AND MOTIVO_ENC_AGRP = 'CONDENACAO'
    GROUP BY 1
''')

df_assunto_loja_uf.createOrReplaceTempView('df_assunto_loja_uf')
df_assunto_uf.createOrReplaceTempView('df_assunto_uf')
df_assunto.createOrReplaceTempView('df_assunto')
df_assunto_global.createOrReplaceTempView('df_assunto_global')
df_tkm_sem_assunto.createOrReplaceTempView('df_tkm_sem_assunto')


# COMMAND ----------

from datetime import datetime
import calendar

mes_extracao = dbutils.widgets.get("mes_extracao")
inicio_mes = mes_extracao + '01'

# Converter para string e criar um objeto datetime
data_convertida = datetime.strptime(str(inicio_mes), "%Y%m%d")

# Obter o último dia do mês
ultimo_dia_mes = calendar.monthrange(data_convertida.year, data_convertida.month)[1]
final_do_mes = datetime(data_convertida.year, data_convertida.month, ultimo_dia_mes)

inicio_mes_str = "'" + data_convertida.strftime("%Y-%m-%d") + "'" 
final_do_mes_str = "'" + final_do_mes.strftime("%Y-%m-%d") + "'" 

print(final_do_mes_str,"\n\n\n\n\n\n")

# COMMAND ----------

df_entradas_mkt = spark.sql(f'''
    SELECT A.PROCESSO_ID
    ,A.`ÁREA_DO_DIREITO`
    ,A.`ASSUNTO_CÍVEL_ASSUNTO`
    ,A.DATA_REGISTRADO
    ,A.`CENTRO_DE_CUSTO_ÁREA_DEMANDANTE_CÓDIGO`
    ,A.ESTADO
    ,A.`NÚMERO_DO_PROCESSO`
    ,A.`COMPRA_DIRETA_OU_MARKETPLACE_NEW`
    ,A.`CÍVEL_MASSA_RESPONSABILIDADE`
    ,A.`NÚMERO_DO_PEDIDO`
    ,A.ID_LOJISTA
    ,A.LOJISTA
    ,A.ASSUNTO_LOJA_UF
    ,B.ASSUNTO_LOJA_UF_QTD
    ,B.ASSUNTO_LOJA_UF_VALOR
    ,A.ASSUNTO_UF
    ,C.ASSUNTO_UF_QTD
    ,coalesce(C.ASSUNTO_UF_VALOR, 0) as ASSUNTO_UF_VALOR
    ,A.ASSUNTO
    ,D.ASSUNTO_QTD
    ,coalesce(D.ASSUNTO_VALOR, 0) as ASSUNTO_VALOR
    ,E.ASSUNTO AS ASSUNTO_GLOBAL
    ,E.ASSUNTO_QTD AS ASSUNTO_GLOBAL_QTD
    ,coalesce(E.ASSUNTO_VALOR, 0) AS ASSUNTO_GLOBAL_VALOR
    FROM global_temp.gerencial_info_produtos_com_pgto A
    LEFT JOIN df_assunto_loja_uf B
    ON A.ASSUNTO_LOJA_UF = B.ASSUNTO_LOJA_UF
    LEFT JOIN df_assunto_uf C
    ON A.ASSUNTO_UF = C.ASSUNTO_UF
    LEFT JOIN df_assunto D
    ON A.ASSUNTO = D.ASSUNTO
    LEFT JOIN df_assunto_global E
    ON A.ASSUNTO = E.ASSUNTO
    WHERE A.DATA_REGISTRADO BETWEEN {inicio_mes_str} AND {final_do_mes_str} AND A.`COMPRA_DIRETA_OU_MARKETPLACE_NEW` = 'MARKETPLACE'
''')

df_entradas_mkt = df_entradas_mkt.fillna(0)

df_entradas_mkt.createOrReplaceTempView('df_entradas_mkt')

# COMMAND ----------

df_entradas_mkt_tkm = spark.sql('''
    SELECT 
        PROCESSO_ID
        ,`ÁREA_DO_DIREITO`
        ,`ASSUNTO_CÍVEL_ASSUNTO`
        ,DATA_REGISTRADO
        ,`CENTRO_DE_CUSTO_ÁREA_DEMANDANTE_CÓDIGO`
        ,ESTADO
        ,`NÚMERO_DO_PROCESSO`
        ,`COMPRA_DIRETA_OU_MARKETPLACE_NEW`
        ,`CÍVEL_MASSA_RESPONSABILIDADE`
        ,`NÚMERO_DO_PEDIDO`
        ,ID_LOJISTA
        ,LOJISTA
        ,ASSUNTO_LOJA_UF
        ,ASSUNTO_LOJA_UF_QTD
        ,ASSUNTO_LOJA_UF_VALOR
        ,CASE WHEN ASSUNTO_LOJA_UF_QTD >= 15 THEN (ASSUNTO_LOJA_UF_VALOR / ASSUNTO_LOJA_UF_QTD) ELSE 0 END AS ASSUNTO_LOJA_UF_TKM
        ,ASSUNTO_UF
        ,ASSUNTO_UF_QTD
        ,ASSUNTO_UF_VALOR
        ,CASE WHEN ASSUNTO_LOJA_UF_QTD < 15 AND ASSUNTO_UF_QTD >= 15 THEN (ASSUNTO_UF_VALOR / ASSUNTO_UF_QTD) ELSE 0 END AS ASSUNTO_UF_TKM
        ,ASSUNTO
        ,ASSUNTO_QTD
        ,ASSUNTO_VALOR
        ,CASE WHEN ASSUNTO_LOJA_UF_QTD < 15 AND ASSUNTO_UF_QTD <15 THEN (ASSUNTO_VALOR / ASSUNTO_QTD)  
            WHEN ASSUNTO_LOJA_UF_QTD < 15 AND ASSUNTO_UF_QTD <15 AND ASSUNTO_QTD <= 0 THEN (ASSUNTO_GLOBAL_VALOR / ASSUNTO_GLOBAL_QTD)
            ELSE 0                
        END AS ASSUNTO_TKM
        FROM df_entradas_mkt
''')

df_entradas_mkt_tkm.createOrReplaceTempView('df_entradas_mkt_tkm')

# COMMAND ----------

df_entradas_mkt_tkm_f = spark.sql('''
    SELECT * 
        ,'GLOBAL' AS MARCADOR
        ,CASE
        WHEN ASSUNTO_LOJA_UF_TKM >0 THEN ASSUNTO_LOJA_UF_TKM
        WHEN ASSUNTO_UF_TKM >0 THEN ASSUNTO_UF_TKM
        WHEN ASSUNTO_TKM >0 THEN ASSUNTO_TKM
        ELSE 0
        END AS TKM_FINAL
    FROM df_entradas_mkt_tkm
''')

df_entradas_mkt_tkm_f = df_entradas_mkt_tkm_f.fillna(0)

df_entradas_mkt_tkm_f.createOrReplaceTempView('df_entradas_mkt_tkm_f')

# COMMAND ----------

df_entradas_mkt_tkm_final = spark.sql('''
SELECT A.PROCESSO_ID
    ,A.`ÁREA_DO_DIREITO`
    ,A.`ASSUNTO_CÍVEL_ASSUNTO`
    ,A.DATA_REGISTRADO
    ,A.`CENTRO_DE_CUSTO_ÁREA_DEMANDANTE_CÓDIGO`
    ,A.ESTADO
    ,A.`NÚMERO_DO_PROCESSO`
    ,A.`COMPRA_DIRETA_OU_MARKETPLACE_NEW`
    ,A.`NÚMERO_DO_PEDIDO`
    ,A.`ID_LOJISTA`
    ,A.LOJISTA
    ,A.ASSUNTO_LOJA_UF
    ,A.ASSUNTO_LOJA_UF_QTD
    ,A.ASSUNTO_LOJA_UF_VALOR
    ,A.ASSUNTO_LOJA_UF_TKM
    ,A.ASSUNTO_UF
    ,A.ASSUNTO_UF_QTD
    ,A.ASSUNTO_UF_VALOR
    ,A.ASSUNTO_UF_TKM
    ,A.ASSUNTO
    ,A.ASSUNTO_QTD
    ,A.ASSUNTO_VALOR
    ,A.ASSUNTO_TKM
    ,CASE WHEN A.TKM_FINAL <=0 THEN (B.ASSUNTO_GLOBAL_VALOR / B.ASSUNTO_GLOBAL_QTD) ELSE A.TKM_FINAL END AS TKM_FINAL
 FROM df_entradas_mkt_tkm_f A
 LEFT JOIN df_tkm_sem_assunto B
 ON A.MARCADOR = B.ASSUNTO
WHERE `CÍVEL_MASSA_RESPONSABILIDADE` NOT IN ('GRUPO CASAS BAHIA')
''')

df_entradas_mkt_tkm_final.createOrReplaceTempView('df_entradas_mkt_tkm_final')

# COMMAND ----------

df_entradas_mkt_tkm_final_dinstinct = spark.sql('''
    select distinct(*) from df_entradas_mkt_tkm_final
''')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exportação final dos arquivos

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile
import os

# Obtem o nome da gerencial utilizada para o nome do arquivo final
mes_extracao = dbutils.widgets.get("mes_extracao")

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_entradas_mkt_tkm_final_dinstinct.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = '/local_disk0/tmp/df_entradas_mkt_tkm_final.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='df_entradas_mkt_tkm_final')

# Definir o nome do arquivo
qtd = 0
path = '/Volumes/databox/juridico_comum/arquivos/cível/bases_info_produtos/output/'
list_files = dbutils.fs.ls(path)
nome_arquivo = f'ENTRADAS_MARKETPLACE_{mes_extracao}'

# # Essa rotina verifica quantos arquivos com o nome escolhido existem na pasta e faz um controle de versao com base na quantidade
# for file in list_files:
#     name = file.name
#     if nome_arquivo in name:
#         qtd += 1
#         print(f'{name} | {qtd} \n')

# if qtd == 0:
#     print(f'Ainda não foi criado nenhum arquivo com o nome: {nome_arquivo}')
# if qtd >= 1:
#     new_qtd = qtd +1
#     nome_arquivo = f'Novo Histórico {nmtabela} Tot Pgto_{new_qtd}'


# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/b                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                ases_info_produtos/output/{nome_arquivo}.xlsx'
copyfile(local_path, volume_path)

# COMMAND ----------

mes_extracao = dbutils.widgets.get("mes_extracao")

df_entradas_mkt_tkm_final_dinstinct.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.df_entradas_mkt_tkm_{mes_extracao}")

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile
import os

# Obtem o nome da gerencial utilizada para o nome do arquivo final
mes_extracao = dbutils.widgets.get("mes_extracao")

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = gerencial_info_produtos_alcadas_agrupada_massa_tkms_excel.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = '/local_disk0/tmp/df_civel_massa_tkm.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='df_civel_massa_tkm')

# Definir o nome do arquivo
qtd = 0
path = '/Volumes/databox/juridico_comum/arquivos/cível/bases_info_produtos/output/'
list_files = dbutils.fs.ls(path)
nome_arquivo = f'CIVEL_MASSA_TKM_{mes_extracao}'

# # Essa rotina verifica quantos arquivos com o nome escolhido existem na pasta e faz um controle de versao com base na quantidade
# for file in list_files:
#     name = file.name
#     if nome_arquivo in name:
#         qtd += 1
#         print(f'{name} | {qtd} \n')

# if qtd == 0:
#     print(f'Ainda não foi criado nenhum arquivo com o nome: {nome_arquivo}')
# if qtd >= 1:
#     new_qtd = qtd +1
#     nome_arquivo = f'Novo Histórico {nmtabela} Tot Pgto_{new_qtd}'


# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_info_produtos/output/{nome_arquivo}.xlsx'
copyfile(local_path, volume_path)

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile
import os

# Obtem o nome da gerencial utilizada para o nome do arquivo final
mes_extracao = dbutils.widgets.get("mes_extracao")

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = gerencial_info_produtos_alcadas_agrupada_estrategico_tkms_final_f.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = '/local_disk0/tmp/df_civel_estrategico_tkm.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='df_civel_estrategico_tkm')

# Definir o nome do arquivo
qtd = 0
path = '/Volumes/databox/juridico_comum/arquivos/cível/bases_info_produtos/output/'
list_files = dbutils.fs.ls(path)
nome_arquivo = f'CIVEL_ESTRATEGICO_TKM_{mes_extracao}'

# # Essa rotina verifica quantos arquivos com o nome escolhido existem na pasta e faz um controle de versao com base na quantidade
# for file in list_files:
#     name = file.name
#     if nome_arquivo in name:
#         qtd += 1
#         print(f'{name} | {qtd} \n')

# if qtd == 0:
#     print(f'Ainda não foi criado nenhum arquivo com o nome: {nome_arquivo}')
# if qtd >= 1:
#     new_qtd = qtd +1
#     nome_arquivo = f'Novo Histórico {nmtabela} Tot Pgto_{new_qtd}'


# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_info_produtos/output/{nome_arquivo}.xlsx'
copyfile(local_path, volume_path)

# COMMAND ----------

import traceback

print("Conluido, próximo!!!")
