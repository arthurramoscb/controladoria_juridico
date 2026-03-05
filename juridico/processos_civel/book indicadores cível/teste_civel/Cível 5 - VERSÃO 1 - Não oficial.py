# Databricks notebook source
# MAGIC %md
# MAGIC ####Consolidação dados Cível

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 - Carga e tratamentos iniciais

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Carrega histórico dos fechamentos financeiros
df_ff_civel = spark.sql("select * from databox.juridico_comum.tb_fech_civel_consolidado")

df_ff_civel = adjust_column_names(df_ff_civel)
df_ff_civel = remove_acentos(df_ff_civel)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Correcting the code by adding the missing closing parenthesis
tb_fech_civel_consolidado_1 = df_ff_civel.withColumn(
    "ESTRATEGIA",
    when(col("ESTRATEGIA").isin("ACO", "ACOR", "ACORD", "ACORDO"), "ACORDO")
    .when(col("ESTRATEGIA").isin("DEF", "DEFE", "DEFES", "DEFESA"), "DEFESA")
    .otherwise(col("ESTRATEGIA"))
)  # Added the missing closing parenthesis here

# COMMAND ----------

tb_fech_civel_consolidado_1  = tb_fech_civel_consolidado_1.withColumnRenamed('BU', "UNIDADE_DE_NEGOCIO")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Importa bases com informações adicionais

# COMMAND ----------

# IMPORTA A BASE COM O DE PARA DO BU
path_bu = '/Volumes/databox/juridico_comum/arquivos/cível/bases_auxiliares/De para Centro de Custo e BU_v2 - ago-2022.xlsx'
df_bu_civel = read_excel(path_bu)
df_bu_civel.createOrReplaceTempView("TB_DE_PARA_BU")


# IMPORTA A BASE COM O DE PARA DA ÁREA FUNCIONAL
path_funcional = '/Volumes/databox/juridico_comum/arquivos/cível/bases_auxiliares/Centro de Custo e Área Funcional_OKENN_280623.xlsx'
df_funcional_civel = read_excel(path_funcional, "'OKENN'!A1")
df_funcional_civel.createOrReplaceTempView("TB_DE_PARA_AREA_FUNCIONAL")

# COMMAND ----------

tb_fech_civel_consolidado_1.createOrReplaceTempView("TB_FECH_FIN_CIVEL_CONSOLIDADO")

tb_fech_civel_consolidado = spark.sql("""
SELECT A.*
    ,C.`descrição` AS DESCRICAO_CENTRO_CUSTO

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA'
        THEN 'BARTIRA' ELSE B.BU END) AS BU -- Coluna BU já exite em A.* 

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome2 END ) AS VP

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome3 END ) AS DIRETORIA 

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE UPPER(C.`nome responsavel`) END ) AS RESPONSAVEL_AREA 

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BART'
        ELSE C.`area funcional` END ) AS AREA_FUNCIONAL_BART

FROM TB_FECH_FIN_CIVEL_CONSOLIDADO AS A 
LEFT JOIN TB_DE_PARA_BU AS B ON A.CENTRO_DE_CUSTO_M = B.`Centro de custo`
LEFT JOIN TB_DE_PARA_AREA_FUNCIONAL AS C ON A.CENTRO_DE_CUSTO_M = C.centro
WHERE A.ID_PROCESSO IS NOT NULL 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 - Preparação da Base dos Indicadores Cível

# COMMAND ----------

#Colunas ordenadas do fechamento consolidado
schema_details = [(field.name, field.dataType, field.nullable) for field in tb_fech_civel_consolidado.schema.fields]

# Ordena os detalhes do esquema por nome de coluna
sorted_schema_details = sorted(schema_details, key=lambda x: x[0])

# Imprime o esquema ordenado
print("root")
for name, dataType, nullable in sorted_schema_details:
    print(f" |-- {name}: {dataType.simpleString()} (nullable = {nullable})")

# COMMAND ----------

df_ff_civel_consolidado = tb_fech_civel_consolidado.withColumnRenamed("%_EMPRESA_M", "EMPRESA_M_PERC") \
                        .withColumnRenamed("%_RISCO", "RISCO_PERC") \
                        .withColumnRenamed("%_SOCIO_M", "SOCIO_M_PERC") \
                        .withColumnRenamed("%_SOCIO_M_1", "SOCIO_M_1_PERC") \
                        .withColumnRenamed("%_EMPRESA_M_1", "EMPRESA_M_PERC_1") \
                        .withColumnRenamed("EMPRESA:_CORRECAO_M", "EMPRESA_CORRECAO_M")\
                        .withColumnRenamed("CENTRO_DE_CUSTO_M", "CENTRO_CUSTO_M") \
                        .withColumnRenamed("CENTRO_DE_CUSTO_M_1", "CENTRO_CUSTO_M_1") \
                        .withColumnRenamed("EMPRESA:_CLASSIFICACAO_MOV_M", "EMPRESA_CLASSIFICACAO_MOV_M") \
                        .withColumnRenamed("EMPRESA:_PROVISAO_MOV_M", "EMPRESA_PROVISAO_MOV_M") \
                        .withColumnRenamed("EMPRESA:_PROVISAO_MOV_TOTAL_M", "EMPRESA_PROVISAO_MOV_TOTAL_M") \
                        .withColumnRenamed("EMPRESA:_PROV_TOTAL_PASSIVO_M", "EMPRESA_PROV_TOTAL_PASSIVO_M")\
                        .withColumnRenamed("EMPRESA:_PROVISAO_M_1", "EMPRESA_PROVISAO_M_1")\
                        .withColumnRenamed("EMPRESA:_CORRECAO_MOV_M", "EMPRESA_CORRECAO_MOV_M") \
                        .withColumnRenamed("EMPRESA:_CORRECAO_M_1", "EMPRESA_CORRECAO_M_1") \
                        .withColumnRenamed("EMPRESA:_CORRECAO_M_1_1", "EMPRESA_CORRECAO_M_1_1")\
                        .withColumnRenamed("EMPRESA:_CORRECAO_M_1_0001", "EMPRESA_CORRECAO_M_1_0001")\
                        .withColumnRenamed("EMPRESA:_PROVISAO_TOTAL_M", "EMPRESA_PROVISAO_TOTAL_M")\
                        .withColumnRenamed("EMPRESA:_PROVISAO_TOTAL_M_1", "EMPRESA_PROVISAO_TOTAL_M_1")\
                        .withColumnRenamed("MEDIA_DE_PAGAMENTO", "MEDIA_PAGAMENTO") \
                        .withColumnRenamed("NO_PROCESSO", "NUM_PROCESSO") \
                        .withColumnRenamed("FASE_M", "FASE") \
                        .withColumnRenamed("VLR_CAUSA", "VALOR_DA_CAUSA")\
                        .withColumnRenamed("ESTRATEGIA", "INDICACAO_PROCESSO_ESTRATEGICO")\
                        .withColumnRenamed("DATACADASTRO", "CADASTRO")\
                        .withColumnRenamed("SOCIO:_CLASSIFICACAO_MOV_M", "SOCIO_CLASSIFICACAO_MOV_M")\
                        .withColumnRenamed("SOCIO:_CORRECAO_M", "SOCIO_CORRECAO_M")\
                        .withColumnRenamed("SOCIO:_CORRECAO_MOV_M", "SOCIO_CORRECAO_MOV_M")\
                        .withColumnRenamed("SOCIO:_CORRECAO_M_1", "SOCIO_CORRECAO_M_1")\
                        .withColumnRenamed("SOCIO:_CORRECAO_M_1_0001:", "SOCIO_CORRECAO_M_1_0001")\
                        .withColumnRenamed("SOCIO:_CORRECAO_M_1_1", "SOCIO_CORRECAO_M_1_1")\
                        .withColumnRenamed("SOCIO:_PROVISAO_MOV_M", "SOCIO_PROVISAO_MOV_M")\
                        .withColumnRenamed("SOCIO:_PROVISAO_MOV_TOTAL_M", "SOCIO_PROVISAO_MOV_TOTAL_M")\
                        .withColumnRenamed("SOCIO:_PROVISAO_M_1", "SOCIO_PROVISAO_M_1")\
                        .withColumnRenamed("SOCIO:_PROVISAO_TOTAL_M_1", "SOCIO_PROVISAO_TOTAL_M_1")\
                        .withColumnRenamed("SOCIO:_PROV_TOTAL_PASSIVO_M", "SOCIO_PROV_TOTAL_PASSIVO_M")\
                        .withColumnRenamed("SOCIO:_PROVISAO_TOTAL_M", "SOCIO_PROVISAO_TOTAL_M")\
                        .withColumnRenamed("STATUS_M_1", "STATUS_MES_ANT")\
                        .withColumnRenamed("TIPO_PGTO", "TIPO_PAGAMENTO")\
                        .withColumnRenamed("VALOR_DA_CAUSA", "VLR_CAUSA")





# COMMAND ----------

df_ff_civel_consolidado.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO")

# COMMAND ----------

fecham_civel_consol_1 = spark.sql("""
SELECT
    ID_PROCESSO,
    CADASTRO,
    EMPRESA_M_PERC,
    EMPRESA_M_PERC_1,
    RISCO_PERC,
    SOCIO_M_PERC,
    SOCIO_M_1_PERC,
    AREA_DO_DIREITO,
    CENTRO_CUSTO_M,
    CENTRO_CUSTO_M_1,
    CLASSIFICACAO_MOV_M,
    CORRECAO_M,
    CORRECAO_M_1,
    CORRECAO_MOV_M,
    CORRECAO_MOV_M,
    
    DEMITIDO_POR_REESTRUTURACAO,
    DISTRIBUICAO,
    EMPRESA_M,
    EMPRESA_M_1,
    EMPRESA_CLASSIFICACAO_MOV_M,
    EMPRESA_CORRECAO_M,
    EMPRESA_CORRECAO_M_1,
    EMPRESA_CORRECAO_M_1_1,
    EMPRESA_CORRECAO_MOV_M,
    EMPRESA_PROV_TOTAL_PASSIVO_M,
    EMPRESA_PROVISAO_M_1,
    EMPRESA_PROVISAO_MOV_M,
    EMPRESA_PROVISAO_MOV_TOTAL_M,
    EMPRESA_PROVISAO_TOTAL_M,
    EMPRESA_PROVISAO_TOTAL_M_1,
    ENCERRADOS,
    ESCRITORIO,
    ESTOQUE,
    FILIAL_M,
    GRUPO_FECHAMENTO,
    GRUPO_M,
    GRUPO_M_1,
    INDICACAO_PROCESSO_ESTRATEGICO,
    LINHA,
    MEDIA_PAGAMENTO,
    NATUREZA_OPERACIONAL_M,
    NATUREZA_OPERACIONAL_M_1,
    NUM_PROCESSO,
    NOVOS,
    OBJETO_ASSUNTO_CARGO_M,
    OBJETO_ASSUNTO_CARGO_M_1,
    ORGAO_OFENSOR_FLUXO_M,
    ORGAO_OFENSOR_FLUXO_M_1,
    PASTA,
    
    PROVISAO_M_1,
    PROVISAO_MOV_M,
    PROVISAO_MOV_TOTAL_M,
    PROVISAO_TOTAL_M,
    PROVISAO_TOTAL_M_1,
    PROVISAO_TOTAL_PASSIVO_M,
    
    REABERTURA,
    SOCIO_CLASSIFICACAO_MOV_M,
    
    SOCIO_CORRECAO_M,
    SOCIO_CORRECAO_M_1,
    
    SOCIO_CORRECAO_MOV_M,
    
    
    SOCIO_PROV_TOTAL_PASSIVO_M,
    
    SOCIO_PROVISAO_M_1,
    SOCIO_PROVISAO_MOV_M,
    SOCIO_PROVISAO_MOV_M,
  
    SOCIO_PROVISAO_MOV_TOTAL_M,
    
    SOCIO_PROVISAO_TOTAL_M,
    SOCIO_PROVISAO_TOTAL_M_1,
    STATUS_M,
    STATUS_MES_ANT,
    SUB_OBJETO_ASSUNTO_CARGO_M,
    SUB_OBJETO_ASSUNTO_CARGO_M_1,
    SUB_AREA_DO_DIREITO,
    TIPO_PAGAMENTO,
    
    VLR_CAUSA
    
FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO
""")

# COMMAND ----------

fecham_civel_consol_1.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_1")

# COMMAND ----------

#AJUSTE DO CAMPO DE CADASTRO ACRIAÇÃO DE BASE COM CAMPO ADICIONAL

# Usando uma subconsulta com ROW_NUMBER() para garantir a ordenação correta e a remoção de duplicatas
df_tb_data_cadastro = spark.sql("""
    SELECT ID_PROCESSO, 
           CADASTRO, 
           AREA_DO_DIREITO, 
           SUB_AREA_DO_DIREITO, 
           OBJETO_ASSUNTO_CARGO_M, 
           SUB_OBJETO_ASSUNTO_CARGO_M
    FROM (
        SELECT ID_PROCESSO, 
               CADASTRO, 
               AREA_DO_DIREITO, 
               SUB_AREA_DO_DIREITO, 
               OBJETO_ASSUNTO_CARGO_M, 
               SUB_OBJETO_ASSUNTO_CARGO_M,
               ROW_NUMBER() OVER (PARTITION BY ID_PROCESSO ORDER BY CADASTRO DESC, AREA_DO_DIREITO DESC, SUB_AREA_DO_DIREITO DESC, OBJETO_ASSUNTO_CARGO_M DESC, SUB_OBJETO_ASSUNTO_CARGO_M DESC) AS row_num
        FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_1
    ) tmp
    WHERE row_num = 1
    ORDER BY ID_PROCESSO
""")

# COMMAND ----------

fecham_civel_consol_1.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_2")
df_tb_data_cadastro.createOrReplaceTempView("TB_DATA_CADASTRO")

# COMMAND ----------

#AJUSTE DA DATA CADASTRO


# Executando a consulta SQL para realizar o LEFT JOIN e aplicar as lógicas de preenchimento
df_fecham_civel_consol_2 = spark.sql("""
    SELECT A.*,
           CASE WHEN A.CADASTRO IS NULL THEN B.CADASTRO ELSE A.CADASTRO END AS CADASTRO_AJUSTADO,
           CASE WHEN A.AREA_DO_DIREITO IS NULL THEN B.AREA_DO_DIREITO ELSE A.AREA_DO_DIREITO END AS AREA_DO_DIREITO2,
           CASE WHEN A.SUB_AREA_DO_DIREITO IS NULL THEN B.SUB_AREA_DO_DIREITO ELSE A.SUB_AREA_DO_DIREITO END AS SUB_AREA_DO_DIREITO2,
           CASE WHEN A.OBJETO_ASSUNTO_CARGO_M IS NULL THEN B.OBJETO_ASSUNTO_CARGO_M ELSE A.OBJETO_ASSUNTO_CARGO_M END AS OBJETO_ASSUNTO_CARGO_M2,
           CASE WHEN A.SUB_OBJETO_ASSUNTO_CARGO_M IS NULL THEN B.SUB_OBJETO_ASSUNTO_CARGO_M ELSE A.SUB_OBJETO_ASSUNTO_CARGO_M END AS SUB_OBJETO_ASSUNTO_CARGO_M2
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_2 AS A
    LEFT JOIN TB_DATA_CADASTRO AS B
    ON A.ID_PROCESSO = B.ID_PROCESSO
""")


# COMMAND ----------

# MAGIC %md
# MAGIC #####Ajuste da base de fechamento para a inclusão das demais informações (Bases Gerenciais)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import when, col

# Carregamento informações de fechamento

# Realizando a junção left entre os DataFrames
joined_df = fecham_civel_consolidado_4_df.alias("A") \
    .join(
        tb_ger_civel_1_df.alias("B"),
        F.col("A.ID_PROCESSO") == F.col("B.PROCESSO_ID"), # Substitua "PROCESSO_ID" pelo nome correto da coluna no DataFrame B
        "left"
    ) \
    .select(
        F.col("A.ID_PROCESSO"),
        when(F.col("B.DATA_REGISTRO").isNull(), F.col("A.CADASTRO_AJUSTADO")).otherwise(F.col("B.DATA_REGISTRO")).alias("CADASTRO"),
        when(F.col("B.DATA_REGISTRO").isNull(), F.trunc(F.col("A.CADASTRO_AJUSTADO"), "MM")).otherwise(F.trunc(F.col("B.DATA_REGISTRO"), "MM")).alias("MES_CADASTRO"),
        F.col("B.PARTE_CONTRARIA_CPF"),
        F.col("B.PARTE_CONTRARIA_NOME"),
        F.col("A.STATUS_MES_ANT"),
        when(F.col("A.STATUS_M").isNull(), F.col("B.STATUS")).otherwise(F.col("A.STATUS_M")).alias("STATUS"),
        when(F.col("A.EMPRESA_M").isNull(), F.col("B.EMPRESA")).otherwise(F.col("A.EMPRESA_M")).alias("EMPRESA"),
        when(F.col("A.CENTRO_CUSTO_M").isNull(), F.col("B.CENTRO_CUSTO_DEMANDANTE")).otherwise(F.col("A.CENTRO_CUSTO_M")).alias("CENTRO_CUSTO_DEMANDANTE"),
        # Repita essa lógica para demais campos conforme necessário
        F.col("B.DATA_REGISTRADO"),
        F.col("B.DISTRIBUICAO"),
        F.col("B.DATA_AUDIENCIA_INICIAL"),
        F.col("B.DIRETA_OU_MKT"),
        when(F.col("A.AREA_DO_DIREITO2").isNull(), F.col("B.AREA_DO_DIREITO")).otherwise(F.col("A.AREA_DO_DIREITO2")).alias("AREA_DO_DIREITO"),
        when(F.col("A.SUB_AREA_DO_DIREITO2").isNull(), F.col("B.SUB_AREA_DO_DIREITO")).otherwise(F.col("A.SUB_AREA_DO_DIREITO2")).alias("SUB_AREA_DO_DIREITO"),
        # Continue adicionando as demais expressões de seleção conforme o exemplo acima
        F.col("A.MES_FECH")
    )

# COMMAND ----------

from pyspark.sql.functions import col, when

# Supondo que `fecham_civel_consolidado_5_df` seja equivalente à FECHAM_CIVEL_CONSOL_&NMMES._5 do SAS
# e `pj_informacoes_produtos_df` seja equivalente à PJ_INFORMACOES_PRODUTOS

fecham_civel_consolidado_6_df = fecham_civel_consolidado_5_df.alias("A")\
    .join(pj_informacoes_produtos_df.alias("B"), col("A.ID_PROCESSO") == col("B.`PROCESSO - ID`N"), "left")\
    .select(
        col("A.*"),
        when(col("A.DATA_DO_PEDIDO").isNull(), col("B.`DATA DO PEDIDO`N")).otherwise(col("A.DATA_DO_PEDIDO")).alias("DATA_DO_PEDIDO"),
        when(col("A.DATA_FATO_GERADOR").isNull(), col("B.`DATA DO FATO GERADOR`N")).otherwise(col("A.DATA_FATO_GERADOR")).alias("DATA_FATO_GERADOR"),
        when(col("A.INDIQUE_A_FILIAL").isNull(), col("B.FILIAL")).otherwise(col("A.INDIQUE_A_FILIAL")).alias("INDIQUE_A_FILIAL"),
        when(col("A.FABRICANTE").isNull(), col("B.`FABRICANTE DO PRODUTO`N")).otherwise(col("A.FABRICANTE")).alias("FABRICANTE"),
        when(col("A.PRODUTO").isNull(), col("B.PRODUTO")).otherwise(col("A.PRODUTO")).alias("PRODUTO"),
        when(col("A.TRANSPORTADORA").isNull(), col("B.TRANSPORTADORA")).otherwise(col("A.TRANSPORTADORA")).alias("TRANSPORTADORA")
    )

# Lembre-se de ajustar o formato das datas `DATA_DO_PEDIDO` e `DATA_FATO_GERADOR`, se necessário
fecham_civel_consolidado_6_df = fecham_civel_consolidado_6_df.withColumn("DATA_DO_PEDIDO", to_date(col("DATA_DO_PEDIDO"), "ddMMyyyy"))\
    .withColumn("DATA_FATO_GERADOR", to_date(col("DATA_FATO_GERADOR"), "ddMMyyyy"))

# COMMAND ----------

from pyspark.sql.functions import when, col, current_date, to_date

fecham_civel_consolidado_7_df = fecham_civel_consolidado_6_df.select(
    "ID_PROCESSO",
    "CADASTRO",
    "MES_CADASTRO",
    "PARTE_CONTRARIA_CPF",
    "PARTE_CONTRARIA_NOME",
    "STATUS_MES_ANT",
    "STATUS",
    "EMPRESA",
    "CENTRO_CUSTO_DEMANDANTE",
    "BU",
    "VP",
    "DIRETORIA",
    "RESPONSAVEL_AREA",
    "AREA_FUNCIONAL",
    "PROVISAO_TOTAL_PASSIVO_M",
    "PROVISAO_MOV_M",
    "PROVISAO_TOTAL_M",
    "EMPRESA_PROVISAO_MOV_M",
    "ACORDOS",
    col("`CONDENAÇÃO`").alias("CONDENACAO"),
    "PENHORA",
    "OUTROS_PAGAMENTOS",
    "GARANTIA",
    "TOTAL_PAGAMENTOS",
    "DT_ULT_PGTO",
    "INDICACAO_PROCESSO_ESTRATEGICO",
    "NOVOS",
    "ENCERRADOS",
    "ESTOQUE",
    "DATA_REGISTRADO",
    "DISTRIBUICAO",
    "DATA_AUDIENCIA_INICIAL",
    "DIRETA_OU_MKT",
    "AREA_DO_DIREITO",
    "SUB_AREA_DO_DIREITO",
    "LISTA_LOJISTA_ID",
    "LISTA_LOJISTA_NOME",
    "ASSUNTO_CIVEL_PRI",
    "ASSUNTO_CIVEL_ASS",
    when(col("DATA_DO_PEDIDO") >= current_date(), None).otherwise(col("DATA_DO_PEDIDO")).alias("DATA_DO_PEDIDO"),
    when(col("DATA_DA_COMPRA") >= current_date(), None).otherwise(col("DATA_DA_COMPRA")).alias("DATA_DA_COMPRA"),
    when(col("DATA_FATO_GERADOR") >= current_date(), None).otherwise(col("DATA_FATO_GERADOR")).alias("DATA_FATO_GERADOR"),
    "NUM_PEDIDO",
    "INDIQUE_A_FILIAL",
    "VALOR_DA_CAUSA",
    "ACAO",
    "ESCRITORIO_EXTERNO",
    "FASE",
    "MOTIVO_DE_ENCERRAMENTO",
    "ESFERA",
    "ESTADO",
    "COMARCA",
    "FABRICANTE",
    "PRODUTO",
    "TRANSPORTADORA",
    "GRUPO",
    "RECLAMACAO_BLACK_FRIDAY",
    "MES_FECH"
)

# Assegure-se de que as colunas DATA_DO_PEDIDO, DATA_DA_COMPRA e DATA_FATO_GERADOR estão no formato correto antes de aplicar o `when`.
# Caso as datas estejam em formatos diferentes de "yyyy-MM-dd" será necessário convertê-las usando `to_date(col("NOME_COLUNA"), "FORMATO_ORIGINAL")`.

# COMMAND ----------

from pyspark.sql import functions as F

# Supondo que df seja o DataFrame carregado previamente com os dados de FECHAM_CIVEL_CONSOL_&NMMES._7
# Substitua &NMMES. pelo valor correspondente

fecham_civel_consolidado_8_df = fecham_civel_consolidado_7_df.withColumn("ACORDO", fecham_civel_consolidado_7_df["ACORDOS"]) \
    .withColumn("CONDENACAO", F.lit("CONDENAÇÃO")) \
    .withColumn("MESES_AGING_ENCERR",
                F.when(df["ENCERRADOS"] == 1, F.months_between(df["MES_FECH"], df["MES_CADASTRO"]))) \
    .withColumn("FX_MES_AGING_ENCERR",
                F.when(df["ENCERRADOS"] == 1, F.format_string("%d dias", F.datediff(df["MES_FECH"], df["MES_CADASTRO"])))) \
    .withColumn("MESES_AGING_ESTOQ",
                F.when(df["ESTOQUE"] == 1, F.months_between(df["MES_FECH"], df["MES_CADASTRO"]))) \
    .withColumn("FX_MES_AGING_ESTOQ",
                F.when(df["ESTOQUE"] == 1, F.format_string("%d dias", F.datediff(df["MES_FECH"], df["MES_CADASTRO"])))) \
    .withColumn("DT_FATO_GERADOR_F",
                F.format_string("%d/%m/%Y", F.greatest(df["DATA_DO_PEDIDO"], df["DATA_DA_COMPRA"], df["DATA_FATO_GERADOR"]))) \
    .withColumn("MES_FATO_GERADOR",
                F.date_format(F.trunc(F.greatest(df["DATA_DO_PEDIDO"], df["DATA_DA_COMPRA"], df["DATA_FATO_GERADOR"]), "MM"), "dd/MM/yyyy")) \
    .withColumn("DIAS_FATOGER_ENTR",
                F.datediff(df["CADASTRO"], F.greatest(df["DATA_DO_PEDIDO"], df["DATA_DA_COMPRA"], df["DATA_FATO_GERADOR"]))) \
    .withColumn("MESES_FATOGER_ENTR",
                F.months_between(df["CADASTRO"], F.greatest(df["DATA_DO_PEDIDO"], df["DATA_DA_COMPRA"], df["DATA_FATO_GERADOR"]))) \
    .withColumn("FX_TMP_FATO_GERADOR",
                F.format_string("%d dias", F.datediff(df["CADASTRO"], F.greatest(df["DATA_DO_PEDIDO"], df["DATA_DA_COMPRA"], df["DATA_FATO_GERADOR"]))))

# Certifique-se de ajustar os nomes das colunas e tipos de formatação conforme necessário

# COMMAND ----------

# Suponha que 'df' é o seu DataFrame equivalente ao FECHAM_CIVEL_CONSOL_&NMMES._8 no SAS

# Para remover duplicatas
fecham_civel_consolidado_8_df = fecham_civel_consolidado_8_df.dropDuplicates()

# Supondo que você queira ordenar baseado em todas as colunas, você pode conseguir isso assim:
colunas = fecham_civel_consolidado_8_df.columns
fecham_civel_consolidado_8_df = fecham_civel_consolidado_8_df.orderBy(*colunas)

# COMMAND ----------

#carrega o valor histórico de provisão na base

from pyspark.sql.functions import when, col

fecham_civel_consolidado_10_df = fecham_civel_consolidado_9_df.withColumn(
    "MOTIVO_ENC_AGRP",
    when(
        (col("ENCERRADOS") == 1) & (col("ACORDO") > 1),
        "ACORDO"
    ).when(
        (col("ENCERRADOS") == 1) & 
        (col("CONDENACAO") + col("PENSAO") + col("PENHORA") + col("OUTROS_PAGAMENTOS") + col("GARANTIA") > 1),
        "CONDENACAO"
    ).when(
        (col("ENCERRADOS") == 1) & 
        (col("ACORDO") + col("CONDENACAO") + col("PENSAO") + col("PENHORA") + col("OUTROS_PAGAMENTOS") + col("GARANTIA") <= 1),
        "SEM ONUS"
    ).otherwise(None)
)

# COMMAND ----------

from pyspark.sql.functions import when, col

# Supondo que fecham_civel_consolidado_10_df é o DataFrame que estamos modificando
fecham_civel_consolidado_11_df = fecham_civel_consolidado_10_df.withColumn(
    "FLAG_EMPRESA",
    when(
        (col("EMPRESA").isin('NOVA PONTOCOM COMERCIO ELETRÔNICO S/A', 'VIA VAREJO ONLINE', 'CASAS BAHIA ONLINE') &
         (col("DIRETA_OU_MKT") == 'DIRETA')),
        '1P'
    ).when(
        (col("EMPRESA").isin('NOVA PONTOCOM COMERCIO ELETRÔNICO S/A', 'VIA VAREJO ONLINE', 'CASAS BAHIA ONLINE') &
         (col("DIRETA_OU_MKT") == 'MARKETPLACE')),
        '3P'
    ).when(
        (col("EMPRESA").isin('GLOBEX UTILIDADES S/A (GPA)', 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'VIA VAREJO S/A', 'GRUPO CASAS BAHIA S/A', 'NOVA CASA BAHIA S/A (ATUAL VIA VAREJO)', 'PONTOCRED NEGÓCIOS DE VAREJO LTDA') &
         (col("DIRETA_OU_MKT") == 'DIRETA')),
        'LOJA FÍSICA'
    ).when(
        (col("EMPRESA").isin('GLOBEX UTILIDADES S/A (GPA)', 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'VIA VAREJO S/A', 'GRUPO CASAS BAHIA S/A', 'NOVA CASA BAHIA S/A (ATUAL VIA VAREJO)') &
         (col("DIRETA_OU_MKT") == 'MARKETPLACE')),
        '3P'
    ).otherwise(col("DIRETA_OU_MKT"))
)

# COMMAND ----------

#marcação campo DIRETA_OU_MKT

from pyspark.sql.functions import when, col

fecham_civel_consolidado_12_df = fecham_civel_consolidado_11_df.withColumn(
    "FLAG_EMPRESA_",
    when(
        col("FLAG_EMPRESA").isNull() &
        col("EMPRESA").isin('NOVA PONTOCOM COMERCIO ELETRÔNICO S/A', 'VIA VAREJO ONLINE', 'CASAS BAHIA ONLINE'),
        '1P'
    ).when(
        col("FLAG_EMPRESA").isNull() &
        col("EMPRESA") == 'BANQI INSTITUIÇÃO DE PAGAMENTO LTDA.',
        'BANQI'
    ).when(
        col("FLAG_EMPRESA").isNull() &
        col("EMPRESA") == 'ASAP LOG LTDA.',
        'ASAP LOG'
    ).when(
        col("FLAG_EMPRESA").isNull() &
        col("EMPRESA").isin(
            'CASAS BAHIA COMERCIAL LTDA', 'GLOBEX UTILIDADES S/A (ATUAL VIA VAREJO)',
            'GLOBEX UTILIDADES S/A (CASAS BAHIA)', 'GLOBEX UTILIDADES S/A (GPA)',
            'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'NOVA CASA BAHIA S/A (ATUAL VIA VAREJO)',
            'GRUPO CASAS BAHIA S/A', 'VIA VAREJO S/A'),
        'LOJA FÍSICA'
    ).otherwise(col("FLAG_EMPRESA"))
)

# Remover a antiga coluna FLAG_EMPRESA e renomear FLAG_EMPRESA_ para FLAG_EMPRESA
fecham_civel_consolidado_12_df = fecham_civel_consolidado_12_df.drop("FLAG_EMPRESA") \
    .withColumnRenamed("FLAG_EMPRESA_", "FLAG_EMPRESA")

# COMMAND ----------


# Removendo duplicatas com base na coluna ID_PROCESSO

tb_fecham_financ_civel_NMMES_f = tb_fecham_financ_civel_NMMES_f.dropDuplicates(['ID_PROCESSO'])

# Ordenando o DataFrame
tb_fecham_financ_civel_NMMES_f = tb_fecham_financ_civel_NMMES_f.orderBy(
    'ID_PROCESSO', 'MES_FECH', 'NOVOS', 'ENCERRADOS', 'ESTOQUE'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Base final para a geração do indicador Cível Consolidado

# COMMAND ----------

spark.sql("""
SELECT
    MES_FECH,
    ID_PROCESSO,
    AREA_DO_DIREITO,
    EMPRESA,
    FLAG_EMPRESA,
    DP_ASSUNTO_PRINCIPAL,
    ESCRITORIO_EXTERNO,
    DP_FASE,
    MOTIVO_ENC_AGRP,
    ESTADO,
    MESES_AGING_ENCERR,
    FX_MES_AGING_ENCERR,
    DT_FATO_GERADOR_F,
    MES_FATO_GERADOR,
    MESES_FATOGER_ENTR,
    FX_TMP_FATO_GERADOR,
    FX_ANO_AGING_NOVO,
    INDICACAO_PROCESSO_ESTRATEGICO,
    RESPONSABILIDADE,
    CASE
        WHEN PROVISAO_TOTAL_PASSIVO_M > 0 THEN 'PASSÍVEL PROVISÃO'
        WHEN RESPONSABILIDADE = 'TERCEIRO' THEN 'TERCEIRO'
        WHEN INDICACAO_PROCESSO_ESTRATEGICO = 'DEFESA' THEN 'DEFESA'
        WHEN AREA_DO_DIREITO = 'CÍVEL ESTRATÉGICO' THEN 'ESTRATÉGICO'
        ELSE 'PASSÍVEL PROVISÃO'
    END AS DP_NATUREZA,
    SUM(NOVOS) AS NOVOS,
    SUM(ENCERRADOS) AS ENCERRADOS,
    SUM(ESTOQUE) AS ESTOQUE,
    SUM(ACORDO) AS ACORDO,
    SUM(CONDENACAO) AS CONDENACAO,
    SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS,
    SUM(PROVISAO_TOTAL_PASSIVO_M) AS PROVISAO_TOTAL_PASSIVO_M,
    SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M,
    SUM(`Provisão Total Passivo Hist`) AS PROVISAO_TOTAL_M_1
FROM TB_FECHAM_FINANC_CIVEL_NMMES_F
WHERE MES_FECH >= '2023-01-01'
GROUP BY
    MES_FECH, ID_PROCESSO, AREA_DO_DIREITO, EMPRESA, FLAG_EMPRESA, DP_ASSUNTO_PRINCIPAL, ESCRITORIO_EXTERNO, DP_FASE, MOTIVO_ENC_AGRP,
    ESTADO, MESES_AGING_ENCERR, FX_MES_AGING_ENCERR, DT_FATO_GERADOR_F, MES_FATO_GERADOR, MESES_FATOGER_ENTR,
    FX_TMP_FATO_GERADOR, FX_ANO_AGING_NOVO, INDICACAO_PROCESSO_ESTRATEGICO, RESPONSABILIDADE
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Formatação variáveis númericas 

# COMMAND ----------


