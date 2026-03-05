# Databricks notebook source
# MAGIC %md
# MAGIC #Consolidação dados Cível

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1- Carga e tratamentos iniciais

# COMMAND ----------

# Parametro do nome da tabela da competência atual. Ex: 202404
dbutils.widgets.text("NMES", "")

# COMMAND ----------

NMES = dbutils.widgets.get("NMES")

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Carrega histórico dos fechamentos financeiros
df_ff_civel = spark.sql("select * from databox.juridico_comum.fechamento_civel_consolidado")

df_ff_civel = adjust_column_names(df_ff_civel)
df_ff_civel = remove_acentos(df_ff_civel)

# COMMAND ----------

#Checagem do campo MES_FECH deve estar de acordo com a última atualização da base consolidada
df_ff_civel.select("MES_FECH").distinct().show()

# COMMAND ----------

from pyspark.sql.functions import col, when

# Correcting the code by adding the missing closing parenthesis
tb_fech_civel_consolidado_1 = df_ff_civel.withColumn(
    "ESTRATEGIA",
    when(col("ESTRATEGIA").isin("ACO", "ACOR", "ACORD", "ACORDO"), "ACORDO")
    .when(col("ESTRATEGIA1").isin("ACO", "ACOR", "ACORD", "ACORDO"), "ACORDO")
    .when(col("ESTRATEGIA").isin("DEF", "DEFE", "DEFES", "DEFESA"), "DEFESA")
    .when(col("ESTRATEGIA1").isin("DEF", "DEFE", "DEFES", "DEFESA"), "DEFESA")
    
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
        ELSE C.`area funcional` END ) AS AREA_FUNCIONAL

FROM TB_FECH_FIN_CIVEL_CONSOLIDADO AS A 
LEFT JOIN TB_DE_PARA_BU AS B ON A.CENTRO_DE_CUSTO_M = B.`Centro de custo`
LEFT JOIN TB_DE_PARA_AREA_FUNCIONAL AS C ON A.CENTRO_DE_CUSTO_M = C.centro
WHERE A.ID_PROCESSO IS NOT NULL 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1 - Preparação da Base dos Indicadores Cível

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
                         .withColumnRenamed("EMPRESA_M", "EMPRESA") \
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
                        .withColumnRenamed("STATUS_M", "STATUS")\
                        .withColumnRenamed("STATUS_M_1", "STATUS_MES_ANT")\
                        .withColumnRenamed("TIPO_PGTO", "TIPO_PAGAMENTO")\
                        .withColumnRenamed("VALOR_DA_CAUSA", "VLR_CAUSA")





# COMMAND ----------

df_ff_civel_consolidado.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO")

# COMMAND ----------

fecham_civel_consol_1 = spark.sql("""
SELECT
    MES_FECH,
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
    EMPRESA,
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
    COALESCE(ACORDOS, 0) AS ACORDOS,
    COALESCE(CONDENACAO, 0) AS CONDENACAO,
    COALESCE(PENHORA, 0) AS PENHORA,
    COALESCE(OUTROS_PAGAMENTOS, 0) AS OUTROS_PAGAMENTOS,
    COALESCE(GARANTIA, 0) AS GARANTIA, 
    COALESCE(TOTAL_PAGAMENTOS, 0) AS TOTAL_PAGAMENTOS,
    COALESCE(IMPOSTO, 0) AS IMPOSTO,
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
    STATUS,
    STATUS_MES_ANT,
    SUB_OBJETO_ASSUNTO_CARGO_M,
    SUB_OBJETO_ASSUNTO_CARGO_M_1,
    SUB_AREA_DO_DIREITO,
    TIPO_PAGAMENTO,
    BU,
    VP,
    DIRETORIA,
    RESPONSAVEL_AREA,
    AREA_FUNCIONAL,
    VALOR,
    VLR_CAUSA,
    DT_ULT_PGTO
FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO
""")

# COMMAND ----------

tb_fech_civel_consolidado_1 = tb_fech_civel_consolidado_1.withColumnRenamed("DATACADASTRO", "CADASTRO")

# COMMAND ----------


tb_fech_civel_consolidado_1.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_1")

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
fecham_civel_consol_2 = spark.sql("""
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
# MAGIC #### 2.2 Ajuste da base de fechamento para a inclusão das demais informações (Bases Gerenciais)

# COMMAND ----------

fecham_civel_consol_2.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_3")


# COMMAND ----------

fecham_civel_consol_3 = spark.sql("""
    SELECT A.ID_PROCESSO,
           CASE WHEN B.DATA_REGISTRADO IS NULL THEN A.CADASTRO_AJUSTADO ELSE B.DATA_REGISTRADO END AS CADASTRO,
           CASE WHEN B.DATA_REGISTRADO IS NULL THEN DATE_ADD(LAST_DAY(ADD_MONTHS(A.CADASTRO_AJUSTADO, -1)), 1) 
                ELSE DATE_ADD(LAST_DAY(ADD_MONTHS(B.DATA_REGISTRADO, -1)), 1) END AS MES_CADASTRO,
           B.`PARTE_CONTRÁRIA_CPF`,
           B.`PARTE_CONTRÁRIA_NOME`,
           A.STATUS_MES_ANT,
           CASE WHEN A.STATUS IS NULL THEN B.STATUS ELSE A.STATUS END AS STATUS,
           CASE WHEN A.EMPRESA IS NULL THEN B.EMPRESA ELSE A.EMPRESA END AS EMPRESA,
           CASE WHEN A.CENTRO_CUSTO_M IS NULL THEN B.CENTRO_DE_CUSTO_AREA_DEMANDANTE 
                ELSE A.CENTRO_CUSTO_M END AS CENTRO_CUSTO_DEMANDANTE,
           A.BU,
           A.VP,
           A.DIRETORIA,
           A.RESPONSAVEL_AREA,
           A.AREA_FUNCIONAL,
           A.PROVISAO_TOTAL_PASSIVO_M,
           A.PROVISAO_MOV_M,
           A.PROVISAO_TOTAL_M,
           A.EMPRESA_PROVISAO_MOV_M,
           A.INDICACAO_PROCESSO_ESTRATEGICO,
           A.NOVOS,
           A.ENCERRADOS,
           A.ESTOQUE,
           A.ACORDOS, 
           A.CONDENACAO, 
           A.PENHORA, 
           A.OUTROS_PAGAMENTOS, 
           A.GARANTIA, 
           A.TOTAL_PAGAMENTOS, 
           A.DT_ULT_PGTO,
           B.DATA_REGISTRADO,
           B.`DISTRIBUIÇÃO`,
           B.DATA_AUDIENCIA_INICIAL,
           B.DIRETA_OU_MKT,
           CASE WHEN A.AREA_DO_DIREITO2 IS NULL THEN B.`ÁREA_DO_DIREITO` ELSE A.AREA_DO_DIREITO2 END AS AREA_DO_DIREITO,
           CASE WHEN A.SUB_AREA_DO_DIREITO2 IS NULL THEN B.SUB_AREA_DO_DIREITO ELSE A.SUB_AREA_DO_DIREITO2 END AS SUB_AREA_DO_DIREITO,
           B.LISTA_LOJISTA_ID,
           B.LISTA_LOJISTA_NOME,
           CASE WHEN A.OBJETO_ASSUNTO_CARGO_M2 IS NULL THEN B.ASSUNTO_CIVEL_PRI ELSE A.OBJETO_ASSUNTO_CARGO_M2 END AS ASSUNTO_CIVEL_PRI,
           CASE WHEN A.SUB_OBJETO_ASSUNTO_CARGO_M2 IS NULL THEN B.ASSUNTO_CIVEL_ASS ELSE A.SUB_OBJETO_ASSUNTO_CARGO_M2 END AS ASSUNTO_CIVEL_ASS,
           B.NUM_PEDIDO,
           B.INDIQUE_A_FILIAL,
           B.DATA_DO_PEDIDO,
           B.DATA_DA_COMPRA,
           B.DATA_FATO_GERADOR,
           B.VALOR_DA_CAUSA,
           B.ACAO,
           B.ESCRITORIO,
           B.FASE,
           B.MOTIVO_DE_ENCERRAMENTO,
           B.ESFERA,
           B.ESTADO,
           B.COMARCA,
           B.FABRICANTE,
           B.PRODUTO,
           B.TRANSPORTADORA,
           B.GRUPO,
           B.RECLAMACAO_BLACK_FRIDAY,
           A.MES_FECH
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_3 AS A
    LEFT JOIN databox.juridico_comum.TB_GER_CIVEL_1 AS B ON A.ID_PROCESSO = B.PROCESSO_ID
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3 Junção com a base de produtos PJ

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1 Base Gerencial Produtos PJ

# COMMAND ----------



# Now, you can safely retrieve the value of the widget
nmtabelaPJ = dbutils.widgets.get("nmtabelapj")

# COMMAND ----------


diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/'
arquivo_produtos = f'PJ_-_INFORMACOES_PRODUTOS- {nmtabelaPJ}.xlsx'
path_produtos = diretorio_origem + arquivo_produtos


# COMMAND ----------

gerencial_produtos_civel = read_excel(path_produtos, "'INFORMACOES'!A6")

# COMMAND ----------

# Lista as colunas com datas
produtos_civel_data_cols = find_columns_with_word(gerencial_produtos_civel, 'DATA ')
produtos_civel_data_cols.append('DISTRIBUIÇÃO')

# COMMAND ----------

print("produtos_civel_data_cols")
print(produtos_civel_data_cols)
print("\n")

# COMMAND ----------

produtos_civel_data_cols = convert_to_date_format(gerencial_produtos_civel, produtos_civel_data_cols)

# COMMAND ----------

gerencial_produtos_civel = adjust_column_names(gerencial_produtos_civel)
gerencial_produtos_civel = remove_acentos(gerencial_produtos_civel)

# COMMAND ----------

fecham_civel_consol_3.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_4")
gerencial_produtos_civel.createOrReplaceTempView("TB_PRODUTOS_PJ_CIVEL")

# COMMAND ----------

fecham_civel_consol_4 = spark.sql("""
    SELECT A.ID_PROCESSO
            ,A.CADASTRO
            ,A.MES_CADASTRO
            ,A.`PARTE_CONTRÁRIA_CPF`
            ,A.`PARTE_CONTRÁRIA_NOME`
            ,A.STATUS_MES_ANT
            ,A.STATUS
            ,A.EMPRESA
            ,A.CENTRO_CUSTO_DEMANDANTE
            ,A.BU
            ,A.VP
            ,A.DIRETORIA
            ,A.RESPONSAVEL_AREA
            ,A.AREA_FUNCIONAL
            ,A.PROVISAO_TOTAL_PASSIVO_M
            ,A.PROVISAO_MOV_M
            ,A.PROVISAO_TOTAL_M
            ,A.EMPRESA_PROVISAO_MOV_M
            ,A.ACORDOS
            ,A.CONDENACAO AS CONDENACAO
            ,A.PENHORA
            ,A.OUTROS_PAGAMENTOS
            ,A.GARANTIA 
            ,A.TOTAL_PAGAMENTOS
            ,A.DT_ULT_PGTO
            ,A.INDICACAO_PROCESSO_ESTRATEGICO
            ,A.NOVOS
            ,A.ENCERRADOS
            ,A.ESTOQUE
            ,A.DATA_REGISTRADO
            ,A.`DISTRIBUIÇÃO`
            ,A.DATA_AUDIENCIA_INICIAL
            ,A.DIRETA_OU_MKT 
            ,A.AREA_DO_DIREITO 
            ,A.SUB_AREA_DO_DIREITO
            ,A.LISTA_LOJISTA_ID
            ,A.LISTA_LOJISTA_NOME
            ,A.ASSUNTO_CIVEL_PRI
            ,A.ASSUNTO_CIVEL_ASS
            ,TO_DATE(COALESCE(A.DATA_DO_PEDIDO, B.`DATA_DO_PEDIDO`)) AS DATA_DO_PEDIDO
            ,A.DATA_DA_COMPRA
            ,TO_DATE(COALESCE(A.DATA_FATO_GERADOR, B.`DATA_DO_FATO_GERADOR`)) AS DATA_FATO_GERADOR
            ,A.NUM_PEDIDO
            ,COALESCE(A.INDIQUE_A_FILIAL, B.FILIAL) AS INDIQUE_A_FILIAL
            ,A.VALOR_DA_CAUSA
            ,A.ACAO
            ,A.ESCRITORIO
            ,A.FASE
            ,A.MOTIVO_DE_ENCERRAMENTO
            ,A.ESFERA
            ,A.ESTADO
            ,A.COMARCA
            ,COALESCE(A.FABRICANTE, B.FABRICANTE_DO_PRODUTO) AS FABRICANTE
            ,COALESCE(A.PRODUTO, B.PRODUTO) AS PRODUTO
            ,COALESCE(A.TRANSPORTADORA, B.TRANSPORTADORA) AS TRANSPORTADORA
            ,A.GRUPO
            ,A.RECLAMACAO_BLACK_FRIDAY
            ,A.MES_FECH
    FROM 
   TB_FECHAMENTO_CIVEL_CONSOLIDADO_4 AS A
    LEFT JOIN TB_PRODUTOS_PJ_CIVEL AS B ON A.ID_PROCESSO = B.PROCESSO_ID
""")

# COMMAND ----------

fecham_civel_consol_4.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_5")


# COMMAND ----------

# MAGIC %md
# MAGIC ####2.4 Tratamento Data Fato Gerador

# COMMAND ----------

# CRIA VARIÁVEIS COM O CAMPO DATA DO FATO GERADOR - PARTE 1


fecham_civel_consol_5 = spark.sql("""
    SELECT ID_PROCESSO
            ,CADASTRO
            ,MES_CADASTRO
            ,`PARTE_CONTRÁRIA_CPF`
            ,`PARTE_CONTRÁRIA_NOME`
            ,STATUS_MES_ANT
            ,STATUS
            ,EMPRESA
            ,CENTRO_CUSTO_DEMANDANTE
            ,BU
            ,VP
            ,DIRETORIA
            ,RESPONSAVEL_AREA
            ,AREA_FUNCIONAL
            ,PROVISAO_TOTAL_PASSIVO_M
            ,PROVISAO_MOV_M
            ,PROVISAO_TOTAL_M
            ,EMPRESA_PROVISAO_MOV_M
            ,ACORDOS
            ,CONDENACAO
            ,PENHORA
            ,OUTROS_PAGAMENTOS
            ,GARANTIA
            ,TOTAL_PAGAMENTOS
            ,DT_ULT_PGTO
            ,INDICACAO_PROCESSO_ESTRATEGICO
            ,NOVOS
            ,ENCERRADOS
            ,ESTOQUE
            ,DATA_REGISTRADO
            ,`DISTRIBUIÇÃO`
            ,DATA_AUDIENCIA_INICIAL
            ,DIRETA_OU_MKT 
            ,AREA_DO_DIREITO 
            ,SUB_AREA_DO_DIREITO
            ,LISTA_LOJISTA_ID
            ,LISTA_LOJISTA_NOME
            ,ASSUNTO_CIVEL_PRI
            ,ASSUNTO_CIVEL_ASS
            ,CASE WHEN DATA_DO_PEDIDO >= CURRENT_DATE THEN NULL ELSE DATA_DO_PEDIDO END AS DATA_DO_PEDIDO
            ,CASE WHEN DATA_DA_COMPRA >= CURRENT_DATE THEN NULL ELSE DATA_DA_COMPRA END AS DATA_DA_COMPRA
            ,CASE WHEN DATA_FATO_GERADOR >= CURRENT_DATE THEN NULL ELSE DATA_FATO_GERADOR END AS DATA_FATO_GERADOR
            ,NUM_PEDIDO 
            ,INDIQUE_A_FILIAL
            ,VALOR_DA_CAUSA
            ,ACAO
            ,ESCRITORIO
            ,FASE
            ,MOTIVO_DE_ENCERRAMENTO
            ,ESFERA
            ,ESTADO
            ,COMARCA
            ,FABRICANTE
            ,PRODUTO
            ,TRANSPORTADORA
            ,GRUPO
            ,RECLAMACAO_BLACK_FRIDAY
            ,MES_FECH
    FROM  TB_FECHAMENTO_CIVEL_CONSOLIDADO_5 A
""")


# COMMAND ----------

fecham_civel_consol_5.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_6")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.4.1 Tabelas Auxiliares

# COMMAND ----------

# AgingCivel
data_civel = [
    (-9999, 90, 'até 3 meses'),
    (90, 180, '3 - 6 meses'),
    (180, 360, '6 - 12 meses'),
    (360, 720, '12 - 24 meses'),
    (720, 99999999, '+24meses')
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("start_range", IntegerType(), False),
    StructField("end_range", IntegerType(), False),
    StructField("label", StringType(), False)
])

df_AgingCivel = spark.createDataFrame(data_civel, schema)
df_AgingCivel.createOrReplaceTempView("AgingCivel")
df_AgingCivel.show()

# COMMAND ----------

# AgingNovoModeloCivel

data_civel_modelo = [
    (-9999, 360, 'até 1 ano'),
    (360, 720, '1 - 2 anos'),
    (720, 1080, '2 - 3 anos'),
    (1080, 1440, '3 - 4 anos'),
    (1440, 1800, '4 - 5 anos'),
    (1800, 2160, '5 - 6 anos'),
    (2160, 2520, '6 - 7 anos'),
    (2520, 9999999, '+7 anos')
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("start_range", IntegerType(), False),
    StructField("end_range", IntegerType(), False),
    StructField("label", StringType(), False)
])

df_AgingNovoModeloCivel = spark.createDataFrame(data_civel_modelo, schema)
df_AgingNovoModeloCivel.createOrReplaceTempView("df_AgingNovoModeloCivel")
df_AgingNovoModeloCivel.show()

# COMMAND ----------

#BASE CAMPO FATO GERADOR PARTE 2 

fecham_civel_consol_6 = spark.sql("""
    SELECT ID_PROCESSO
            ,CADASTRO
            ,MES_CADASTRO
            ,`PARTE_CONTRÁRIA_CPF`
            ,`PARTE_CONTRÁRIA_NOME`
            ,STATUS_MES_ANT
            ,STATUS
            ,EMPRESA
            ,CENTRO_CUSTO_DEMANDANTE
            ,BU
            ,VP
            ,DIRETORIA
            ,RESPONSAVEL_AREA
            ,AREA_FUNCIONAL
            ,PROVISAO_TOTAL_PASSIVO_M
            ,PROVISAO_MOV_M
            ,PROVISAO_TOTAL_M
            ,EMPRESA_PROVISAO_MOV_M
            ,ACORDOS AS ACORDOS
            ,CONDENACAO 
            ,PENHORA
            ,OUTROS_PAGAMENTOS
            ,GARANTIA
            ,TOTAL_PAGAMENTOS
            ,DT_ULT_PGTO
            ,INDICACAO_PROCESSO_ESTRATEGICO
            ,NOVOS
            ,ENCERRADOS
            ,ESTOQUE
            ,CASE WHEN ESTOQUE = 1 THEN MONTHS_BETWEEN(TO_DATE(MES_FECH), TO_DATE(MES_CADASTRO)) END AS MESES_AGING_ESTOQ
            ,CASE WHEN ESTOQUE = 1 THEN CONCAT(INT(DATEDIFF(TO_DATE(MES_FECH), TO_DATE(MES_CADASTRO))), ' ', 'AgingCivel') END AS FX_MES_AGING_ESTOQ
            ,DATA_REGISTRADO
            ,`DISTRIBUIÇÃO`
            ,DATA_AUDIENCIA_INICIAL
            ,DIRETA_OU_MKT 
            ,AREA_DO_DIREITO 
            ,SUB_AREA_DO_DIREITO
            ,LISTA_LOJISTA_ID
            ,LISTA_LOJISTA_NOME
            ,ASSUNTO_CIVEL_PRI
            ,ASSUNTO_CIVEL_ASS
            ,DATA_DO_PEDIDO
            ,DATA_DA_COMPRA
            ,DATA_FATO_GERADOR
            ,NUM_PEDIDO 
            ,INDIQUE_A_FILIAL
            ,VALOR_DA_CAUSA
            ,ACAO
            ,ESCRITORIO
            ,FASE
            ,MOTIVO_DE_ENCERRAMENTO
            ,ESFERA
            ,ESTADO
            ,COMARCA
            ,FABRICANTE
            ,PRODUTO
            ,TRANSPORTADORA
            ,GRUPO
            ,RECLAMACAO_BLACK_FRIDAY
            ,MES_FECH
            ,CASE WHEN ENCERRADOS = 1 THEN DATEDIFF(MES_FECH, MES_CADASTRO) END AS DIAS_AGING_ENCERR
            ,CASE WHEN ESTOQUE = 1 THEN DATEDIFF(MES_FECH, MES_CADASTRO) END AS DIAS_AGING_ESTOQUE
            ,DATEDIFF(MES_FECH,GREATEST(DATA_DO_PEDIDO,DATA_DA_COMPRA,DATA_FATO_GERADOR,CADASTRO)) AS FATO_GERADOR_AGING
            ,DATEDIFF(CADASTRO, GREATEST(DATA_DO_PEDIDO, DATA_DA_COMPRA, DATA_FATO_GERADOR)) AS DIAS_FATOGER_ENTR
            ,MONTHS_BETWEEN(CADASTRO, GREATEST(DATA_DO_PEDIDO, DATA_DA_COMPRA, DATA_FATO_GERADOR)) AS MESES_FATOGER_ENTR
            ,TO_DATE(CONCAT(YEAR(GREATEST(DATA_DO_PEDIDO, DATA_DA_COMPRA, DATA_FATO_GERADOR)), '-', LPAD(MONTH(GREATEST(DATA_DO_PEDIDO, 
             DATA_DA_COMPRA, DATA_FATO_GERADOR)), 2, '0'), '-01'), 'yyyy-MM-dd') AS MES_FATO_GERADOR


       FROM  TB_FECHAMENTO_CIVEL_CONSOLIDADO_6 A
""")

# Remove duplicatas considerando todas as colunas
fecham_civel_consol_6  = fecham_civel_consol_6.dropDuplicates()

# Ordena por todas as colunas
fecham_civel_consol_6 = fecham_civel_consol_6.sort(fecham_civel_consol_6.columns)

# COMMAND ----------

fecham_civel_consol_6.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_7")

# COMMAND ----------

fecham_civel_consol_7 = spark.sql("""
SELECT
  A.*,
  B.label AS FX_MES_AGING_ENCERR,
  C.label AS FX_MES_AGING_ESTOQ
FROM
  TB_FECHAMENTO_CIVEL_CONSOLIDADO_7 A
  LEFT JOIN AgingCivel B ON A.DIAS_AGING_ENCERR > B.start_range AND A.DIAS_AGING_ENCERR <= B.end_range
  LEFT JOIN AgingCivel C ON A.DIAS_AGING_ESTOQUE> C.start_range AND A.DIAS_AGING_ESTOQUE <= C.end_range;
"""
)

# COMMAND ----------

# Ordena por todas as colunas
fecham_civel_consol_7= fecham_civel_consol_7.sort(fecham_civel_consol_7.columns)

# COMMAND ----------

fecham_civel_consol_7 = fecham_civel_consol_7.fillna({
    'ACORDOS': 0,
    'CONDENACAO': 0,
    'PENHORA': 0,
    'OUTROS_PAGAMENTOS': 0,
    'GARANTIA': 0,
    'ENCERRADOS': 0

})

fecham_civel_consol_7.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_8")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Criação do Campo Motivo Agrupado

# COMMAND ----------

fecham_civel_consol_8 = spark.sql(
    """
SELECT
  *,
  CASE
    WHEN ENCERRADOS = 1 THEN CASE
      WHEN ACORDOS > 1 THEN 'ACORDO'
      WHEN (
        CONDENACAO  + PENHORA  + OUTROS_PAGAMENTOS + GARANTIA
      ) > 1 THEN 'CONDENACAO'
      WHEN (
        ACORDOS + CONDENACAO + PENHORA + OUTROS_PAGAMENTOS +  GARANTIA
      ) <= 1 THEN 'SEM ONUS'
    END
  END AS MOTIVO_ENC_AGRP
FROM
  TB_FECHAMENTO_CIVEL_CONSOLIDADO_8
"""
)

# COMMAND ----------

fecham_civel_consol_8.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_9")

# COMMAND ----------

#CRIA O DE PARA PARA LOJA FÍSICA E ONLINE

fecham_civel_consol_9 = spark.sql("""
    SELECT *,
           CASE WHEN EMPRESA IN ('NOVA PONTOCOM COMERCIO ELETRÔNICO S/A', 'VIA VAREJO ONLINE', 'CASAS BAHIA ONLINE')
                     AND DIRETA_OU_MKT = 'DIRETA' THEN '1P'
                WHEN EMPRESA IN ('NOVA PONTOCOM COMERCIO ELETRÔNICO S/A', 'VIA VAREJO ONLINE', 'CASAS BAHIA ONLINE')
                     AND DIRETA_OU_MKT = 'MARKETPLACE' THEN '3P'
                WHEN EMPRESA IN ('GLOBEX UTILIDADES S/A (GPA)', 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'VIA VAREJO S/A',
                                 'GRUPO CASAS BAHIA S/A', 'NOVA CASA BAHIA S/A (ATUAL VIA VAREJO)',
                                 'PONTOCRED NEGÓCIOS DE VAREJO LTDA')
                     AND DIRETA_OU_MKT = 'DIRETA' THEN 'LOJA FÍSICA'
                WHEN EMPRESA IN ('GLOBEX UTILIDADES S/A (GPA)', 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA', 'VIA VAREJO S/A',
                                 'GRUPO CASAS BAHIA S/A', 'NOVA CASA BAHIA S/A (ATUAL VIA VAREJO)')
                     AND DIRETA_OU_MKT = 'MARKETPLACE' THEN '3P'
                ELSE DIRETA_OU_MKT
           END AS FLAG_EMPRESA
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_9 
""")


# COMMAND ----------

fecham_civel_consol_9.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_10")

# COMMAND ----------

#READEQUAÇÃO DO CAMPO LOJA
# Executa o SQL para aplicar as transformações
fecham_civel_consol_10 = spark.sql("""
    SELECT *,
        CASE
            WHEN FLAG_EMPRESA = '' AND EMPRESA IN ('NOVA PONTOCOM COMERCIO ELETRÔNICO S/A', 'VIA VAREJO ONLINE', 'CASAS BAHIA ONLINE') THEN '1P'
            WHEN FLAG_EMPRESA = '' AND EMPRESA = 'BANQI INSTITUIÇÃO DE PAGAMENTO LTDA.' THEN 'BANQI'
            WHEN FLAG_EMPRESA = '' AND EMPRESA = 'ASAP LOG LTDA.' THEN 'ASAP LOG'
            WHEN FLAG_EMPRESA = '' AND EMPRESA IN (
                'CASAS BAHIA COMERCIAL LTDA',
                'GLOBEX UTILIDADES S/A (ATUAL VIA VAREJO)',
                'GLOBEX UTILIDADES S/A (CASAS BAHIA)',
                'GLOBEX UTILIDADES S/A (GPA)',
                'INDÚSTRIA DE MÓVEIS BARTIRA LTDA',
                'NOVA CASA BAHIA S/A (ATUAL VIA VAREJO)',
                'GRUPO CASAS BAHIA S/A',
                'VIA VAREJO S/A'
            ) THEN 'LOJA FÍSICA'
            ELSE FLAG_EMPRESA
        END AS FLAG_EMPRESA_
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_10
""")

# Remove a coluna FLAG_EMPRESA original e renomeia FLAG_EMPRESA_ para FLAG_EMPRESA
fecham_civel_consol_10  = fecham_civel_consol_10 .drop("FLAG_EMPRESA").withColumnRenamed("FLAG_EMPRESA_", "FLAG_EMPRESA")

# COMMAND ----------

fecham_civel_consol_10.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_11")

# COMMAND ----------


# Executa o SQL para aplicar as transformações
fecham_civel_consol_11 = spark.sql("""
    SELECT *,
        CASE
            WHEN ASSUNTO_CIVEL_PRI = 'ENTREGA E MONTAGEM' THEN 
                CASE
                    WHEN INSTR(ASSUNTO_CIVEL_ASS, 'MONTAGEM') > 0 THEN 'MONTAGEM'
                    ELSE 'ENTREGA'
                END
            ELSE ASSUNTO_CIVEL_PRI
        END AS DP_ASSUNTO_PRINCIPAL
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_11
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ######2.4.2 Importação arquivos auxiliares (Comarca e Removidos Elaw)

# COMMAND ----------


diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/cível/bases_auxiliares/'
arquivo_comarca = 'DE_PARA_COMARCA.xlsx'
arquivo_removidos_elaw = 'processos_removidos_elaw.xlsx'


path_comarca = diretorio_origem + arquivo_comarca
path_removidos_elaw = diretorio_origem + arquivo_removidos_elaw


# COMMAND ----------

df_comarca = read_excel(path_comarca, "'DE_PARA_COMARCA'!A1")
df_removidos_elaw = read_excel(path_removidos_elaw, "'removidos'!A1")


# COMMAND ----------

# DBTITLE 1,Cleaning and Adjusting Data with Helper Functions
df_comarca = adjust_column_names(df_comarca)
df_removidos_elaw = remove_acentos(df_removidos_elaw).withColumnRenamed("Processo - ID", "PROCESSO_ID")

# COMMAND ----------

fecham_civel_consol_11.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_12")
df_comarca.createOrReplaceTempView("DE_PARA_COMARCA")
df_removidos_elaw.createOrReplaceTempView("PROCESSOS_REMOVIDOS_ELAW")

# COMMAND ----------


# Executa o SQL para aplicar as transformações
fecham_civel_consol_12 = spark.sql("""
    SELECT 
        A.*,
        CASE 
            WHEN B.STATUS IS NOT NULL THEN 'SIM' 
            ELSE 'NÃO' 
        END AS ID_REMOVIDO,
        CASE 
            WHEN C.DP_COMARCA IS NOT NULL THEN TRIM(C.DP_COMARCA)
            ELSE TRIM(A.COMARCA) 
        END AS DP_COMARCA
    FROM 
        TB_FECHAMENTO_CIVEL_CONSOLIDADO_12 AS A
    LEFT JOIN 
       PROCESSOS_REMOVIDOS_ELAW AS B 
    ON 
        A.ID_PROCESSO = B.PROCESSO_ID
    LEFT JOIN 
        DE_PARA_COMARCA AS C 
    ON 
        A.COMARCA = C.COMARCA
""")



# COMMAND ----------

fecham_civel_consol_12.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_13")
#tb_responsabilidade_civel.createOrReplaceTempView("TB_RESPONSABILIDADE_CIVEL")

# COMMAND ----------


# Inclusão da informação do campo Responsabilidade Cível
fecham_civel_consol_13 = spark.sql("""
    SELECT 
        A.*,
        B.`CÍVEL_MASSA_RESPONSABILIDADE` AS RESPONSABILIDADE,
        CASE 
            WHEN FASE IN ('', 'N/A', 'INATIVO', 'INICIAL / CONHECIMENTO', 'INSTRUTÓRIA / PROBATÓRIA', 'INSTRUÇÃO/PROBATÓRIA', 'ENCERRADO / ARQUIVADO', 'ENCERRAMENTO', 'ADMINISTRATIVO') THEN 'DEMAIS'
            WHEN FASE IN ('EXECUÇÃO', 'EXECUÇÃO DEFINITIVA', 'EXECUÇÃO PROVISORIA (TRT)', 'EXECUÇÃO PROVISORIA (TST)') THEN 'EXECUÇÃO'
            WHEN FASE IN ('RECURSAL', 'RECURSAL TRT', 'RECURSAL TST') THEN 'RECURSAL'
            ELSE FASE 
        END AS DP_FASE,
        CASE 
            WHEN ENCERRADOS = 1 THEN CAST(DATEDIFF(MES_FECH, MES_CADASTRO) AS STRING)
            WHEN ESTOQUE = 1 THEN CAST(DATEDIFF(MES_FECH, MES_CADASTRO) AS STRING)
            ELSE '' 
        END AS FX_ANO_AGING_NOVO
    FROM 
        TB_FECHAMENTO_CIVEL_CONSOLIDADO_13 AS A
    LEFT JOIN 
        databox.juridico_comum.TB_RESPONSABILIDADE_CIVEL AS B 
    ON 
        A.ID_PROCESSO = B.`PROCESSO_ID`
""")


# COMMAND ----------

fecham_civel_consol_13.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_14")

# COMMAND ----------

from pyspark.sql.functions import expr, col, when

# Executa o SQL para aplicar as transformações
tb_fecham_financ_civel_f = spark.sql("""
    SELECT 
        *,
        CASE 
            WHEN ASSUNTO_CIVEL_PRI IN ('VENDA DE CARTEIRA DE CRÉDITO', 'VENDA DE CARTEIRA DE CRÉDITO (CHEQUES)') THEN 'NÃO SE APLICA'
            ELSE FLAG_EMPRESA
        END AS FLAG_EMPRESA_
    FROM 
        TB_FECHAMENTO_CIVEL_CONSOLIDADO_14
""")

# COMMAND ----------

from pyspark.sql.functions import expr, col, when

# Executa o SQL para aplicar as transformações
tb_fecham_financ_civel_f = spark.sql("""
    SELECT 
        *,
        CASE 
            WHEN ASSUNTO_CIVEL_PRI IN ('VENDA DE CARTEIRA DE CRÉDITO', 'VENDA DE CARTEIRA DE CRÉDITO (CHEQUES)') THEN 'NÃO SE APLICA'
            ELSE FLAG_EMPRESA
        END AS FLAG_EMPRESA_
    FROM 
        TB_FECHAMENTO_CIVEL_CONSOLIDADO_14
""")

# Remove a coluna FLAG_EMPRESA original e renomeia FLAG_EMPRESA_ para FLAG_EMPRESA
tb_fecham_financ_civel_f = tb_fecham_financ_civel_f.drop("FLAG_EMPRESA").withColumnRenamed("FLAG_EMPRESA_", "FLAG_EMPRESA")

# Remove duplicatas
tb_fecham_financ_civel_f = tb_fecham_financ_civel_f.dropDuplicates(["ID_PROCESSO", "MES_FECH", "NOVOS", "ENCERRADOS", "ESTOQUE"])

# Agora tb_fecham_financ_civel_f é o DataFrame final transformado

# COMMAND ----------

# MAGIC %md
# MAGIC ###3-Base final para a geração do indicador Cível Consolidado

# COMMAND ----------

tb_fecham_financ_civel_f.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_15")

# COMMAND ----------

tb_fecham_financ_civel_f_f = spark.sql(f"""SELECT
    CAST(MES_FECH AS DATE),
    ID_PROCESSO,
    AREA_DO_DIREITO,
    EMPRESA,
    FLAG_EMPRESA,
    DP_ASSUNTO_PRINCIPAL,
    ESCRITORIO,
    DP_FASE,
    MOTIVO_ENC_AGRP,
    ESTADO,
    FX_MES_AGING_ENCERR,
    MES_FATO_GERADOR,
    MESES_FATOGER_ENTR,
    FX_ANO_AGING_NOVO,
    INDICACAO_PROCESSO_ESTRATEGICO,
    RESPONSABILIDADE,
    CASE
        WHEN PROVISAO_TOTAL_PASSIVO_M > 0 THEN 'PASSÍVEL PROVISÃO'
        WHEN AREA_DO_DIREITO = 'CÍVEL ESTRATÉGICO' THEN 'ESTRATÉGICO'
        WHEN RESPONSABILIDADE = 'TERCEIRO' THEN 'TERCEIRO'
        WHEN INDICACAO_PROCESSO_ESTRATEGICO = 'DEFESA' THEN 'DEFESA'
        ELSE 'SEM PROVISÃO'
    END AS DP_NATUREZA,
    SUM(PROVISAO_TOTAL_PASSIVO_M) AS PROVISAO_TOTAL_PASSIVO_M,
    SUM(CAST(COALESCE(NOVOS, 0) AS INTEGER)) AS NOVOS,
    SUM(CAST(COALESCE(ENCERRADOS, 0) AS INTEGER)) AS ENCERRADOS,
    SUM(CAST(ESTOQUE AS INTEGER)) AS ESTOQUE,
    SUM(CAST(ACORDOS AS FLOAT)) AS ACORDO,
    SUM(CAST(CONDENACAO AS DOUBLE)) AS CONDENACAO,
    SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS,
    SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M
FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_15
WHERE MES_FECH >= '2023-01-01'
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
""")

# COMMAND ----------

# Drop the duplicate column if it exists
tb_fecham_financ_civel_f = tb_fecham_financ_civel_f.drop("fx_mes_aging_estoq")

nmmes = dbutils.widgets.get("NMES")

# Now write the DataFrame to the table
tb_fecham_financ_civel_f.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"databox.juridico_comum.tb_fecham_financ_civel_{nmmes}")

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

nmmes = dbutils.widgets.get("NMES")

# Converter PySpark DataFrame para Pandas DataFrame
tb_fecham_financ_civel_f_2_pandas = tb_fecham_financ_civel_f_f.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/tb_fecham_financ_civel_{nmmes}_2.xlsx'
tb_fecham_financ_civel_f_2_pandas.to_excel(local_path, index=False, sheet_name='FECHAMENTO')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_auxiliares/tb_fecham_financ_civel_f_{nmmes}_2.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

tb_fecham_financ_civel_f.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_15")

# COMMAND ----------

tb_fecham_financ_civel_f_f.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_16")
tb_fecham_financ_civel_f_f.createOrReplaceTempView("TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL")


# COMMAND ----------

civel_total = spark.sql("""
    SELECT  MES_FECH
            ,SUM(CAST(NOVOS AS INT)) AS NOVOS
            ,SUM(CAST(ENCERRADOS AS INT)) AS ENCERRADOS 
            ,SUM(CAST(ESTOQUE AS INT)) AS ESTOQUE 
            ,SUM(ACORDO) AS ACORDO 
            ,SUM(CONDENACAO) AS CONDENACAO 
            ,SUM(CAST(TOTAL_PAGAMENTOS AS FLOAT)) AS TOTAL_PAGAMENTOS 
            ,SUM(PROVISAO_TOTAL_PASSIVO_M) AS PROVISAO_TOTAL_PASSIVO_M 
            ,SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M 
            
        FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
        GROUP BY MES_FECH
    ORDER BY 1
""")

# COMMAND ----------

display(civel_total)

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Indicadores Agregados 

# COMMAND ----------

#Delimitação do período da base a ser gerada (start_date & end_date) - Fazer a seleção no Widget

from datetime import datetime, timedelta

# Função para gerar a lista de meses no formato YYYY-MM-01
def generate_months(start_date, end_date):
    months = []
    current_date = start_date
    while current_date <= end_date:
        months.append(current_date.strftime('%Y-%m-01'))
        # Avança para o próximo mês
        next_month = current_date + timedelta(days=31)
        current_date = datetime(next_month.year, next_month.month, 1)
    return months

# Define o intervalo de datas
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 8, 1)

# Gera a lista de meses
months = generate_months(start_date, end_date)


# COMMAND ----------

from pyspark.sql import Row

# Cria a parte da consulta SQL para cada variável e mês
pivot_sql_parts_novos = [
    f"SUM(CASE WHEN MES_FECH = '{month}' THEN NOVOS ELSE 0 END) AS `{month}`"
    for month in months
]

pivot_sql_parts_encerrados = [
    f"SUM(CASE WHEN MES_FECH = '{month}' THEN ENCERRADOS ELSE 0 END) AS `{month}`"
    for month in months
]

pivot_sql_parts_provisao = [
    f"SUM(CASE WHEN MES_FECH = '{month}' THEN PROVISAO_TOTAL_PASSIVO_M ELSE 0 END) AS `{month}`"
    for month in months
]

pivot_sql_parts_estoque = [
    f"SUM(CASE WHEN MES_FECH = '{month}' THEN ESTOQUE ELSE 0 END) AS `{month}`"
    for month in months
]

pivot_sql_parts_total_pagamentos = [
    f"SUM(CASE WHEN MES_FECH = '{month}' THEN CAST(TOTAL_PAGAMENTOS AS DOUBLE) ELSE 0 END) AS `{month}`"
    for month in months
]

pivot_sql_parts_provisao_mov_m = [
    f"SUM(CASE WHEN MES_FECH = '{month}' THEN PROVISAO_MOV_M ELSE 0 END) AS `{month}`"
    for month in months
]

# Cria a consulta SQL para cada conjunto de dados
pivot_sql_novos = f"""
    SELECT
        'NOVOS' AS Indicador,
        {', '.join(pivot_sql_parts_novos)}
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
"""

pivot_sql_encerrados = f"""
    SELECT
        'ENCERRADOS' AS Indicador,
        {', '.join(pivot_sql_parts_encerrados)}
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
"""

pivot_sql_provisao = f"""
    SELECT
        'PROVISAO_TOTAL_PASSIVO_M' AS Indicador,
        {', '.join(pivot_sql_parts_provisao)}
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
"""

pivot_sql_estoque = f"""
    SELECT
        'ESTOQUE' AS Indicador,
        {', '.join(pivot_sql_parts_estoque)}
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
"""

pivot_sql_total_pagamentos = f"""
    SELECT
        'TOTAL_PAGAMENTOS' AS Indicador,
        {', '.join(pivot_sql_parts_total_pagamentos)}
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
"""

pivot_sql_provisao_mov_m = f"""
    SELECT
        'PROVISAO_MOV_M' AS Indicador,
        {', '.join(pivot_sql_parts_provisao_mov_m)}
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
"""

# Cria a consulta SQL para ENCERRADOS por MOTIVO_ENC_AGRP
pivot_sql_encerrados_motivo = f"""
    SELECT
        MOTIVO_ENC_AGRP AS Indicador,
        {', '.join(pivot_sql_parts_encerrados)}
    FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
    WHERE MOTIVO_ENC_AGRP IN ('ACORDO', 'CONDENACAO', 'SEM ONUS')
    GROUP BY MOTIVO_ENC_AGRP
    ORDER BY MOTIVO_ENC_AGRP
"""

# Ajuste na consulta do Ticket Médio Cível (foco no JOIN correto)
pivot_sql_ticket_medio = f"""
SELECT
    'TICKET_MEDIO_CIVEL' AS Indicador,
    {', '.join([
        f"CASE WHEN ENCERRADOS.`{month}` > 0 THEN TOTAL_PAGAMENTOS.`{month}` / ENCERRADOS.`{month}` ELSE 0 END AS `{month}`"
        for month in months
    ])}
FROM
    ({pivot_sql_encerrados}) ENCERRADOS
JOIN
    ({pivot_sql_total_pagamentos}) TOTAL_PAGAMENTOS
ON 1=1
"""

# Ajuste na consulta para calcular o Ticket Médio Cível para Acordo
pivot_sql_ticket_medio_acordo = f"""
SELECT
    'TICKET_MEDIO_CIVEL_ACORDO' AS Indicador,
    {', '.join([
        f"CASE WHEN ENC_ACORDO.`{month}` > 0 THEN PAG_ACORDO.`{month}` / ENC_ACORDO.`{month}` ELSE 0 END AS `{month}`"
        for month in months
    ])}
FROM
    (
        SELECT
            {', '.join(pivot_sql_parts_encerrados)},
            'ACORDO' AS MOTIVO_ENC_AGRP
        FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
        WHERE MOTIVO_ENC_AGRP = 'ACORDO'
    ) ENC_ACORDO
JOIN
    (
        SELECT
            {', '.join(pivot_sql_parts_total_pagamentos)},
            'ACORDO' AS MOTIVO_ENC_AGRP
        FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
        WHERE MOTIVO_ENC_AGRP = 'ACORDO'
    ) PAG_ACORDO
ON ENC_ACORDO.MOTIVO_ENC_AGRP = PAG_ACORDO.MOTIVO_ENC_AGRP
"""

# Ajuste na consulta para calcular o Ticket Médio Cível para Condenação
pivot_sql_ticket_medio_condenacao = f"""
SELECT
    'TICKET_MEDIO_CIVEL_CONDENACAO' AS Indicador,
    {', '.join([
        f"CASE WHEN ENC_CONDENACAO.`{month}` > 0 THEN PAG_CONDENACAO.`{month}` / ENC_CONDENACAO.`{month}` ELSE 0 END AS `{month}`"
        for month in months
    ])}
FROM
    (
        SELECT
            {', '.join(pivot_sql_parts_encerrados)},
            'CONDENACAO' AS MOTIVO_ENC_AGRP
        FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
        WHERE MOTIVO_ENC_AGRP = 'CONDENACAO'
    ) ENC_CONDENACAO
JOIN
    (
        SELECT
            {', '.join(pivot_sql_parts_total_pagamentos)},
            'CONDENACAO' AS MOTIVO_ENC_AGRP
        FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
        WHERE MOTIVO_ENC_AGRP = 'CONDENACAO'
    ) PAG_CONDENACAO
ON ENC_CONDENACAO.MOTIVO_ENC_AGRP = PAG_CONDENACAO.MOTIVO_ENC_AGRP
"""


pivot_sql_encerrados_motivo_percentual = f"""
    SELECT
        MOTIVO_ENC_AGRP AS Indicador,
        ROUND(SUM(quantidade) * 100.0 / NULLIF(total.total_encerrados, 0), 2) AS Percentual
    FROM (
        SELECT
            MOTIVO_ENC_AGRP,
            COUNT(*) AS quantidade
        FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
        WHERE MOTIVO_ENC_AGRP IN ('ACORDO', 'CONDENACAO', 'SEM ONUS')
        GROUP BY MOTIVO_ENC_AGRP
    ) AS encerrados
    CROSS JOIN (
        SELECT COUNT(*) AS total_encerrados
        FROM TB_FECHAMENTO_CIVEL_CONSOLIDADO_FINAL
        WHERE MOTIVO_ENC_AGRP IN ('ACORDO', 'CONDENACAO', 'SEM ONUS')
    ) AS total
    GROUP BY MOTIVO_ENC_AGRP, total.total_encerrados
    ORDER BY MOTIVO_ENC_AGRP
"""


# Executa as consultas SQL
df_novos = spark.sql(pivot_sql_novos)
df_encerrados = spark.sql(pivot_sql_encerrados)
df_provisao = spark.sql(pivot_sql_provisao)
df_estoque = spark.sql(pivot_sql_estoque)
df_total_pagamentos = spark.sql(pivot_sql_total_pagamentos)
df_provisao_mov_m = spark.sql(pivot_sql_provisao_mov_m)
df_encerrados_motivo = spark.sql(pivot_sql_encerrados_motivo)
df_ticket_medio_civel = spark.sql(pivot_sql_ticket_medio)
df_ticket_medio_acordo = spark.sql(pivot_sql_ticket_medio_acordo)
df_ticket_medio_condenacao = spark.sql(pivot_sql_ticket_medio_condenacao)
df_encerrados_motivo_percentual = spark.sql(pivot_sql_encerrados_motivo_percentual)

# Renomeia as colunas para remover o prefixo e deixar apenas as datas
new_column_names = ['Indicadores Cível'] + [month for month in months]
df_novos = df_novos.toDF(*new_column_names)
df_encerrados = df_encerrados.toDF(*new_column_names)
df_provisao = df_provisao.toDF(*new_column_names)
df_estoque = df_estoque.toDF(*new_column_names)
df_total_pagamentos = df_total_pagamentos.toDF(*new_column_names)
df_provisao_mov_m = df_provisao_mov_m.toDF(*new_column_names)
df_encerrados_motivo = df_encerrados_motivo.toDF(*new_column_names)
df_ticket_medio_civel = df_ticket_medio_civel.toDF(*new_column_names)
df_ticket_medio_acordo = df_ticket_medio_acordo.toDF(*new_column_names)
df_ticket_medio_condenacao = df_ticket_medio_condenacao.toDF(*new_column_names)
df_encerrados_motivo_percentual = df_encerrados_motivo_percentual.toDF('Indicadores Cível', 'Percentual')

# Combine as DataFrames em uma única DataFrame final
df_final = df_novos.union(df_encerrados).union(df_provisao).union(df_estoque)\
    .union(df_total_pagamentos).union(df_provisao_mov_m).union(df_encerrados_motivo)\
    .union(df_ticket_medio_civel).union(df_ticket_medio_acordo).union(df_ticket_medio_condenacao).union(df_encerrados_motivo_percentual)

# Exibe o resultado final
#df_final.show(truncate=False)


# COMMAND ----------

display(df_final)
