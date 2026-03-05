# Databricks notebook source
# MAGIC %run
# MAGIC "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

dbutils.widgets.text("nm_base", "", "Data fechamento")
dbutils.widgets.text("dt_garantias", "", "Data histórico de garantias")
dbutils.widgets.text("dt_pagamentos", "", "Data histórico de pagamentos")
dbutils.widgets.text("dt_mensal_pagamentos", "", "Data mensal de pagamentos")
dbutils.widgets.dropdown("tipo_arquvivo", "Prévia", ["Prévia", "Fechamento"], "Tipo de aquivo")

# COMMAND ----------

# Listar todos os arquivos dentro do diretório especificado
from pyspark.sql.functions import lit

nm_base = dbutils.widgets.get("nm_base")
dt_garantias = dbutils.widgets.get("dt_garantias")
dt_pagamentos =  dbutils.widgets.get("dt_pagamentos")
dt_mensal_pagamentos =  dbutils.widgets.get("dt_mensal_pagamentos")
tipo_arquvivo = dbutils.widgets.get("tipo_arquvivo")

# Caminho do diretório
base_provisao = f"/Volumes/databox/juridico_comum/arquivos/financeiro/bases de provisão/Input/{tipo_arquvivo} Cível_{nm_base}.xlsx"
base_historica_garantia = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_{dt_garantias}.xlsx'
base_historica_pgto = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{dt_pagamentos}.xlsx'
base_mensal_pgto = f'/Volumes/databox/juridico_comum/arquivos/financeiro/base mensal de pagamentos/Pagamentos_{dt_mensal_pagamentos}.xlsx'

# COMMAND ----------

# base auxiliar com o de para de cluster de valor
validador = False
x = 0

# while validador == False:
#     x += 1
#     df_base_provisao = read_excel(base_provisao, f"'FINAL'!A{x}:BV1048576")
#     df_base_provisao.columns = df_base_provisao.columns.str.upper()

#     if 'LINHA' in df_base_provisao.columns:
#         validador = True
#         # Verificar se a coluna 'LINHA' está presente
#         validador = 'LINHA' in df_base_provisao.columns
    
#     else:
#         pass
df_base_provisao = read_excel(base_provisao, f"'FINAL'!A4:BV1048576")
df_base_mensal_pgto = read_excel(base_mensal_pgto, "'PJ - PAGAMENTOS - GERENCIAL_PAG'!A6")
df_base_historica_pgto = read_excel(base_historica_pgto, "'PAGAMENTOS'!A6")
df_base_historica_garantia = read_excel(base_historica_garantia, "'gerencial_garantias'!A6")

# COMMAND ----------

df_base_provisao.createOrReplaceTempView("TABELA_BASE_PROVISAO")

df_base_provisao_1 = spark.sql(f"""
/* primeira etapa: aplica as marcações de novo, reativado, encerrados e estoque na base de provisão */
SELECT *
	,(CASE WHEN `LINHA` = 'linha03' OR `STATUS (M-1)` IN ('BAIXA PROVISORIA','REMOVIDO','ENCERRADO') AND `STATUS (M)` IN ('ATIVO', 'REATIVADO') THEN 1 ELSE NULL END) AS NOVOS
 
    ,(CASE WHEN `LINHA` = 'linha03' AND `STATUS (M)` IN ('BAIXA PROVISORIA', 'ENCERRADO', 'REMOVIDO') OR `LINHA` IN ('linha02', 'linha03', 'linha00') AND `STATUS (M-1)` IN ('ATIVO', 'REATIVADO') AND `STATUS (M)` IN ('BAIXA PROVISORIA', 'ENCERRADO', 'REMOVIDO', 'BAIXA PAGAMENTO') THEN 1 ELSE NULL END) AS ENCERRADOS
    
    , (CASE WHEN `LINHA` IN ('linha00', 'linha02', 'linha03') AND `STATUS (M)` IN ('ATIVO', 'REATIVADO') THEN 1 END) AS ESTOQUE

	FROM TABELA_BASE_PROVISAO AS A; 
""")

df_base_provisao_1 = df_base_provisao_1.withColumnRenamed("Id do Processo", "ID PROCESSO")

# COMMAND ----------

df_base_mensal_pgto = df_base_mensal_pgto.withColumnRenamed("ID DO PROCESSO", "ID PROCESSO")

df_base_mensal_pgto.createOrReplaceTempView("TB_BASE_MENSAL_PGTO")

df_base_mensal_pgto_1 = spark.sql("""
/* segunda etapa: faz os filtros sobre a área do direito, a responsabilidade, o status de pagamento e o sub tipo */
SELECT A.*
FROM TB_BASE_MENSAL_PGTO AS A
WHERE 
(
    `ÁREA DO DIREITO` IN ('CÍVEL MASSA', 'CÍVEL ESTRATÉGICO') 
    AND `STATUS DO PAGAMENTO` NOT IN ('Pendente','REJEITADO', 'CANCELADO', 'EM CORREÇÃO','PAGAMENTO', 'DEVOLVIDO', 'PAGAMENTO DEVOLVIDO', 'EM LEVANTAMENTO')
    AND `Processo - CÍVEL MASSA - RESPONSABILIDADE` IN ('TERCEIRO') 
    AND `SUB TIPO` IN ('ACORDO - CÍVEL','CONDENAÇÃO - CÍVEL ESTRATÉGICO','ACORDO - CÍVEL ESTRATÉGICO','CONDENAÇÃO - CÍVEL','LIBERAÇÃO DE PENHORA - MASSA','MULTA PROCESSUAL - CÍVEL','LIBERAÇÃO DE PENHORA MASSA','LIBERAÇÃO DE PENHORA - CÍVEL ESTRATÉGICO','MULTA PROCESSUAL - CÍVEL ESTRATÉGICO','CONDENAÇÃO - CÍVEL MASSA','ACORDO - CÍVEL MASSA','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)','PENSÃO - CÍVEL')
)
OR 
(
    `ÁREA DO DIREITO` IN ('CÍVEL MASSA', 'CÍVEL ESTRATÉGICO') 
    AND `STATUS DO PAGAMENTO` NOT IN ('Pendente','REJEITADO', 'CANCELADO', 'EM CORREÇÃO','PAGAMENTO', 'DEVOLVIDO', 'PAGAMENTO DEVOLVIDO', 'EM LEVANTAMENTO') 
    AND (`Processo - CÍVEL MASSA - RESPONSABILIDADE` IN ('GRUPO CASAS BAHIA', 'SEM AVALIAÇÃO', 'SOLIDÁRIA') OR `Processo - CÍVEL MASSA - RESPONSABILIDADE` IS NULL)
    AND `SUB TIPO` IN ('ACORDO - CÍVEL','CONDENAÇÃO - CÍVEL ESTRATÉGICO','ACORDO - CÍVEL ESTRATÉGICO','CONDENAÇÃO - CÍVEL','LIBERAÇÃO DE PENHORA - MASSA','MULTA PROCESSUAL - CÍVEL','LIBERAÇÃO DE PENHORA MASSA','LIBERAÇÃO DE PENHORA - CÍVEL ESTRATÉGICO','MULTA PROCESSUAL - CÍVEL ESTRATÉGICO','CONDENAÇÃO - CÍVEL MASSA','ACORDO - CÍVEL MASSA','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)','PENSÃO - CÍVEL'))
""")

# COMMAND ----------

df_base_mensal_pgto_1.createOrReplaceTempView("TB_BASE_MENSAL_PGTO_1")

df_base_mensal_pgto_2 = spark.sql("""
/* terceira etapa: filtra o tipo de pagamento, criando sua coluna ao final da tabela */
SELECT *
,(CASE WHEN `SUB TIPO` IN ('CONDENAÇÃO - CÍVEL ESTRATÉGICO','CONDENAÇÃO - CÍVEL','LIBERAÇÃO DE PENHORA - MASSA','MULTA PROCESSUAL - CÍVEL','LIBERAÇÃO DE PENHORA MASSA','LIBERAÇÃO DE PENHORA - CÍVEL ESTRATÉGICO','MULTA PROCESSUAL - CÍVEL ESTRATÉGICO','CONDENAÇÃO - CÍVEL MASSA') THEN 'Condenacao'

    WHEN `SUB TIPO` IN ('CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)') THEN 'Condenacao Incontroverso'
 
    WHEN `SUB TIPO` IN ('ACORDO - CÍVEL','ACORDO - CÍVEL ESTRATÉGICO','ACORDO - CÍVEL MASSA') THEN 'Acordo' END) AS TIPO_PGTO

FROM TB_BASE_MENSAL_PGTO_1 ;
""")

# COMMAND ----------

df_base_mensal_pgto_2.createOrReplaceTempView("TB_BASE_MENSAL_PGTO_2")

df_base_mensal_pgto_3 = spark.sql("""
/* quarta etapa: filtra o tipo de pagamento, criando sua coluna ao final da tabela */
SELECT *,
(CASE WHEN `SUB TIPO` IN ('CONDENAÇÃO - CÍVEL ESTRATÉGICO','CONDENAÇÃO - CÍVEL','LIBERAÇÃO DE PENHORA - MASSA','MULTA PROCESSUAL - CÍVEL','LIBERAÇÃO DE PENHORA MASSA','LIBERAÇÃO DE PENHORA - CÍVEL ESTRATÉGICO','MULTA PROCESSUAL - CÍVEL ESTRATÉGICO','CONDENAÇÃO - CÍVEL MASSA','CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR','PENSÃO - CÍVEL','CONDENAÇÃO - CÍVEL MASSA - MKTPLACE','CONDENAÇÃO - CÍVEL MASSA - CARTÕES','CONDENAÇÃO - CÍVEL MASSA - SEGURO OU SERVIÇO','CONDENAÇÃO - CÍVEL MASSA - B2B') THEN 'Condenacao'
    WHEN `SUB TIPO` IN ('CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO OU SERVIÇO') THEN 'Condenacao Incontroverso'
    WHEN `SUB TIPO` IN ('ACORDO - CÍVEL','ACORDO - CÍVEL ESTRATÉGICO','ACORDO - CÍVEL MASSA','ACORDO - CÍVEL MASSA - FORNECEDOR','ACORDO - CÍVEL MASSA - MKTPLACE','ACORDO - CÍVEL MASSA - SEGURO OU SERVIÇO','ACORDO - CÍVEL MASSA - CARTÕES','ACORDO - CÍVEL MASSA - B2B') THEN 'Acordo' END) AS TIPO_PGTO
FROM TB_BASE_MENSAL_PGTO_2 ;
""")

# COMMAND ----------

df_base_mensal_pgto_2.createOrReplaceTempView("TB_BASE_MENSAL_PGTO_2")

df_base_mensal_pgto_3 = spark.sql(f"""
/* quinta etapa: agrupa as colunas do sub tipo para não duplicar na base de provisão */ 
SELECT
    `ID PROCESSO`,
    MIN(`SUB TIPO`) AS `SUB TIPO`,
    MIN(`TIPO_PGTO`) AS `TIPO_PGTO`,
    MIN(`PARCELAMENTO ACORDO`) AS `PARCELAMENTO ACORDO`,
    MIN(`PARCELAMENTO CONDENAÇÃO`) AS `PARCELAMENTO CONDENAÇÃO`,
    SUM(`VALOR`) AS SUM_of_VALOR
FROM
    TB_BASE_MENSAL_PGTO_2
GROUP BY 1
""")

# COMMAND ----------

df_base_provisao_1.createOrReplaceTempView("TB_BASE_PROVISAO_2")
df_base_mensal_pgto_3.createOrReplaceTempView("TB_BASE_MENSAL_PGTO_3")

df_base_provisao_2 = spark.sql(f"""
/* sexta etapa: cruza a base de provisão com as marcações da primeira etapa com as etapas 2, 3 e 4 */ 
SELECT
    A.*,
    B.`SUB TIPO`,
    B.`TIPO_PGTO`,
    B.`PARCELAMENTO ACORDO`,
    B.`PARCELAMENTO CONDENAÇÃO`,
    B.`SUM_of_VALOR`
FROM
    TB_BASE_PROVISAO_2 AS A
    LEFT JOIN TB_BASE_MENSAL_PGTO_3 AS B ON A.`ID PROCESSO` = B.`ID PROCESSO`
""")

# COMMAND ----------

df_base_provisao_2.createOrReplaceTempView("TB_BASE_PROVISAO_3")

df_base_provisao_3 = spark.sql("""
/* sétima etapa: cria as colunas outras_adições e outras_reversões aplicando suas regras de marcação*/ 
SELECT *, 
CASE 
    WHEN `NOVOS` IS NULL AND `ENCERRADOS` IS NULL AND `Provisão Mov. (M)` > 0 AND `STATUS (M)` IN ('ATIVO','REATIVADO') THEN 1 
    ELSE NULL 
END AS OUTRAS_ADICOES,
CASE 
    WHEN `NOVOS` IS NULL AND `ENCERRADOS` IS NULL AND `Provisão Mov. (M)` < 0 AND `STATUS (M)` IN ('ATIVO','REATIVADO') THEN 1 
    ELSE NULL 
END AS OUTRAS_REVERSOES
FROM TB_BASE_PROVISAO_3
""")

# COMMAND ----------

df_base_historica_pgto.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO")

df_base_historica_pgto_1 = spark.sql("""
/* oitava etapa: faz os filtros sobre a área do direito, o status de pagamento e o sub tipo na base histórica de pagamentos */
SELECT A.*
FROM TB_BASE_HISTORICA_PGTO AS A
WHERE `ÁREA DO DIREITO` IN ('CÍVEL MASSA', 'CÍVEL ESTRATÉGICO') 
AND `STATUS DO PAGAMENTO` NOT IN ('Pendente','REJEITADO', 'CANCELADO', 'EM CORREÇÃO','PAGAMENTO', 'DEVOLVIDO', 'PAGAMENTO DEVOLVIDO', 'EM LEVANTAMENTO') 
AND `SUB TIPO` NOT IN ('CUSTAS PROCESSUAIS - CÍVEL','CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO','CUSTAS PERITOS - CÍVEL','HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO','CUSTAS PROCESSUAIS - REGULATÓRIO','CUSTAS PROCESSUAIS - CÍVEL MASSA','CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR','CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE','HONORÁRIOS PERICIAIS - CÍVEL MASSA','CUSTAS PROCESSUAIS - CÍVEL MASSA - SEGURO OU SERVIÇO','CUSTAS PROCESSUAIS - CÍVEL MASSA - CARTÕES','HONORÁRIOS PERICIAIS - CÍVEL MASSA - FORNECEDOR','HONORÁRIOS CONCILIADOR - CIVEL MASSA','HONORÁRIOS CONCILIADOR - CIVEL MASSA - SEGURO OU SERVIÇO','HONORÁRIOS PERICIAIS - CÍVEL MASSA - CARTÕES','HONORÁRIOS PERICIAIS - CÍVEL MASSA - SEGURO OU SERVIÇO','LIMINAR (CÍV. MASSA)','HONORÁRIOS PERICIAIS - CÍVEL MASSA - MKTPLACE','HONORÁRIOS CONCILIADOR - CIVEL MASSA - FORNECEDOR','HONORÁRIOS CONCILIADOR - CIVEL MASSA - CARTÕES','HONORÁRIOS CONCILIADOR - CIVEL MASSA - MKTPLACE','CUSTAS PROCESSUAIS - CÍVEL MASSA - B2B','HONORÁRIOS CONCILIADOR - CIVEL MASSA - B2B')
""")

# COMMAND ----------

df_base_historica_pgto_1.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO_1")

df_base_historica_pgto_2 = spark.sql("""
/* nona etapa: filtra o tipo de pagamento, criando a coluna tipo_ptgo ao final da tabela */
SELECT *
,(CASE 
    WHEN `SUB TIPO` IN ('ACORDO - CÍVEL','ACORDO - CÍVEL ESTRATÉGICO','ACORDO - CÍVEL ESTRATÉGICO','ACORDO - CÍVEL MASSA','ACORDO - CÍVEL MASSA - FORNECEDOR','ACORDO - CÍVEL MASSA - MKTPLACE','ACORDO - CÍVEL MASSA - SEGURO OU SERVIÇO','ACORDO - CÍVEL MASSA - CARTÕES','ACORDO - CÍVEL MASSA - B2B') THEN 'Acordo'
    WHEN `SUB TIPO` IN ('CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO OU SERVIÇO','CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) -  B2B') THEN 'Condenacao Incontroverso'
    ELSE 'Condenacao' 
END) AS TIPO_PGTO
FROM TB_BASE_HISTORICA_PGTO_1
""")

# COMMAND ----------

df_base_historica_pgto_2.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO_2")

df_base_historica_pgto_3 = spark.sql(f"""
/* décima etapa: agrupa as colunas do sub tipo para não duplicar na base de provisão */ 
SELECT
    `PROCESSO - ID`,
    MIN(`SUB TIPO`) AS `SUB_TIPO`,
    MIN(`TIPO_PGTO`) AS `TIPO_PGTO`,
    MIN(`PARCELAMENTO ACORDO`) AS `PARCELAMENTO ACORDO`,
    MIN(`PARCELAMENTO CONDENAÇÃO`) AS `PARCELAMENTO CONDENAÇÃO`
FROM
    TB_BASE_HISTORICA_PGTO_2
GROUP BY `PROCESSO - ID`
""")

# COMMAND ----------

df_base_historica_pgto_3.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO_3")
df_base_provisao_3.createOrReplaceTempView("TB_BASE_PROVISAO_4")
df_base_provisao_4 = spark.sql("""
/* décima primeira etapa: marca o sub_tipo, tipo_pgto, parcelamento acordo e parcelamento condenação nas linhas que foram encerradas e que não receberam pagamento no mês */ 
SELECT
    A.*,
    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL 
        THEN B.`PARCELAMENTO ACORDO` ELSE A.`PARCELAMENTO ACORDO` END ) AS `PARCELAMENTO ACORDO 2`,

    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL 
        THEN B.`PARCELAMENTO CONDENAÇÃO` ELSE A.`PARCELAMENTO CONDENAÇÃO` END ) AS `PARCELAMENTO CONDENAÇÃO 2`,

    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL 
        THEN B.`SUB_TIPO` ELSE A.`SUB TIPO` END ) AS `SUB TIPO 2`,

    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL 
        THEN B.`TIPO_PGTO` ELSE A.`TIPO_PGTO` END ) AS `TIPO_PGTO 2`
FROM
    TB_BASE_PROVISAO_4 AS A
    LEFT JOIN TB_BASE_HISTORICA_PGTO_3 AS B ON A.`ID PROCESSO` = B.`PROCESSO - ID`
""")

df_base_provisao_4 = df_base_provisao_4.drop("PARCELAMENTO ACORDO","PARCELAMENTO CONDENAÇÃO", "SUB TIPO", "TIPO_PGTO" )

df_base_provisao_4 = df_base_provisao_4.withColumnRenamed("PARCELAMENTO ACORDO 2", "PARCELAMENTO ACORDO") \
                                       .withColumnRenamed("PARCELAMENTO CONDENAÇÃO 2", "PARCELAMENTO CONDENAÇÃO") \
                                       .withColumnRenamed("SUB TIPO 2", "SUB_TIPO") \
                                       .withColumnRenamed("TIPO_PGTO 2", "TIPO_PGTO")

# COMMAND ----------

df_base_historica_garantia.createOrReplaceTempView("TB_BASE_HISTORICA_GARANTIA")

df_base_historica_garantia_1 = spark.sql("""
/* décima segunda etapa: faz os filtros sobre a área do direito, o status de pagamento e o sub tipo na base histórica de pagamentos */
SELECT A.*
FROM TB_BASE_HISTORICA_GARANTIA AS A

WHERE `ÁREA DO DIREITO` IN ('CÍVEL MASSA', 'CÍVEL ESTRATÉGICO')
        AND `STATUS DA GARANTIA` NOT IN ('PENDENTE', 'EM LEVANTAMENTO', 'CANCELADO', 'EM CORREÇÃO','AGUARDANDO LANÇAMENTO NO BANCO') 
        AND `TIPO GARANTIA` NOT IN ('BEM MÓVEL - CÍVEL MASSA','SEGURO GARANTIA - CÍVEL MASSA','LEVANTAMENTO DE CRÉDITO - CÍVEL MASSA','SEGURO GARANTIA - CÍVEL','INATIVO','PENHORA - GARANTIA','ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL','CARTA DE FIANÇA - CÍVEL','LEVANTAMENTO DE CRÉDITO','BENS MÓVEIS - TRIBUTÁRIO','CARTA DE FIANÇA - CÍVEL MASSA','IMÓVEL - CÍVEL MASSA','BEM MÓVEL - CÍVEL')
        AND `DATA DE LEVANTAMENTO (PARTE CONTRÁRIA)` IS NOT NULL
""")

# COMMAND ----------

df_base_historica_garantia_1.createOrReplaceTempView("TB_BASE_HISTORICA_GARANTIA_1")

df_base_historica_garantia_2 = spark.sql("""
/* décima terceira etapa: cria a coluna tipo_ptgo ao final da tabela */
SELECT *,
       (CASE WHEN `ÁREA DO DIREITO` IN ('CÍVEL MASSA', 'CÍVEL ESTRATÉGICO') THEN 'Condenacao' END) AS TIPO_PGTO
FROM TB_BASE_HISTORICA_GARANTIA_1
""")

# COMMAND ----------

df_base_historica_garantia_2.createOrReplaceTempView("TB_BASE_HISTORICA_GARANTIA_2")

df_base_historica_garantia_3 = spark.sql(f"""
/* décima quarta etapa: remove processo id repetido */ 
SELECT
    `(PROCESSO) ID`,
    MIN(`TIPO_PGTO`) AS `TIPO_PGTO`,
    MIN(`STATUS DA GARANTIA`) AS `STATUS DA GARANTIA`,
    MIN(`TIPO GARANTIA`) AS `SUB TIPO`
FROM
    TB_BASE_HISTORICA_GARANTIA_2
GROUP BY `(PROCESSO) ID`
""")

# COMMAND ----------

df_base_historica_garantia_3.createOrReplaceTempView("TB_BASE_HISTORICA_GARANTIA_3")
df_base_provisao_4.createOrReplaceTempView("TB_BASE_PROVISAO_4")
df_base_provisao_5 = spark.sql("""
/* décima quinta etapa: marca o sub_tipo, tipo_pgto de garantia nas linhas que foram encerradas e que não receberam pagamento no mês */ 
SELECT
    A.*,
    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL AND A.`TIPO_PGTO` NOT IN ('Acordo', 'Condenacao')
        THEN B.`TIPO_PGTO` ELSE A.`TIPO_PGTO` END ) AS `TIPO_PGTO 1`,

    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL AND A.`TIPO_PGTO` NOT IN ('Acordo', 'Condenacao')
        THEN B.`SUB TIPO` ELSE A.`SUB_TIPO` END ) AS `SUB TIPO 1`
FROM
    TB_BASE_PROVISAO_4 AS A
    LEFT JOIN TB_BASE_HISTORICA_GARANTIA_3 AS B ON A.`ID PROCESSO` = B.`(PROCESSO) ID`
""")

df_base_provisao_5 = df_base_provisao_5.drop("SUB_TIPO", "TIPO_PGTO" )

df_base_provisao_5 = df_base_provisao_5.withColumnRenamed("SUB TIPO 1", "SUB_TIPO") \
                                       .withColumnRenamed("TIPO_PGTO 1", "TIPO_PGTO")

# COMMAND ----------

df_base_provisao_5.createOrReplaceTempView("TB_BASE_PROVISAO_5")
df_base_provisao_6 = spark.sql("""
/* décima sexta etapa: marca os encerramentos sem ônus*/ 
SELECT
    *,
    (CASE 
    WHEN ENCERRADOS IS NOT NULL AND SUM_of_VALOR IS NULL AND TIPO_PGTO IS NULL
    THEN 'Sem Onus' ELSE TIPO_PGTO END ) AS `TIPO_PGTO 2`
FROM
    TB_BASE_PROVISAO_5
""")

df_base_provisao_6 = df_base_provisao_6.drop("TIPO_PGTO" )
df_base_provisao_6 = df_base_provisao_6.withColumnRenamed("TIPO_PGTO 2", "TIPO_PGTO")

# COMMAND ----------

# Remove espaçoes adjacentes das colunas (COPIAR PARA AS OUTRAS AREAS)
df_base_provisao_6 = adjust_column_names(df_base_provisao_6)

# Altera colunas duplicatas
df_base_provisao_6 = deduplica_cols(df_base_provisao_6)

# COMMAND ----------

df_base_provisao_6 = remove_acentos(df_base_provisao_6)

df_base_provisao_6 = df_base_provisao_6.withColumnRenamed("VLR_CAUSA", "VALOR_DA_CAUSA")

# COMMAND ----------

# Apenas printa as colunas para visualização
c = df_base_provisao_6.columns

print(c)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cria dataframe que contém # 'TIPOS_DE_TERCEIRO' | # 'RESPONSABILIDADE' | # 'ESTRATÉGIA', 

# COMMAND ----------

# Select the columns in the desired order (COPIAR PARA AS OUTRAS AREAS)
df_com_as_mesmas_colunas_da_origem = df_base_provisao_6.select(
    'LINHA',
    'ID_PROCESSO',
    'PASTA',
    'AREA_DO_DIREITO',
    'SUB_AREA_DO_DIREITO',
    'GRUPO_M_1',
    'EMPRESA_M_1',
    'GRUPO_FECHAMENTO',
    'EMPRESA_M',
    'STATUS_M_1',
    'STATUS_M',
    'CENTRO_DE_CUSTO_M_1',
    'CENTRO_DE_CUSTO_M',
    'CADASTRO',
    'REABERTURA',
    'OBJETO_ASSUNTO_CARGO_M_1',
    'SUB_OBJETO_ASSUNTO_CARGO_M_1',
    'OBJETO_ASSUNTO_CARGO_M',
    'SUB_OBJETO_ASSUNTO_CARGO_M', 
    'ÓRGÃO_OFENSOR_FLUXO_M_1', 
    'ÓRGÃO_OFENSOR_FLUXO_M', 
    'NATUREZA_OPERACIONAL_M_1', 
    'NATUREZA_OPERACIONAL_M', 
    'MEDIA_DE_PAGAMENTO', 
    '%_RISCO', 
    'DISTRIBUICAO', 
    'No_PROCESSO', 
    'ESCRITORIO', 
    'VALOR_DA_CAUSA', 
    '%_SOCIO_M_1', 
    '%_EMPRESA_M_1', 
    '%_SOCIO_M', 
    '%_EMPRESA_M',
    'PROVISAO_M_1', 
    'CORRECAO_M_1', 
    'PROVISAO_TOTAL_M_1', 
    'CLASSIFICACAO_MOV_M', 
    'PROVISAO_MOV_M', 
    'CORRECAO_MOV_M', 
    'PROVISAO_MOV_TOTAL_M', 
    'PROVISAO_TOTAL_M', 
    'CORRECAO_M_1', 
    'CORRECAO_M', 
    'PROVISAO_TOTAL_PASSIVO_M', 
    'SOCIO:_PROVISAO_M_1', 
    'SOCIO:_CORRECAO_M_1', 
    'SOCIO:_PROVISAO_TOTAL_M_1', 
    'SOCIO:_CLASSIFICACAO_MOV_M', 
    'SOCIO:_PROVISAO_MOV_M', 
    'SOCIO:_CORRECAO_MOV_M', 
    'SOCIO:_PROVISAO_MOV_TOTAL_M', 
    'SOCIO:_PROVISAO_TOTAL_M', 
    'SOCIO:_CORRECAO_M', 
    'SOCIO:_PROV_TOTAL_PASSIVO_M', 
    'EMPRESA:_PROVISAO_M_1', 
    'EMPRESA:_CORRECAO_M_1', 
    'EMPRESA:_PROVISAO_TOTAL_M_1', 
    'EMPRESA:_CLASSIFICACAO_MOV_M', 
    'EMPRESA:_PROVISAO_MOV_M', 
    'EMPRESA:_CORRECAO_MOV_M', 
    'EMPRESA:_PROVISAO_MOV_TOTAL_M', 
    'EMPRESA:_PROVISAO_TOTAL_M', 
    'EMPRESA:_CORRECAO_M_1', 
    'EMPRESA:_CORRECAO_M', 
    'EMPRESA:_PROV_TOTAL_PASSIVO_M', 
    'DEMITIDO_POR_REESTRUTURACAO', 
    'RESPONSABILIDADE', 
    'ESTRATEGIA', 
    'TIPOS_DE_TERCEIRO',
    'NOVOS', 
    'ENCERRADOS', 
    'ESTOQUE', 
    'SUB_TIPO', 
    'TIPO_PGTO',
    'SUM_OF_VALOR', 
    'OUTRAS_ADICOES', 
    'OUTRAS_REVERSOES'
)

# COMMAND ----------

# Select the columns in the desired order (COPIAR PARA AS OUTRAS AREAS)
df_com_as_mesmas_colunas_da_origem = df_base_provisao_6.select(
    'LINHA',
    'ID_PROCESSO',
    'PASTA',
    'AREA_DO_DIREITO',
    'SUB_AREA_DO_DIREITO',
    'GRUPO_M_1',
    'EMPRESA_M_1',
    'GRUPO_FECHAMENTO',
    'EMPRESA_M',
    'STATUS_M_1',
    'STATUS_M',
    'CENTRO_DE_CUSTO_M_1',
    'CENTRO_DE_CUSTO_M',
    'CADASTRO',
    'REABERTURA',
    'OBJETO_ASSUNTO_CARGO_M_1',
    'SUB_OBJETO_ASSUNTO_CARGO_M_1',
    'OBJETO_ASSUNTO_CARGO_M',
    'SUB_OBJETO_ASSUNTO_CARGO_M', 
    'ORGAO_OFENSOR_FLUXO_M_1', 
    'ORGAO_OFENSOR_FLUXO_M', 
    'NATUREZA_OPERACIONAL_M_1', 
    'NATUREZA_OPERACIONAL_M', 
    'MEDIA_DE_PAGAMENTO', 
    '%_RISCO', 
    'DISTRIBUICAO', 
    'No_PROCESSO', 
    'ESCRITORIO', 
    'VALOR_DA_CAUSA', 
    '%_SOCIO_M_1', 
    '%_EMPRESA_M_1', 
    '%_SOCIO_M', 
    '%_EMPRESA_M',
    'PROVISAO_M_1', 
    'CORRECAO_M_1', 
    'PROVISAO_TOTAL_M_1', 
    'CLASSIFICACAO_MOV_M', 
    'PROVISAO_MOV_M', 
    'CORRECAO_MOV_M', 
    'PROVISAO_MOV_TOTAL_M', 
    'PROVISAO_TOTAL_M', 
    'CORRECAO_M_1', 
    'CORRECAO_M', 
    'PROVISAO_TOTAL_PASSIVO_M', 
    'SOCIO:_PROVISAO_M_1', 
    'SOCIO:_CORRECAO_M_1', 
    'SOCIO:_PROVISAO_TOTAL_M_1', 
    'SOCIO:_CLASSIFICACAO_MOV_M', 
    'SOCIO:_PROVISAO_MOV_M', 
    'SOCIO:_CORRECAO_MOV_M', 
    'SOCIO:_PROVISAO_MOV_TOTAL_M', 
    'SOCIO:_PROVISAO_TOTAL_M', 
    'SOCIO:_CORRECAO_M', 
    'SOCIO:_PROV_TOTAL_PASSIVO_M', 
    'EMPRESA:_PROVISAO_M_1', 
    'EMPRESA:_CORRECAO_M_1', 
    'EMPRESA:_PROVISAO_TOTAL_M_1', 
    'EMPRESA:_CLASSIFICACAO_MOV_M', 
    'EMPRESA:_PROVISAO_MOV_M', 
    'EMPRESA:_CORRECAO_MOV_M', 
    'EMPRESA:_PROVISAO_MOV_TOTAL_M', 
    'EMPRESA:_PROVISAO_TOTAL_M', 
    'EMPRESA:_CORRECAO_M_1', 
    'EMPRESA:_CORRECAO_M', 
    'EMPRESA:_PROV_TOTAL_PASSIVO_M', 
    'DEMITIDO_POR_REESTRUTURACAO', 
    'RESPONSABILIDADE', 
    'ESTRATEGIA', 
    'TIPOS_DE_TERCEIRO',
    'NOVOS', 
    'ENCERRADOS', 
    'ESTOQUE', 
    'SUB_TIPO', 
    'TIPO_PGTO',
    'SUM_OF_VALOR', 
    'OUTRAS_ADICOES', 
    'OUTRAS_REVERSOES'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cria dataframe final com as colunas organizadas do formato a ser utilizado no excel

# COMMAND ----------


df_base_provisao_7 = df_base_provisao_6.orderBy(df_base_provisao_6["ID_PROCESSO"].desc())



# COMMAND ----------

# Select the columns in the desired order (COPIAR PARA AS OUTRAS AREAS)
df_reorganizado = df_base_provisao_7.select(
   'LINHA',
    'ID_PROCESSO',
    'AREA_DO_DIREITO',
    'SUB_AREA_DO_DIREITO',
    'GRUPO_M_1',
    'EMPRESA_M_1',
    'GRUPO_FECHAMENTO',
    'EMPRESA_M',
    'STATUS_M_1',
    'STATUS_M',
    'CENTRO_DE_CUSTO_M_1',
    'CENTRO_DE_CUSTO_M',
    'CADASTRO',
    'REABERTURA',
    'OBJETO_ASSUNTO_CARGO_M_1',
    'SUB_OBJETO_ASSUNTO_CARGO_M_1',
    'OBJETO_ASSUNTO_CARGO_M',
    'SUB_OBJETO_ASSUNTO_CARGO_M', 
    'ORGAO_OFENSOR_FLUXO_M_1', 
    'ORGAO_OFENSOR_FLUXO_M', 
    'NATUREZA_OPERACIONAL_M_1', 
    'NATUREZA_OPERACIONAL_M', 
    'DISTRIBUICAO', 
    'No_PROCESSO', 
    'ESCRITORIO', 
    'VALOR_DA_CAUSA', 
    '%_SOCIO_M_1', 
    '%_EMPRESA_M_1', 
    '%_SOCIO_M', 
    '%_EMPRESA_M',
    'PROVISAO_M_1', 
    'CORRECAO_M_1', 
    'PROVISAO_TOTAL_M_1', 
    'CLASSIFICACAO_MOV_M', 
    'PROVISAO_MOV_M', 
    'CORRECAO_MOV_M', 
    'PROVISAO_MOV_TOTAL_M', 
    'PROVISAO_TOTAL_M', 
    'CORRECAO_M_1', 
    'CORRECAO_M', 
    'PROVISAO_TOTAL_PASSIVO_M', 
    'SOCIO:_PROVISAO_M_1', 
    'SOCIO:_CORRECAO_M_1', 
    'SOCIO:_PROVISAO_TOTAL_M_1', 
    'SOCIO:_CLASSIFICACAO_MOV_M', 
    'SOCIO:_PROVISAO_MOV_M', 
    'SOCIO:_CORRECAO_MOV_M', 
    'SOCIO:_PROVISAO_MOV_TOTAL_M', 
    'SOCIO:_PROVISAO_TOTAL_M', 
    'SOCIO:_CORRECAO_M', 
    'SOCIO:_PROV_TOTAL_PASSIVO_M', 
    'EMPRESA:_PROVISAO_M_1', 
    'EMPRESA:_CORRECAO_M_1', 
    'EMPRESA:_PROVISAO_TOTAL_M_1', 
    'EMPRESA:_CLASSIFICACAO_MOV_M', 
    'EMPRESA:_PROVISAO_MOV_M', 
    'EMPRESA:_CORRECAO_MOV_M', 
    'EMPRESA:_PROVISAO_MOV_TOTAL_M', 
    'EMPRESA:_PROVISAO_TOTAL_M', 
    'EMPRESA:_CORRECAO_M_1', 
    'EMPRESA:_CORRECAO_M', 
    'EMPRESA:_PROV_TOTAL_PASSIVO_M', 
    'DEMITIDO_POR_REESTRUTURACAO', 
    'RESPONSABILIDADE', 
    'ESTRATEGIA', 
    'TIPOS_DE_TERCEIRO',
    'NOVOS', 
    'ENCERRADOS', 
    'ESTOQUE', 
    'SUB_TIPO', 
    'TIPO_PGTO',
    'SUM_OF_VALOR', 
    'OUTRAS_ADICOES', 
    'OUTRAS_REVERSOES'
)

    #'PARCELAMENTO_ACORDO', 
    #'PARCELAMENTO_CONDENAÇÃO',
    #'SUB_TIPO', 
    #'TIPO_PGTO'


# COMMAND ----------


import pandas as pd
import re
from shutil import copyfile
 
# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_reorganizado.toPandas()
 
# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/Base_consolidada_cível.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='BASE')
# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/financeiro/bases de provisão/output/Base_consolidada_cível.xlsx'
 
copyfile(local_path, volume_path)
