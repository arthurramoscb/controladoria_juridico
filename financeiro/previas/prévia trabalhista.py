# Databricks notebook source
# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

Limpar_pasta = False

if Limpar_pasta == True:

    # Caminho para a pasta onde os arquivos estão localizados
    pasta = "/Volumes/databox/juridico_comum/arquivos/financeiro/bases de provisão/Input/"

    # Listar todos os arquivos dentro da pasta
    arquivos = dbutils.fs.ls(pasta)

    # Apagar cada arquivo individualmente
    for arquivo in arquivos:
        if arquivo.isFile():  # Verifica se é um arquivo
            dbutils.fs.rm(arquivo.path)

    print(f'Arquivos excluidos!')
    
else:
    pass

# COMMAND ----------

dbutils.widgets.text("nm_base", "", "Data fechamento")
dbutils.widgets.text("dt_garantias", "", "Data histórico de garantias")
dbutils.widgets.text("dt_pagamentos", "", "Data histórico de pagamentos")
dbutils.widgets.text("dt_mensal_pagamentos", "", "Data mensal de pagamentos")
dbutils.widgets.text("dt_gerencial", "", "data do arquivo gerencial")
dbutils.widgets.dropdown("tipo_arquvivo", "Prévia", ["Prévia", "Fechamento"], "Tipo de aquivo")

# COMMAND ----------

# Listar todos os arquivos dentro do diretório especificado
from pyspark.sql.functions import lit
import re

nm_base = dbutils.widgets.get("nm_base")
dt_garantias = dbutils.widgets.get("dt_garantias")
dt_pagamentos =  dbutils.widgets.get("dt_pagamentos")
dt_mensal_pagamentos =  dbutils.widgets.get("dt_mensal_pagamentos")
dt_gerencial =  dbutils.widgets.get("dt_gerencial")
tipo_arquvivo = dbutils.widgets.get("tipo_arquvivo")
 
# Caminho do diretório
base_provisao = f'/Volumes/databox/juridico_comum/arquivos/financeiro/bases de provisão/Input/{tipo_arquvivo} Trabalhista_{nm_base}.xlsx'

base_historica_garantia = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_{dt_garantias}.xlsx'

# Base historica
base_historica_pgto = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{dt_pagamentos}.xlsx' 

# Base mensal (extraida toda semana do ELAW)
base_mensal_pgto = f'/Volumes/databox/juridico_comum/arquivos/financeiro/base mensal de pagamentos/Pagamentos_{dt_mensal_pagamentos}.xlsx'

# Base gerencial para obter campos que não existem no fechamento
base_gerencial = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_gerenciais/external/TRABALHISTA_GERENCIAL_(CONSOLIDADOS)-{dt_gerencial}.xlsx'

# COMMAND ----------

# base auxiliar com o de para de cluster de valor
df_base_provisao = read_excel(base_provisao, "'FINAL'!A4:BT1048576")
df_base_mensal_pgto = read_excel(base_mensal_pgto, "'PJ - PAGAMENTOS - GERENCIAL_PAG'!A6")
df_base_historica_pgto = read_excel(base_historica_pgto, "'PAGAMENTOS'!A6")
df_base_historica_garantia = read_excel(base_historica_garantia, "'gerencial_garantias'!A6")
df_gerencial = read_excel(base_gerencial,"'TRABALHISTA'!A6")

df_base_provisao = df_base_provisao.orderBy(df_base_provisao["ID PROCESSO"].desc())

df_base_provisao = df_base_provisao.toDF(*[
    re.sub(r'\s+', ' ', col).strip()  # substitui múltiplos espaços por um e remove espaços das bordas
    for col in df_base_provisao.columns
])

df_base_provisao.createOrReplaceTempView("df_base_provisao_original")

# COMMAND ----------

df_base_provisao.count()

# COMMAND ----------

df_gerencial = adjust_column_names(df_gerencial)
df_gerencial = remove_acentos(df_gerencial)

df_gerencial.createOrReplaceTempView("df_gerencial")

# COMMAND ----------


df_base_provisao.createOrReplaceTempView("TABELA_BASE_PROVISAO")

df_base_provisao_1 = spark.sql(f"""
/* primeira etapa: aplica as marcações de novo, reativado, encerrados e estoque na base de provisão */
SELECT *
	,(CASE WHEN `LINHA` = 'linha03' THEN 1 ELSE NULL END) AS NOVOS
 
    ,(CASE 
        WHEN (`STATUS (M-1)` IN ('BAIXA PROVISORIA','REMOVIDO','ENCERRADO')) 
            AND `STATUS (M)` IN ('ATIVO', 'REATIVADO') 
        THEN 1 
        ELSE NULL 
        END) AS REATIVADOS
    
    ,(CASE WHEN `LINHA` = 'linha03' AND `STATUS (M)` IN ('BAIXA PROVISORIA', 'ENCERRADO', 'REMOVIDO') OR `LINHA` IN ('linha02', 'linha03', 'linha00') AND `STATUS (M-1)` IN ('ATIVO', 'REATIVADO') AND `STATUS (M)` IN ('BAIXA PROVISORIA', 'ENCERRADO', 'REMOVIDO', 'BAIXA PAGAMENTO') THEN 1 ELSE NULL END) AS ENCERRADOS
    
    , (CASE WHEN `LINHA` IN ('linha00', 'linha02', 'linha03') AND `STATUS (M)` IN ('ATIVO', 'REATIVADO') THEN 1 END) AS ESTOQUE

	FROM TABELA_BASE_PROVISAO AS A; 
""")

# COMMAND ----------

df_base_mensal_pgto.createOrReplaceTempView("TB_BASE_MENSAL_PGTO")

df_base_mensal_pgto_1 = spark.sql("""
/* segunda etapa: faz os filtros sobre a área do direito, o status de pagamento e o sub tipo */
SELECT A.*
FROM TB_BASE_MENSAL_PGTO AS A
WHERE `ÁREA DO DIREITO` IN ('TRABALHISTA', 'TRABALHISTA COLETIVO') 
AND `STATUS DO PAGAMENTO` NOT IN ('Pendente','REJEITADO', 'CANCELADO', 'EM CORREÇÃO','PAGAMENTO', 'DEVOLVIDO', 'PAGAMENTO DEVOLVIDO', 'EM LEVANTAMENTO') 
AND `SUB TIPO` IN ('RNO - CONDENAÇÃO - TRABALHISTA','RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CONDENAÇÃO - TRABALHISTA', 'RNO - ACORDO - TRABALHISTA', 'MULTA POR OBRIGAÇÃO DE FAZER - TRABALHISTA','CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'ACORDO - TRABALHISTA','MULTA POR EMBRAGOS PROTELATÓRIOS - TRABALHISTA','MULTA LITIGÂNCIA DE MÁ FÉ - TRABALHISTA','PAGAMENTO DE EXECUÇÃO - TRABALHISTA','PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA','MULTA ACORDO - TRABALHISTA','PAGAMENTO DE ACORDO - TRABALHISTA (FK)','RNO - MULTA ACORDO - TRABALHISTA')
""")

# COMMAND ----------

df_base_mensal_pgto_1.createOrReplaceTempView("TB_BASE_MENSAL_PGTO_1")

df_base_mensal_pgto_2 = spark.sql("""
/* terceira etapa: filtra o tipo de pagamento, criando sua coluna ao final da tabela */
SELECT *
,(CASE WHEN `SUB TIPO` IN ('RNO - CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA', 'MULTA POR OBRIGAÇÃO DE FAZER - TRABALHISTA', 'MULTA POR EMBRAGOS PROTELATÓRIOS - TRABALHISTA','MULTA LITIGÂNCIA DE MÁ FÉ - TRABALHISTA','PAGAMENTO DE EXECUÇÃO - TRABALHISTA','PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA','MULTA ACORDO - TRABALHISTA', 'RNO - MULTA ACORDO - TRABALHISTA' ) THEN 'Condenacao' 

    WHEN `SUB TIPO` IN ('RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)') THEN 'Condenacao Incontroverso'
 
    WHEN `SUB TIPO` IN ('RNO - ACORDO - TRABALHISTA', 'ACORDO - TRABALHISTA') THEN 'Acordo' END) AS TIPO_PGTO

FROM TB_BASE_MENSAL_PGTO_1 ;
""")

# COMMAND ----------

df_base_mensal_pgto_2.createOrReplaceTempView("TB_BASE_MENSAL_PGTO_2")

df_base_mensal_pgto_3 = spark.sql("""
/* quarta etapa: filtra o tipo de pagamento, criando sua coluna ao final da tabela */
SELECT *
,(CASE WHEN `SUB TIPO` IN ('RNO - CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA', 'MULTA POR OBRIGAÇÃO DE FAZER - TRABALHISTA', 'MULTA POR EMBRAGOS PROTELATÓRIOS - TRABALHISTA','MULTA LITIGÂNCIA DE MÁ FÉ - TRABALHISTA','PAGAMENTO DE EXECUÇÃO - TRABALHISTA','PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA','MULTA ACORDO - TRABALHISTA', 'RNO - MULTA ACORDO - TRABALHISTA' ) THEN 'Condenacao' 

    WHEN `SUB TIPO` IN ('RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)') THEN 'Condenacao Incontroverso'
 
    WHEN `SUB TIPO` IN ('RNO - ACORDO - TRABALHISTA', 'ACORDO - TRABALHISTA') THEN 'Acordo' END) AS TIPO_PGTO

FROM TB_BASE_MENSAL_PGTO_1 ;
""")

# COMMAND ----------

df_base_mensal_pgto_3.createOrReplaceTempView("TB_BASE_MENSALPGTO")

df_base_mensal_pgto_4 = spark.sql(f"""
/* quinta etapa: agrupa as colunas do sub tipo para não duplicar na base de provisão */ 
SELECT
    `ID DO PROCESSO`,
    MIN(`SUB TIPO`) AS `SUB TIPO`,
    MIN(`TIPO_PGTO`) AS `TIPO_PGTO`,
    MIN(`PARCELAMENTO ACORDO`) AS `PARCELAMENTO ACORDO`,
    MIN(`PARCELAMENTO CONDENAÇÃO`) AS `PARCELAMENTO CONDENAÇÃO`,
    SUM(COALESCE(`VALOR`, 0)) AS SUM_of_VALOR
FROM
    TB_BASE_MENSALPGTO
GROUP BY 1
""")

# COMMAND ----------

df_base_provisao_1.createOrReplaceTempView("TB_BASE_PROVISAO_3")
df_base_mensal_pgto_4.createOrReplaceTempView("TB_BASE_MENSALPGTO")

df_base_provisao_3 = spark.sql(f"""
/* sexta etapa: cruza a base de provisão com as marcações da primeira etapa com as etapas 2, 3 e 4 */ 
SELECT
    A.*,
    B.`SUB TIPO`,
    B.`TIPO_PGTO`,
    B.`PARCELAMENTO ACORDO`,
    B.`PARCELAMENTO CONDENAÇÃO`,
    B.`SUM_of_VALOR`
FROM
    TB_BASE_PROVISAO_3 AS A
    LEFT JOIN TB_BASE_MENSALPGTO AS B ON A.`ID PROCESSO` = B.`ID DO PROCESSO`
""")

# COMMAND ----------

df_base_provisao_3.createOrReplaceTempView("TB_BASE_PROVISAO_4")

df_base_provisao_4 = spark.sql("""
/* sétima etapa: cria as colunas outras_adições e outras_reversões aplicando suas regras de marcação*/ 
SELECT *, 
CASE 
    WHEN `NOVOS` IS NULL AND `REATIVADOS` IS NULL AND `ENCERRADOS` IS NULL AND `Provisão Mov. (M)` > 0 AND `STATUS (M)` IN ('ATIVO','REATIVADO') THEN 1 
    ELSE NULL 
END AS OUTRAS_ADICOES,
CASE 
    WHEN `NOVOS` IS NULL AND `REATIVADOS` IS NULL AND `ENCERRADOS` IS NULL AND `Provisão Mov. (M)` < 0 AND `STATUS (M)` IN ('ATIVO','REATIVADO') THEN 1 
    ELSE NULL 
END AS OUTRAS_REVERSOES
FROM TB_BASE_PROVISAO_4
""")

# COMMAND ----------

df_base_historica_pgto.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO")

df_base_historica_pgto_1 = spark.sql("""
/* oitava etapa: faz os filtros sobre a área do direito, o status de pagamento e o sub tipo na base histórica de pagamentos */
SELECT A.*
FROM TB_BASE_HISTORICA_PGTO AS A
WHERE `ÁREA DO DIREITO` IN ('TRABALHISTA', 'TRABALHISTA COLETIVO') 
AND `STATUS DO PAGAMENTO` NOT IN ('CANCELADO', 'EM CORREÇÃO', 'PAGAMENTO', 'DEVOLVIDO','PAGAMENTO DEVOLVIDO', 'Pendente', 'REJEITADO', 'Em Levantamento') AND `SUB TIPO` NOT IN ('HONORÁRIOS PERICIAIS - TRABALHISTA', 'RNO - CUSTAS PROCESSUAIS - TRABALHISTA', 'CUSTAS PROCESSUAIS - TRABALHISTA','CUSTAS FK - TRABALHISTA', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA','PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)')
""")

# COMMAND ----------

df_base_historica_pgto_1.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO_2")

df_base_historica_pgto_2 = spark.sql("""
/* nona etapa: filtra o tipo de pagamento, criando a coluna tipo_ptgo ao final da tabela */
SELECT *
,(CASE 
    WHEN `SUB TIPO` IN ('ACORDO - TRABALHISTA','RNO - ACORDO - TRABALHISTA') THEN 'Acordo'
    WHEN `SUB TIPO` IN ('RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)','CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)','CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)') THEN 'Condenacao Incontroverso'
    ELSE 'Condenacao' 
END) AS TIPO_PGTO
FROM TB_BASE_HISTORICA_PGTO_2
""")

# COMMAND ----------

df_base_historica_pgto_2.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO_3")

df_base_historica_pgto_3 = spark.sql(f"""
/* décima etapa: agrupa as colunas do sub tipo para não duplicar na base de provisão */ 
SELECT
    `PROCESSO - ID`,
    MIN(`SUB TIPO`) AS `SUB_TIPO`,
    MIN(`TIPO_PGTO`) AS `TIPO_PGTO`,
    MIN(`PARCELAMENTO ACORDO`) AS `PARCELAMENTO ACORDO`,
    MIN(`PARCELAMENTO CONDENAÇÃO`) AS `PARCELAMENTO CONDENAÇÃO`
FROM
    TB_BASE_HISTORICA_PGTO_3
GROUP BY `PROCESSO - ID`
""")

# COMMAND ----------

df_base_historica_pgto_3.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO_4")
df_base_provisao_4.createOrReplaceTempView("TB_BASE_PROVISAO_5")
df_base_provisao_5 = spark.sql("""
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
    TB_BASE_PROVISAO_5 AS A
    LEFT JOIN TB_BASE_HISTORICA_PGTO_4 AS B ON A.`ID PROCESSO` = B.`PROCESSO - ID`
""")

df_base_provisao_5 = df_base_provisao_5.drop("PARCELAMENTO ACORDO","PARCELAMENTO CONDENAÇÃO", "SUB TIPO", "TIPO_PGTO" )

df_base_provisao_5 = df_base_provisao_5.withColumnRenamed("PARCELAMENTO ACORDO 2", "PARCELAMENTO ACORDO") \
                                       .withColumnRenamed("PARCELAMENTO CONDENAÇÃO 2", "PARCELAMENTO CONDENAÇÃO") \
                                       .withColumnRenamed("SUB TIPO 2", "SUB_TIPO") \
                                       .withColumnRenamed("TIPO_PGTO 2", "TIPO_PGTO")


# COMMAND ----------

df_base_historica_garantia.createOrReplaceTempView("TB_BASE_HISTORICA_GARANTIA")

df_base_historica_garantia_1 = spark.sql("""
/* décima segunda etapa: faz os filtros sobre a área do direito, o status de pagamento e o sub tipo na base histórica de pagamentos */
SELECT A.*
FROM TB_BASE_HISTORICA_GARANTIA A

WHERE `ÁREA DO DIREITO` IN ('TRABALHISTA', 'TRABALHISTA COLETIVO')
        AND `STATUS DA GARANTIA` NOT IN ('PENDENTE', 'EM LEVANTAMENTO', 'CANCELADO', 'EM CORREÇÃO') 
        AND `TIPO GARANTIA` NOT IN ('FIANÇA - TRABALHISTA','INATIVAR','INATIVO','LEVANTAMENTO DE CRÉDITO','SEGURO GARANTIA - TRABALHISTA','SEGURO GARANTIA JUDICIAL - TRABALHISTA','SEGURO GARANTIA RECURSAL - TRABALHISTA','BENS IMÓVEIS - TRABALHISTA','BENS MÓVEIS - TRABALHISTA','INATIVA DEPÓSITO RECURSAL - AIRR','INATIVA DEPÓSITO RECURSAL AIRR')
        AND `DATA DE LEVANTAMENTO (PARTE CONTRÁRIA)` IS NOT NULL

""")

# COMMAND ----------

df_base_historica_garantia_1.createOrReplaceTempView("TB_BASE_HISTORICA_GARANTIA_1")

df_base_historica_garantia_2 = spark.sql("""
/* décima terceira etapa: cria a coluna tipo_ptgo ao final da tabela */
SELECT *
,(CASE WHEN `ÁREA DO DIREITO` IN ('TRABALHISTA', 'TRABALHISTA COLETIVO') THEN 'Condenacao' END) AS TIPO_PGTO

FROM TB_BASE_HISTORICA_GARANTIA_1 ;
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
df_base_provisao_5.createOrReplaceTempView("TB_BASE_PROVISAO_6")
df_base_provisao_6 = spark.sql("""
/* décima quinta etapa: marca o sub_tipo, tipo_pgto de garantia nas linhas que foram encerradas e que não receberam pagamento no mês */ 
SELECT
    A.*,
    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL AND A.`TIPO_PGTO` NOT IN ('Acordo', 'Condenacao')
        THEN B.`TIPO_PGTO` ELSE A.`TIPO_PGTO` END ) AS `TIPO_PGTO 1`,

    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL AND A.`TIPO_PGTO` NOT IN ('Acordo', 'Condenacao')
        THEN B.`SUB TIPO` ELSE A.`SUB_TIPO` END ) AS `SUB TIPO 1`
FROM
    TB_BASE_PROVISAO_6 AS A
    LEFT JOIN TB_BASE_HISTORICA_GARANTIA_3 AS B ON A.`ID PROCESSO` = B.`(PROCESSO) ID`
""")

df_base_provisao_6 = df_base_provisao_6.drop("SUB_TIPO", "TIPO_PGTO" )

df_base_provisao_6 = df_base_provisao_6.withColumnRenamed("SUB TIPO 1", "SUB_TIPO") \
                                       .withColumnRenamed("TIPO_PGTO 1", "TIPO_PGTO")


# COMMAND ----------

df_base_provisao_6.createOrReplaceTempView("TB_BASE_PROVISAO_7")
df_base_provisao_7 = spark.sql("""
/* décima sexta etapa: marca os encerramentos sem ônus*/ 
SELECT
    *,
    (CASE 
    WHEN ENCERRADOS IS NOT NULL AND SUM_of_VALOR IS NULL AND TIPO_PGTO IS NULL
    THEN 'Sem Onus' ELSE TIPO_PGTO END ) AS `TIPO_PGTO 2`
FROM
    TB_BASE_PROVISAO_7
""")

df_base_provisao_7 = df_base_provisao_7.drop("TIPO_PGTO" )
df_base_provisao_7 = df_base_provisao_7.withColumnRenamed("TIPO_PGTO 2", "TIPO_PGTO")

df_base_provisao_7 = df_base_provisao_7.fillna({'SUM_of_VALOR': 0})

# COMMAND ----------

# display(df_base_provisao_7)

# COMMAND ----------

# Remove espaçoes adjacentes das colunas (COPIAR PARA AS OUTRAS AREAS)
df_base_provisao_6 = adjust_column_names(df_base_provisao_6)

# Altera colunas duplicatas
df_base_provisao_6 = deduplica_cols(df_base_provisao_6)

# COMMAND ----------

df_base_provisao_7 = df_base_provisao_7.orderBy(df_base_provisao_7["ID PROCESSO"].desc())

# Apenas printa as colunas para visualização
c = df_base_provisao_7.columns

print(c)

# COMMAND ----------

# Adicione esta verificação em uma nova célula ANTES da célula 27

df_base_provisao_7.createOrReplaceTempView("df_base_provisao_7_temp_check")

soma_intermediaria = spark.sql("""
    SELECT 
        SUM(`Provisão Mov. (M)`) as soma_total,
        COUNT(*) as total_linhas
    FROM df_base_provisao_7_temp_check
""")

display(soma_intermediaria)

# COMMAND ----------

df_reorganizado = df_base_provisao_7.select(
    'LINHA', 
    '`ID PROCESSO`', 
    '`Área do Direito`', 
    '`Sub-área do Direito`', 
    '`Grupo (M-1)`', 
    '`Empresa (M-1)`', 
    '`Grupo (Fechamento)`', 
    '`Empresa (M)`', 
    '`STATUS (M-1)`', 
    '`STATUS (M)`', 
    '`Centro de Custo (M-1)`', 
    '`Centro de Custo (M)`', 
    'Cadastro', 
    'Reabertura', 
    '`Objeto Assunto/Cargo (M-1)`', 
    '`Sub Objeto Assunto/Cargo (M-1)`', 
    '`Objeto Assunto/Cargo (M)`', 
    '`Sub Objeto Assunto/Cargo (M)`', 
    '`Órgão ofensor (Fluxo) (M-1)`', 
    '`Órgão ofensor (Fluxo) (M)`', 
    '`Natureza Operacional (M-1)`', 
    '`Natureza Operacional (M)`', 
    'Distribuição', 
    '`Nº Processo`', 
    'Escritorio', 
    'VALOR DA CAUSA', 
    '`% Sócio (M-1)`', 
    '`% Empresa (M-1)`', 
    '`% Sócio (M)`', 
    '`% Empresa (M)`', 
    '`Provisão (M-1)`', 
    '`Correção (M-1)`',
    '`Provisão Total (M-1)`', 
    '`CLASSIFICAÇÃO MOV. (M)`', 
    '`PROVISÃO MOV. (M)`', # <--- ADICIONE ESTA LINHA
    '`Correção Mov. (M)`', 
    '`Provisão Mov. Total (M)`', 
    '`Provisão Total (M)`', 
    '`Correção (M-1)_1`', # Aqui sempre vem um nome diferente
    '`Correção (M)`', 
    '`Provisão Total Passivo (M)`', 
    '`Socio: Provisão (M-1)`', 
    '`Socio: Correção (M-1)`', 
    '`Socio: Provisão Total (M-1)`', 
    '`SÓCIO: CLASSIFICAÇÃO MOV. (M)`', 
    '`SÓCIO: PROVISÃO MOV. (M)`', 
    '`SÓCIO: CORREÇÃO MOV. (M)`', 
    '`SÓCIO: PROVISÃO MOV. TOTAL (M)`', 
    '`SÓCIO: PROVISÃO TOTAL (M)`', 
    '`SÓCIO: CORREÇÃO (M-1)_1`', 
    '`SÓCIO: CORREÇÃO (M)`', 
    '`SÓCIO: PROV. TOTAL PASSIVO (M)`', 
    '`EMPRESA: PROVISÃO (M-1)`', 
    '`EMPRESA: CORREÇÃO (M-1)`', 
    '`EMPRESA: PROVISÃO TOTAL (M-1)`', 
    '`EMPRESA: CLASSIFICAÇÃO MOV.(M)`', 
    '`EMPRESA: PROVISÃO MOV. (M)`', 
    '`EMPRESA: CORREÇÃO MOV. (M)`', 
    '`EMPRESA: PROVISÃO MOV.TOTAL (M)`', 
    '`EMPRESA: PROVISÃO TOTAL (M)`', 
    '`EMPRESA: CORREÇÃO (M-1)_1`', 
    '`EMPRESA: CORREÇÃO (M)`', 
    '`EMPRESA: PROV TOTAL PASSIVO(M)`', 
    'Demitido por Reestruturação', 
    'Indicação Processo Estratégico',  
    'NOVOS', 
    'REATIVADOS', 
    'ENCERRADOS', 
    'ESTOQUE', 
    'SUB_TIPO', 
    'TIPO_PGTO', 
    'PARCELAMENTO CONDENAÇÃO', 
    'PARCELAMENTO ACORDO', 
    'SUM_of_VALOR', 
    'OUTRAS_ADICOES', 
    'OUTRAS_REVERSOES'
)

# df_reorganizado = df_reorganizado.withColumn('Cadastro', F.to_date(F.col('Cadastro'), 'dd/MM/yyyy'))


df_reorganizado.createOrReplaceTempView('df_reorganizado') 

# COMMAND ----------

# df_reorganizado.count()

# COMMAND ----------

# %sql

# SELECT * FROM df_reorganizado

# COMMAND ----------

df_reorganizado_final = spark.sql('''
    SELECT A.*
        ,CASE WHEN A.Cadastro <= '2020-01-01' THEN 'LEGADO'
        ELSE 'NOVO' END AS NOVO_LEGADO
        ,CASE 
            WHEN A.`Natureza Operacional (M)` == "TERCEIRO SOLVENTE" THEN "TERCEIRO"
            WHEN A.`Natureza Operacional (M)` == "TERCEIRO INSOLVENTE" THEN "TERCEIRO"
            ELSE "PRÓPRIO" END AS PROPRIO_VS_TERCEIRO
        ,B.ULTIMA_POSICAO_ESTRATEGIA_JUSTIFICATIVA_ACORDO_DEFESA AS DOC
        ,B.FASE AS FASE_M
        ,C.`TIPO DE CÁLCULO` AS TIPO_CALCULO_M
        ,'' AS ESTRATEGIA_M_1
        ,'' AS FASE_M_1
        ,'' AS TIPO_CALCULO_M_1
    FROM df_reorganizado A
    LEFT JOIN df_gerencial B
    ON A.`ID PROCESSO` = B.PROCESSO_ID
    LEFT JOIN df_base_provisao_original C 
    ON A.`ID PROCESSO` = C.`ID PROCESSO` and A.LINHA = C.LINHA
''')

# COMMAND ----------

# --- CÉLULA DE VERIFICAÇÃO ---

# 1. Registra o DataFrame que acabou de ser criado como uma view temporária
df_reorganizado_final.createOrReplaceTempView("df_reorganizado_final_check")

# 2. Executa a query de verificação para somar a coluna de interesse
soma_apos_join_final = spark.sql("""
    SELECT 
        SUM(`Provisão Mov. (M)`) as soma_total,
        COUNT(*) as total_linhas
    FROM df_reorganizado_final_check
""")

# 3. Exibe o resultado da verificação
display(soma_apos_join_final)

print("Verificação concluída. Compare a 'soma_total' acima com o valor da verificação anterior.")

# COMMAND ----------

df_reorganizado_final.count()

# COMMAND ----------

# df_reorganizado.count()

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile
 
# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_reorganizado_final.toPandas()
 
# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/Base Consolidada_trabalhista.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='BASE')
# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/financeiro/bases de provisão/output/Base_consolidada_trabalhista.xlsx'
copyfile(local_path, volume_path)

# COMMAND ----------

print("Processo concluido!!!\n")

print("Baixe o arquivo no caminho: /Volumes/databox/juridico_comum/arquivos/financeiro/bases de provisão/output/Base_consolidada_trabalhista.xlsx")

