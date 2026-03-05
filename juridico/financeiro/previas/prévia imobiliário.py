# Databricks notebook source
# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions" 

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
base_provisao = f'/Volumes/databox/juridico_comum/arquivos/financeiro/bases de provisão/Input/{tipo_arquvivo} Imobiliário_{nm_base}.xlsx'
base_historica_garantia = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_{dt_garantias}.xlsx'
base_historica_pgto = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{dt_pagamentos}.xlsx'
base_mensal_pgto = f'/Volumes/databox/juridico_comum/arquivos/financeiro/base mensal de pagamentos/Pagamentos_{dt_mensal_pagamentos}.xlsx'

# COMMAND ----------

# base auxiliar com o de para de cluster de valor
df_base_provisao = read_excel(base_provisao, "'Final'!A4:BQ1048576")
df_base_mensal_pgto = read_excel(base_mensal_pgto, "'PJ - PAGAMENTOS - GERENCIAL_PAG'!A6")
df_base_historica_pgto = read_excel(base_historica_pgto, "'PAGAMENTOS'!A6")
df_base_historica_garantia = read_excel(base_historica_garantia, "'gerencial_garantias'!A6")

# COMMAND ----------

df_base_provisao = df_base_provisao.withColumnRenamed("Id do Processo", "ID PROCESSO")

# COMMAND ----------

df_base_provisao.head()

# COMMAND ----------

df_filtrado = df_base_provisao.filter(df_base_provisao["ID PROCESSO"].isNotNull())

print(df_filtrado.count())

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

df_base_provisao_1.createOrReplaceTempView("TABELA_BASE_PROVISAO_1")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from TABELA_BASE_PROVISAO_1 where ESTOQUE = 1

# COMMAND ----------

df_base_mensal_pgto.createOrReplaceTempView("TB_BASE_MENSAL_PGTO")

df_base_mensal_pgto_1 = spark.sql("""
/* segunda etapa: faz os filtros sobre a área do direito, o status de pagamento e o sub tipo */
SELECT A.*
FROM TB_BASE_MENSAL_PGTO AS A
WHERE `ÁREA DO DIREITO` IN ('IMOBILIÁRIO')
AND `STATUS DO PAGAMENTO` NOT IN ('Pendente','REJEITADO', 'CANCELADO', 'EM CORREÇÃO','PAGAMENTO', 'DEVOLVIDO', 'PAGAMENTO DEVOLVIDO', 'EM LEVANTAMENTO') 
AND `SUB TIPO` IN ('IMOBILIARIO - LITIGIOSO CONDENAÇÃO','ACORDO - IMOBILIÁRIO','CONDENAÇÃO - IMOBILIÁRIO','ALUGUEL PJ - IMOBILIÁRIO','CONDENAÇÃO - IMOBILIÁRIO (INCONTROVERSO)','CONDENAÇÃO EM HONORÁRIOS - IMOBILIÁRIO','MULTA PROCESSUAL - IMOBILIÁRIO','LIBERAÇÃO DE PENHORA - MASSA','ALUGUEL LITIGIOSO','LIBERAÇÃO DE PENHORA - IMOBILIÁRIO','CONDENAÇÃO - REGULATÓRIO','CONSIGNATÓRIA - ALUGUEL - IMOBILIÁRIO')
""")

# COMMAND ----------

df_base_mensal_pgto_1.createOrReplaceTempView("TB_BASE_MENSAL_PGTO_1")

df_base_mensal_pgto_2 = spark.sql("""
/* terceira etapa: filtra o tipo de pagamento, criando sua coluna ao final da tabela */
SELECT *
,(CASE WHEN `SUB TIPO` IN ('IMOBILIARIO - LITIGIOSO CONDENAÇÃO','CONDENAÇÃO - IMOBILIÁRIO','ALUGUEL PJ - IMOBILIÁRIO','CONDENAÇÃO EM HONORÁRIOS - IMOBILIÁRIO','MULTA PROCESSUAL - IMOBILIÁRIO','LIBERAÇÃO DE PENHORA - MASSA','ALUGUEL LITIGIOSO','LIBERAÇÃO DE PENHORA - IMOBILIÁRIO','CONDENAÇÃO - REGULATÓRIO','CONSIGNATÓRIA - ALUGUEL - IMOBILIÁRIO') THEN 'Condenacao' 

    WHEN `SUB TIPO` IN ('CONDENAÇÃO - IMOBILIÁRIO (INCONTROVERSO)') THEN 'Condenacao Incontroverso'
 
    WHEN `SUB TIPO` IN ('ACORDO - IMOBILIÁRIO') THEN 'Acordo' END) AS TIPO_PGTO

FROM TB_BASE_MENSAL_PGTO_1 ;
""")

# COMMAND ----------

df_base_mensal_pgto_2.createOrReplaceTempView("TB_BASE_MENSAL_PGTO_2")

df_base_mensal_pgto_3 = spark.sql("""
/* quarta etapa: filtra o tipo de pagamento, criando sua coluna ao final da tabela */
SELECT *
,(CASE WHEN `SUB TIPO` IN ('IMOBILIARIO - LITIGIOSO CONDENAÇÃO','CONDENAÇÃO - IMOBILIÁRIO','ALUGUEL PJ - IMOBILIÁRIO','CONDENAÇÃO EM HONORÁRIOS - IMOBILIÁRIO','MULTA PROCESSUAL - IMOBILIÁRIO','LIBERAÇÃO DE PENHORA - MASSA','ALUGUEL LITIGIOSO','LIBERAÇÃO DE PENHORA - IMOBILIÁRIO','CONDENAÇÃO - REGULATÓRIO','CONSIGNATÓRIA - ALUGUEL - IMOBILIÁRIO') THEN 'Condenacao' 

    WHEN `SUB TIPO` IN ('CONDENAÇÃO - IMOBILIÁRIO (INCONTROVERSO)') THEN 'Condenacao Incontroverso'
 
    WHEN `SUB TIPO` IN ('ACORDO - IMOBILIÁRIO') THEN 'Acordo' END) AS TIPO_PGTO

FROM TB_BASE_MENSAL_PGTO_1 ;
""")

# COMMAND ----------

df_base_mensal_pgto_3.createOrReplaceTempView("TB_BASE_MENSALPGTO_3")

df_base_mensal_pgto_4 = spark.sql(f"""
/* quinta etapa: agrupa as colunas do sub tipo para não duplicar na base de provisão */ 
SELECT
    `ID DO PROCESSO`,
    MIN(`SUB TIPO`) AS `SUB TIPO`,
    MIN(`TIPO_PGTO`) AS `TIPO_PGTO`,
    SUM(`VALOR`) AS SUM_of_VALOR
FROM
    TB_BASE_MENSALPGTO_3
GROUP BY 1
""")

# COMMAND ----------

display(df_base_mensal_pgto_4)

# COMMAND ----------

df_base_provisao_1.createOrReplaceTempView("TB_BASE_PROVISAO_2")
df_base_mensal_pgto_4.createOrReplaceTempView("TB_BASE_MENSALPGTO_4")

df_base_provisao_2 = spark.sql(f"""
/* sexta etapa: cruza a base de provisão com as marcações da primeira etapa com as etapas 2, 3 e 4 */ 
SELECT
    A.*,
    B.`SUB TIPO`,
    B.`TIPO_PGTO`,
    B.`SUM_of_VALOR`
FROM
    TB_BASE_PROVISAO_2 AS A
    LEFT JOIN TB_BASE_MENSALPGTO_4 AS B ON A.`ID PROCESSO` = B.`ID DO PROCESSO`
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
WHERE `ÁREA DO DIREITO` IN ('IMOBILIÁRIO') 
AND `STATUS DO PAGAMENTO` NOT IN ('CANCELADO', 'EM CORREÇÃO', 'PAGAMENTO', 'DEVOLVIDO','PAGAMENTO DEVOLVIDO', 'Pendente', 'REJEITADO', 'Em Levantamento') AND `SUB TIPO` NOT IN ('CUSTAS PROCESSUAIS - IMOBILIÁRIO','CUSTAS PERITOS - IMOBILIÁRIO','HONORÁRIOS PERICIAIS - IMOBILIÁRIO','CUSTAS PROCESSUAIS - REGULATÓRIO')
""")

# COMMAND ----------

df_base_historica_pgto_1.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO_1")

df_base_historica_pgto_2 = spark.sql("""
/* nona etapa: filtra o tipo de pagamento, criando a coluna tipo_ptgo ao final da tabela */
SELECT *
,(CASE 
    WHEN `SUB TIPO` IN ('ACORDO - IMOBILIÁRIO') THEN 'Acordo'
    WHEN `SUB TIPO` IN ('CONDENAÇÃO - IMOBILIÁRIO (INCONTROVERSO)') THEN 'Condenacao Incontroverso'
    ELSE 'Condenacao' 
END) AS TIPO_PGTO
FROM TB_BASE_HISTORICA_PGTO_1
""")

# COMMAND ----------

df_base_historica_pgto_2.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO_3")

df_base_historica_pgto_3 = spark.sql(f"""
/* décima etapa: agrupa as colunas do sub tipo para não duplicar na base de provisão */ 
SELECT
    `PROCESSO - ID`,
    MIN(`SUB TIPO`) AS `SUB_TIPO`,
    MIN(`TIPO_PGTO`) AS `TIPO_PGTO`
FROM
    TB_BASE_HISTORICA_PGTO_3
GROUP BY `PROCESSO - ID`
""")

# COMMAND ----------

df_base_historica_pgto_3.createOrReplaceTempView("TB_BASE_HISTORICA_PGTO_4")
df_base_provisao_3.createOrReplaceTempView("TB_BASE_PROVISAO_4")
df_base_provisao_4 = spark.sql("""
/* décima primeira etapa: marca o sub_tipo, tipo_pgto, parcelamento acordo e parcelamento condenação nas linhas que foram encerradas e que não receberam pagamento no mês */ 
SELECT
    A.*,
    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL 
        THEN B.`SUB_TIPO` ELSE A.`SUB TIPO` END ) AS `SUB TIPO 2`,

    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL 
        THEN B.`TIPO_PGTO` ELSE A.`TIPO_PGTO` END ) AS `TIPO_PGTO 2`
FROM
    TB_BASE_PROVISAO_4 AS A
    LEFT JOIN TB_BASE_HISTORICA_PGTO_4 AS B ON A.`ID PROCESSO` = B.`PROCESSO - ID`
""")

df_base_provisao_4 = df_base_provisao_4.drop("SUB TIPO", "TIPO_PGTO" )

df_base_provisao_4 = df_base_provisao_4.withColumnRenamed("SUB TIPO 2", "SUB_TIPO") \
                                       .withColumnRenamed("TIPO_PGTO 2", "TIPO_PGTO")

# COMMAND ----------

df_base_historica_garantia.createOrReplaceTempView("TB_BASE_HISTORICA_GARANTIA")

df_base_historica_garantia_1 = spark.sql("""
/* décima segunda etapa: faz os filtros sobre a área do direito, o status de pagamento e o sub tipo na base histórica de pagamentos */
SELECT A.*
FROM TB_BASE_HISTORICA_GARANTIA A

WHERE `ÁREA DO DIREITO` IN ('IMOBILIÁRIO')
        AND `STATUS DA GARANTIA` NOT IN ('PENDENTE', 'EM LEVANTAMENTO', 'CANCELADO', 'EM CORREÇÃO', 'ATIVO') 
        AND `TIPO GARANTIA` NOT IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL','LEVANTAMENTO DE CRÉDITO','SEGURO GARANTIA - IMOBILIARIO','ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL (PAG)','BENS IMÓVEIS - IMOBILIÁRIO','INATIVAR','INATIVO','CARTA DE FIANÇA - IMOBILIÁRIO','BEM MÓVEL - IMOBILIÁRIO','SEGURO GARANTIA RECURSAL - TRABALHISTA')

        AND `DATA DE LEVANTAMENTO (PARTE CONTRÁRIA)` IS NOT NULL

""")

# COMMAND ----------

df_base_historica_garantia_1.createOrReplaceTempView("TB_BASE_HISTORICA_GARANTIA_1")

df_base_historica_garantia_2 = spark.sql("""
/* décima terceira etapa: cria a coluna tipo_ptgo ao final da tabela */
SELECT *
,(CASE WHEN `ÁREA DO DIREITO` IN ('IMOBILIÁRIO') THEN 'Condenacao' END) AS TIPO_PGTO

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
df_base_provisao_4.createOrReplaceTempView("TB_BASE_PROVISAO_5")
df_base_provisao_5 = spark.sql("""
/* décima quinta etapa: marca o sub_tipo, tipo_pgto de garantia nas linhas que foram encerradas e que não receberam pagamento no mês */ 
SELECT
    A.*,
    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL AND A.`TIPO_PGTO` NOT IN ('Acordo', 'Condenacao')
        THEN B.`TIPO_PGTO` ELSE A.`TIPO_PGTO` END ) AS `TIPO_PGTO 1`,

    (CASE WHEN A.`ENCERRADOS` IS NOT NULL AND A.`SUM_of_VALOR` IS NULL AND A.`TIPO_PGTO` NOT IN ('Acordo', 'Condenacao')
        THEN B.`SUB TIPO` ELSE A.`SUB_TIPO` END ) AS `SUB TIPO 1`
FROM
    TB_BASE_PROVISAO_5 AS A
    LEFT JOIN TB_BASE_HISTORICA_GARANTIA_3 AS B ON A.`ID PROCESSO` = B.`(PROCESSO) ID`
""")

df_base_provisao_5 = df_base_provisao_5.drop("SUB_TIPO", "TIPO_PGTO" )

df_base_provisao_5 = df_base_provisao_5.withColumnRenamed("SUB TIPO 1", "SUB_TIPO") \
                                       .withColumnRenamed("TIPO_PGTO 1", "TIPO_PGTO")

# COMMAND ----------

df_base_provisao_5.createOrReplaceTempView("TB_BASE_PROVISAO_6")
df_base_provisao_6 = spark.sql("""
/* décima sexta etapa: marca os encerramentos sem ônus*/ 
SELECT
    *,
    (CASE 
    WHEN ENCERRADOS IS NOT NULL AND SUM_of_VALOR IS NULL AND TIPO_PGTO IS NULL
    THEN 'Sem Onus' ELSE TIPO_PGTO END ) AS `TIPO_PGTO 2`
FROM
    TB_BASE_PROVISAO_6
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

# COMMAND ----------

# Apenas printa as colunas para visualização
c = df_base_provisao_6.columns

print(c)

# COMMAND ----------

df_base_provisao_6 = df_base_provisao_6.withColumnRenamed("VLR_CAUSA", "VALOR_DA_CAUSA")

# COMMAND ----------

# Select the columns in the desired order
df_reorganizado = df_base_provisao_6.select(
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
   'CORRECAO_M_1_1',
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
   'SOCIO:_CORRECAO_M_1', 
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
   'INDICACAO_PROCESSO_ESTRATEGICO', 
   'NOVOS', 
   'ENCERRADOS', 
   'ESTOQUE', 
   'SUB_TIPO', 
   'TIPO_PGTO',
   'SUM_OF_VALOR', 
   'OUTRAS_ADICOES', 
   'OUTRAS_REVERSOES', 

)

df_reorganizado.createOrReplaceTempView("df_reorganizado")


# COMMAND ----------

df_reorganizado = spark.sql('''
    select * FROM df_reorganizado
    where `ID_PROCESSO` is not null
''')

# COMMAND ----------

 print(df_reorganizado.count())

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile
 
# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_reorganizado.toPandas()
 
# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/Base_consolidada_imobiliário.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='BASE')
# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/financeiro/bases de provisão/output/Base_consolidada_imobiliário.xlsx'
 
copyfile(local_path, volume_path)
