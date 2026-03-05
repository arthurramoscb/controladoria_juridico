# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------


# Caminho das pastas e arquivos
nmtabela_pgto = dbutils.widgets.get("nmtabela_pgto")
nmtabela_garantias = dbutils.widgets.get("nmtabela_garantias")

path_pagamentos = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{nmtabela_pgto}.xlsx'
path_garantias = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_{nmtabela_garantias}.xlsx'

# COMMAND ----------

# Carrega as planilhas em Spark Data Frames
df_pgto_civel = read_excel(path_pagamentos, "A6")
df_garantias_civel = read_excel(path_garantias, "A6")

# COMMAND ----------

# Lista as colunas com datas
pgto_data_civel_cols = find_columns_with_word(df_pgto_civel, 'DATA ')
garantias_data_civel_cols = find_columns_with_word(df_garantias_civel, 'DATA ')

print("pgto_data_civel_cols")
print(pgto_data_civel_cols)
print("\n")
print("garantias_data_civel_cols")
print(garantias_data_civel_cols)

# COMMAND ----------

# Converte as datas das colunas listadas
historico_de_pagamentos_civel_1 = convert_to_date_format(df_pgto_civel, pgto_data_civel_cols)
historico_de_pagamentos_civel_1 = adjust_column_names(df_pgto_civel)
garantias_civel = convert_to_date_format(df_garantias_civel, garantias_data_civel_cols)
garantias_civel = adjust_column_names(df_garantias_civel)

# COMMAND ----------

historico_de_pagamentos_civel_1.count()

# COMMAND ----------


nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/'

# COMMAND ----------

arquivo_ativos = f'CIVEL_GERENCIAL-(ATIVOS)-{nmtabela}.xlsx'
arquivo_encerrados = f'CIVEL_GERENCIAL-(ENCERRADO)-{nmtabela}.xlsx'

path_ativos = diretorio_origem + arquivo_ativos
path_encerrados = diretorio_origem + arquivo_encerrados

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# Carrega as planilhas em Spark Data Frames
gerencial_ativos_civel = read_excel(path_ativos, "'CÍVEL'!A6")
gerencial_encerrados_civel = read_excel(path_encerrados, "'CÍVEL'!A6")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Tratamento base de Pagamentos e Garantias

# COMMAND ----------

#Importação das bases gerenciais - criação do campo responsabilidade


# Seleciona as colunas necessárias do DataFrame 'gerencial_ativos_civel'
ativos_civel = gerencial_ativos_civel.select('PROCESSO - ID', 'CÍVEL MASSA - RESPONSABILIDADE')

# Seleciona as colunas necessárias do DataFrame 'gerencial_encerrados_civel'
encerrados_civel = gerencial_encerrados_civel.select('PROCESSO - ID', 'CÍVEL MASSA - RESPONSABILIDADE')

# Une os DataFrames 'ativos_civel' e 'encerrados_civel'
tb_responsabilidade = ativos_civel.union(encerrados_civel)


# COMMAND ----------

from pyspark.sql.functions import col

tb_responsabilidade_final = tb_responsabilidade.dropDuplicates(["PROCESSO - ID"]) \
    .orderBy(col("PROCESSO - ID"))

# COMMAND ----------

tb_responsabilidade_final.count()

# COMMAND ----------

tb_responsabilidade_final = tb_responsabilidade_final.withColumnRenamed("PROCESSO - ID", "PROCESSO_ID")
#tb_responsabilidade_final = tb_responsabilidade_final .withColumnRenamed("CÍVEL MASSA - RESPONSABILIDADE", "CÍVEL_MASSA_-_RESPONSABILIDADE")

# COMMAND ----------

tb_responsabilidade_final.count()

# COMMAND ----------

from pyspark.sql.functions import col

# Realiza o left join entre as duas tabelas e adiciona a coluna 'CÍVEL_MASSA_-_RESPONSABILIDADE'
historico_de_pagamentos_civel_1 = historico_de_pagamentos_civel_1.join(
    tb_responsabilidade_final.select('PROCESSO_ID', 'CÍVEL MASSA - RESPONSABILIDADE'),
    on='PROCESSO_ID',
    how='left'
)


# COMMAND ----------

historico_de_pagamentos_civel_1 = historico_de_pagamentos_civel_1.dropDuplicates(["PROCESSO_ID"])

# COMMAND ----------

historico_de_pagamentos_civel_1.count()

# COMMAND ----------

historico_de_pagamentos_civel_1.createOrReplaceTempView("HISTORICO_DE_PAGAMENTOS_CIVEL_1")

# COMMAND ----------

historico_de_pagamentos_civel_2 = spark.sql("""
SELECT 
    HISTORICO_DE_PAGAMENTOS_CIVEL_1.*, 
    CASE 
        WHEN `SUB_TIPO` IN ('ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA', 'ACORDO - TRABALHISTA', 'PAGAMENTO DE ACORDO - TRABALHISTA (FK)', 'RNO - ACORDO - TRABALHISTA', 'ACORDO - CÍVEL MASSA - CARTÕES', 'ACORDO - CÍVEL MASSA - FORNECEDOR', 'ACORDO - CÍVEL MASSA - MKTPLACE', 'ACORDO - CÍVEL MASSA - SEGURO') 
        THEN VALOR 
    END AS ACORDO,
    CASE 
        WHEN `SUB_TIPO` IN ('CONDENAÇÃO - CÍVEL', 'CONDENAÇÃO - CÍVEL ESTRATÉGICO', 'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL', 'MULTA PROCESSUAL - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'RNO - MULTA PROCESSUAL - TRABALHISTA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO', 'CONDENAÇÃO - CÍVEL MASSA - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA - SEGURO') 
        THEN VALOR 
    END AS CONDENACAO,
    CASE 
        WHEN `SUB_TIPO` IN ('HONORÁRIOS CONCILIADOR - CIVEL MASSA', 'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA', 'HONORÁRIOS PERICIAIS - IMOBILIÁRIO', 'HONORÁRIOS PERICIAIS - TRABALHISTA', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA', 'CUSTAS FK - TRABALHISTA', 'CUSTAS PERITOS - CÍVEL', 'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL', 'CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO', 'CUSTAS PROCESSUAIS - TRABALHISTA', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO', 'PERITOS - REGULATÓRIO', 'RNO - CUSTAS PROCESSUAIS - TRABALHISTA', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE') 
        THEN VALOR 
    END AS CUSTAS,
    CASE 
        WHEN `SUB_TIPO` IN ('INSS - TRABALHISTA', 'IR - TRABALHISTA', 'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)', 'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA', 'RNO - FGTS', 'FGTS') 
        THEN VALOR 
    END AS IMPOSTO,
    CASE 
        WHEN `SUB_TIPO` IN ('LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA') 
        THEN VALOR 
    END AS PENHORA,
    CASE 
        WHEN `SUB_TIPO` IN ('PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO', 'PENSÃO - CÍVEL MASSA') 
        THEN VALOR 
    END AS PENSAO,
    CASE 
        WHEN `SUB_TIPO` IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)', 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO', 'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO', 'LIMINAR (CÍV. MASSA)', 'PAGAMENTO', 'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA', 'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK') 
        THEN VALOR 
    END AS OUTROS_PAGAMENTOS
FROM HISTORICO_DE_PAGAMENTOS_CIVEL_1
WHERE `SUB_TIPO` IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)', 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO', 'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL', 'ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA', 'ACORDO - TRABALHISTA', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO', 'ATIVO', 'CONDENAÇÃO - CÍVEL', 'CONDENAÇÃO - CÍVEL ESTRATÉGICO', 'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CUSTAS FK - TRABALHISTA', 'CUSTAS PERITOS - CÍVEL', 'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL', 'CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO', 'CUSTAS PROCESSUAIS - TRABALHISTA', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA', 'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA', 'HONORÁRIOS PERICIAIS - IMOBILIÁRIO', 'HONORÁRIOS PERICIAIS - TRABALHISTA', 'LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA', 'LIMINAR (CÍV. MASSA)', 'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL', 'MULTA PROCESSUAL - IMOBILIÁRIO', 'MULTA PROCESSUAL - TRABALHISTA', 'PAGAMENTO', 'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA', 'PAGAMENTO DE ACORDO - TRABALHISTA (FK)', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO', 'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)', 'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK', 'PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO', 'PENSÃO - CÍVEL MASSA', 'PERITOS - REGULATÓRIO', 'RNO - ACORDO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'RNO - CUSTAS PROCESSUAIS - TRABALHISTA', 'RNO - FGTS', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA', 'RNO - MULTA PROCESSUAL - TRABALHISTA', 'INSS - TRABALHISTA', 'IR - TRABALHISTA', 'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)', 'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO', 'CONDENAÇÃO - CÍVEL MASSA - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA - SEGURO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE')
""")




# COMMAND ----------

from pyspark.sql.functions import col, when



# Defina as condições para cada tipo de pagamento
condicoes = [
    (col("SUB_TIPO").isin('ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA', 
                          'ACORDO - TRABALHISTA', 'PAGAMENTO DE ACORDO - TRABALHISTA (FK)',
                          'RNO - ACORDO - TRABALHISTA', 'ACORDO - CÍVEL ESTRATÉGICO',
                          'ACORDO - CÍVEL MASSA', 'ACORDO - CÍVEL MASSA - CARTÕES',
                          'ACORDO - CÍVEL MASSA - FORNECEDOR', 'ACORDO - CÍVEL MASSA - MKTPLACE',
                          'ACORDO - CÍVEL MASSA - SEGURO'), col("VALOR")),
    (col("SUB_TIPO").isin('CONDENAÇÃO - CÍVEL', 'CONDENAÇÃO - CÍVEL ESTRATÉGICO', 'CONDENAÇÃO - CÍVEL MASSA',
                          'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO',
                          'CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)',
                          'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL', 'MULTA PROCESSUAL - TRABALHISTA',
                          'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)',
                          'RNO - MULTA PROCESSUAL - TRABALHISTA', 'CONDENAÇÃO - CÍVEL MASSA',
                          'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES',
                          'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE',
                          'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO', 'CONDENAÇÃO - CÍVEL MASSA - CARTÕES',
                          'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE',
                          'CONDENAÇÃO - CÍVEL MASSA - SEGURO'), col("VALOR")),
    (col("SUB_TIPO").isin('HONORÁRIOS CONCILIADOR - CIVEL MASSA', 'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO',
                          'HONORÁRIOS PERICIAIS - CÍVEL MASSA', 'HONORÁRIOS PERICIAIS - IMOBILIÁRIO',
                          'HONORÁRIOS PERICIAIS - TRABALHISTA', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)',
                          'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA', 'CUSTAS FK - TRABALHISTA', 'CUSTAS PERITOS - CÍVEL',
                          'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL', 'CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO',
                          'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO',
                          'CUSTAS PROCESSUAIS - TRABALHISTA', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO', 'PERITOS - REGULATÓRIO',
                          'RNO - CUSTAS PROCESSUAIS - TRABALHISTA', 'CUSTAS PROCESSUAIS - CÍVEL MASSA', 
                          'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE'), col("VALOR")),
    (col("SUB_TIPO").isin('INSS - TRABALHISTA', 'IR - TRABALHISTA', 'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)',
                          'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA ',
                          'RNO - FGTS', 'FGTS'), col("VALOR")),
    (col("SUB_TIPO").isin('LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA'), col("VALOR")),
    (col("SUB_TIPO").isin('PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO', 'PENSÃO - CÍVEL MASSA'), col("VALOR")),
    (col("SUB_TIPO").isin('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)',
                          'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO',
                          'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL',
                          'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO',
                          'LIMINAR (CÍV. MASSA)', 'PAGAMENTO', 'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA',
                          'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK'), col("VALOR"))
]

# Use a função 'when' para aplicar as condições e criar novas colunas
historico_pagamentos_jud_civel = historico_de_pagamentos_civel_1 \
    .withColumn("ACORDO", when(condicoes[0][0], condicoes[0][1])) \
    .withColumn("CONDENACAO", when(condicoes[1][0], condicoes[1][1])) \
    .withColumn("CUSTAS", when(condicoes[2][0], condicoes[2][1])) \
    .withColumn("IMPOSTO", when(condicoes[3][0], condicoes[3][1])) \
    .withColumn("PENHORA", when(condicoes[4][0], condicoes[4][1])) \
    .withColumn("PENSAO", when(condicoes[5][0], condicoes[5][1])) \
    .withColumn("OUTROS_PAGAMENTOS", when(condicoes[6][0], condicoes[6][1])) \
    .filter((col("SUB_TIPO").isin('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)',
                                   'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO',
                                   'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL',
                                   'ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA',
                                   'ACORDO - TRABALHISTA', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO',
                                   'ATIVO', 'CONDENAÇÃO - CÍVEL', 'CONDENAÇÃO - CÍVEL ESTRATÉGICO',
                                   'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)',
                                   'CONDENAÇÃO - REGULATÓRIO', 'CONDENAÇÃO - TRABALHISTA',
                                   'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CUSTAS FK - TRABALHISTA',
                                   'CUSTAS PERITOS - CÍVEL', 'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL',
                                   'CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA',
                                   'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO',
                                   'CUSTAS PROCESSUAIS - TRABALHISTA', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA',
                                   'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA',
                                   'HONORÁRIOS PERICIAIS - IMOBILIÁRIO', 'HONORÁRIOS PERICIAIS - TRABALHISTA',
                                   'LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA',
                                   'LIMINAR (CÍV. MASSA)', 'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL',
                                   'MULTA PROCESSUAL - IMOBILIÁRIO', 'MULTA PROCESSUAL - TRABALHISTA', 'PAGAMENTO',
                                   'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA',
                                   'PAGAMENTO DE ACORDO - TRABALHISTA (FK)', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO',
                                   'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)',
                                   'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK', 'PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO',
                                   'PENSÃO - CÍVEL MASSA', 'PERITOS - REGULATÓRIO', 'RNO - ACORDO - TRABALHISTA',
                                   'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)',
                                   'RNO - CUSTAS PROCESSUAIS - TRABALHISTA', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA',
                                   'RNO - MULTA PROCESSUAL - TRABALHISTA', 'INSS - TRABALHISTA', 'IR - TRABALHISTA',
                                   'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)', 'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)',
                                   'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA ', 'RNO - FGTS', 'FGTS',
                                   'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA',
                                   'ACORDO - CÍVEL MASSA - CARTÕES', 'ACORDO - CÍVEL MASSA - FORNECEDOR',
                                   'ACORDO - CÍVEL MASSA - MKTPLACE', 'ACORDO - CÍVEL MASSA - SEGURO',
                                   'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)',
                                   'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR',
                                   'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO',
                                   'CONDENAÇÃO - CÍVEL MASSA - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR',
                                   'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA - SEGURO',
                                   'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR',
                                   'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE'))
          & ~col("STATUS_DO_PAGAMENTO").isin('REMOVIDO', 'CANCELADO', 'REJEITADO', 'EM CORREÇÃO', 'PENDENTE')
          & ~col("STATUS_DO_PAGAMENTO").like('%PAGAMENTO DEVOLVIDO%'))

# COMMAND ----------

c.count()

# COMMAND ----------

from pyspark.sql.functions import col, coalesce

historico_pagamentos_jud_civel_1 = historico_pagamentos_jud_civel.withColumn(
    "DATA_EFETIVA_DO_PAGAMENTO",
    coalesce(col("DATA_EFETIVA_DO_PAGAMENTO"), col("DATA_SOLICITAÇÃO_ESCRITÓRIO"))
)

# COMMAND ----------

historico_pagamentos_jud_civel_1 = historico_pagamentos_jud_civel_1.withColumnRenamed("DATA_EFETIVA_DO_PAGAMENTO", "DATA_EFETIVA_PAGAMENTO")

# COMMAND ----------

from pyspark.sql.functions import col, sum, month, year, expr

# Agrupe a tabela de pagamentos de acordos e condenações
historico_pagamentos_jud_civel_2 = historico_pagamentos_jud_civel_1 \
    .groupBy(col("PROCESSO_ID").alias("PROCESSO_ID"), month("DATA_EFETIVA_PAGAMENTO").alias("MES"), year("DATA_EFETIVA_PAGAMENTO").alias("ANO")) \
    .agg(sum("ACORDO").alias("ACORDO"), sum("CONDENACAO").alias("CONDENACAO"), 
         sum("PENHORA").alias("PENHORA"), sum("PENSAO").alias("PENSAO"), 
         sum("OUTROS_PAGAMENTOS").alias("OUTROS_PAGAMENTOS")) \
    .withColumn("DATA_EFETIVA_PAGAMENTO", expr("to_date(concat(MES,'/01/', ANO), 'MM/dd/yyyy')"))

# Filtrar os resultados onde a soma dos pagamentos não é nula
historico_pagamentos_jud_civel_2 = historico_pagamentos_jud_civel_2 \
    .filter((col("ACORDO").isNotNull()) | (col("CONDENACAO").isNotNull()) | 
            (col("PENHORA").isNotNull()) | (col("PENSAO").isNotNull()) | 
            (col("OUTROS_PAGAMENTOS").isNotNull()))\
                .drop("MES", "ANO")

# COMMAND ----------

historico_pagamentos_jud_civel_2.count()

# COMMAND ----------

from pyspark.sql.functions import col, month, year, sum as spark_sum, when

#AGRUPA TABELA DE PAGAMENTOS DE IMPOSTOS
historico_pagamentos_jud_civel_3 = historico_pagamentos_jud_civel_1\
.groupBy(col("PROCESSO_ID").alias("PROCESSO_ID"), month("DATA_EFETIVA_PAGAMENTO").alias("MES"), year("DATA_EFETIVA_PAGAMENTO").alias("ANO"))\
.agg(spark_sum("IMPOSTO").alias("IMPOSTO"))\
.filter(col("IMPOSTO").isNotNull())\
.withColumn("DATA_EFETIVA_PAGAMENTO", expr("to_date(concat(MES,'/01/', ANO), 'MM/dd/yyyy')"))\
.drop("MES", "ANO")

# COMMAND ----------

historico_pagamentos_jud_civel_3.count()

# COMMAND ----------

tb_pagamentos_f_civel =historico_pagamentos_jud_civel_2.join(historico_pagamentos_jud_civel_3, ['PROCESSO_ID', 'DATA_EFETIVA_PAGAMENTO'], 'full')

# COMMAND ----------

 #Crie um DataFrame com os valores da tabela fase aging
fase_aging= [
    ("low - 1", "Até 1 Ano"),
    ("1 - 3", "1 a 3 Anos"),
    ("3 - 5", "3 a 5 Anos"),
    ("5 - 10", "5 a 10 Anos"),
    ("10 - high", "Acima de 10 Anos")
]

schema = ["value", "fase_aging"]

fase_aging = spark.createDataFrame(fase_aging, schema)

# Exiba a tabela auxiliar
display(fase_aging)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Base de Garantias

# COMMAND ----------

display(garantias_civel)

# COMMAND ----------

#Criação do Dataframe Spark e criação do Schema da tabela
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

schema = StructType([
    StructField("PROCESSO_ID", IntegerType(), True),
    StructField("DATA_REGISTRADO_PROCESSO", DateType(), True),
    StructField("PASTA", StringType(), True),
    StructField("STATUS", StringType(), True),
    StructField("GRUPO", StringType(), True),
    StructField("ÁREA_DO_DIREITO", StringType(), True),
    StructField("NÚMERO_DO_PROCESSO", StringType(), True),
    StructField("CENTRO_DE_CUSTO_ÁREA_DEMANDANTE_CÓDIGO", StringType(), True),
    StructField("CC_DO_SÓCIO", StringType(), True),
    StructField("EMPRESA", StringType(), True),
    StructField("ESCRITÓRIO_RESPONSÁVEL", StringType(), True),
    StructField("PARTE_CONTRÁRIA_NOME", StringType(), True),
    StructField("STATUS_DA_GARANTIA", StringType(), True),
    StructField("ID", IntegerType(), True),
    StructField("TIPO_GARANTIA", StringType(), True),
    StructField("VALOR", FloatType(), True),
    StructField("VALOR_ATUALIZADO", FloatType(), True),
    StructField("RESPONSÁBILIDADE_COMPRADOR_PERCENTUAL", FloatType(), True),
    StructField("RESPONSÁBILIDADE_VENDEDOR_PERCENTUAL", FloatType(), True),
    StructField("DATA_DE_LEVANTAMENTO_EMPRESA", DateType(), True),
    StructField("VALOR_LEVANTADO_EMPRESA", FloatType(), True),
    StructField("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA", DateType(), True),
    StructField("VALOR_LEVANTADO_PARTE_CONTRÁRIA", FloatType(), True),
    StructField("SALDO", FloatType(), True),
    StructField("JUROS", FloatType(), True),
    StructField("INDICE", StringType(), True),
    StructField("ID_1", IntegerType(), True),
    StructField("BANCO_DEPÓSITO_JUDICIAL", StringType(), True),
    StructField("NÚMERO_DA_CONTA_JUDICIAL", StringType(), True),
    StructField("DATA_DA_SOLICITAÇÃO_DO_PAGAMENTO", DateType(), True),
    StructField("DATA_LIMITE_DE_PAGAMENTO", DateType(), True),
    StructField("DATA_DE_VENCIMENTO", DateType(), True),
    StructField("DATA_EFETIVA_DO_PAGAMENTO", DateType(), True),
    StructField("NÚMERO_DO_DOCUMENTO_SAP", StringType(), True),
    StructField("ID_JUDICIAL", StringType(), True),
    StructField("CONTA_JUDICIAL_ID_JUDICIAL", StringType(), True),
    StructField("PROCESSO_ESTADO", StringType(), True),
    StructField("DATA_DO_ALVARÁ", DateType(), True),
    StructField("DATA_DA_CI", DateType(), True),
    StructField("NÚMERO_DA_CI", FloatType(), True),
    StructField("TIPO_DO_SEGURO", StringType(), True),
    StructField("NÚMERO_DA_APÓLICE", StringType(), True),
    StructField("NÚMERO_DO_ENDOSSO", StringType(), True),
    StructField("NOME_DA_SEGURADORA", StringType(), True),
    StructField("CARTEIRA", StringType(), True),
    StructField("FASE", StringType(), True),
    StructField("ESTÁ_NA_BASE_DE_NÃO_IDENTIFICADOS", StringType(), True),
    StructField("INDICAR_EMPRESA", StringType(), True),
    StructField("DATA_REGISTRADO_JUDICIAL", DateType(), True),
    StructField("NÚMERO_DA_GARANTIA_JUDICIAL", StringType(), True),
    StructField("DATA_DA_EMISSÃO_JUDICIAL", DateType(), True),
    StructField("NOME_DA_EMPRESA_JUDICIAL", StringType(), True),
    StructField("DATA_DA_VIGÊNCIA_JUDICIAL", DateType(), True),
    StructField("VALORES_DATA_DA_VIGÊNCIA", StringType(), True),
    StructField("VALORES_DATA_DA_VIGÊNCIA_DA_APÓLICE", StringType(), True),
    StructField("VALORES_DATA_DA_EMISSÃO_DA_APÓLICE", DateType(), True),
    StructField("PROCESSO_VARA_ÓRGÃO", StringType(), True),
    StructField("VALORES_MOTIVOS_DA_PENHORA", StringType(), True),
    StructField("ADVOGADO_RESPONSÁVEL", StringType(), True)
])

# Converter DataFrame Pandas para DataFrame PySpark com o esquema definido
hist_gar = spark.createDataFrame(garantias_1, schema=schema)



# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformação Base de Garantias

# COMMAND ----------

# Renomear e tratar campos da Base de Garantias para formato Pyspark
garantias = hist_gar.toDF(*[col.replace(' ', '_') for col in hist_gar.columns])
garantias = hist_gar.toDF(*[col.replace('(', '') for col in hist_gar.columns])
garantias = hist_gar.toDF(*[col.replace(')', '') for col in hist_gar.columns])
garantias = hist_gar.toDF(*[col.replace('´', '') for col in hist_gar.columns])

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Dados
mes_contabil_dict = {
    '01/11/2018':[('01/01/1900','20/11/2018 23:59:59')],
    '01/12/2018':[('21/11/2018','17/12/2018 23:59:59')],
    '01/01/2019':[('18/12/2018','21/01/2019 23:59:59')],
    '01/02/2019':[('22/01/2019','18/02/2019 23:59:59')],
    '01/03/2019':[('19/02/2019','19/03/2019 23:59:59')],
    '01/04/2019':[('20/03/2019','21/04/2019 23:59:59')],
    '01/05/2019':[('22/04/2019','21/05/2019 23:59:59')],
    '01/06/2019':[('22/05/2019','17/06/2019 23:59:59')],
    '01/07/2019':[('18/06/2019','21/07/2019 23:59:59')],
    '01/08/2019':[('22/07/2019','20/08/2019 23:59:59')],
    '01/09/2019':[('21/08/2019','22/09/2019 23:59:59')],
    '01/10/2019':[('23/09/2019','23/10/2019 23:59:59')],
    '01/11/2019':[('24/10/2019','21/11/2019 23:59:59')],
    '01/12/2019':[('22/11/2019','19/12/2020 23:59:59')],
    '01/01/2020':[('20/12/2019','26/01/2020 23:59:59')],
    '01/02/2020':[('27/01/2020','28/02/2020 23:59:59')],
    '01/03/2020':[('29/02/2020','24/03/2020 23:59:59')],
    '01/04/2020':[('25/03/2020','20/04/2020 23:59:59')],
    '01/05/2020':[('21/04/2020','19/05/2020 23:59:59')],
    '01/06/2020':[('20/05/2020','23/06/2020 23:59:59')],
    '01/07/2020':[('24/06/2020','29/07/2020 23:59:59')],
    '01/08/2020':[('30/07/2020','21/08/2020 23:59:59')],
    '01/09/2020':[('22/08/2020','22/09/2020 23:59:59')],
    '01/10/2020':[('23/09/2020','22/10/2020 23:59:59')],
    '01/11/2020':[('23/10/2020','24/11/2020 23:59:59')],
    '01/12/2020':[('25/11/2020','18/12/2020 23:59:59')],
    '01/01/2021':[('19/12/2020','23/01/2021 23:59:59')],
    '01/02/2021':[('24/01/2021','22/02/2021 23:59:59')],
    '01/03/2021':[('23/02/2021','21/03/2021 23:59:59')],
    '01/04/2021':[('22/03/2021','21/04/2021 23:59:59')],
    '01/05/2021':[('22/04/2021','23/05/2021 23:59:59')],
    '01/06/2021':[('24/05/2021','21/06/2021 23:59:59')],
    '01/07/2021':[('22/06/2021','22/07/2021 23:59:59')],
    '01/08/2021':[('23/07/2021','22/08/2021 23:59:59')],
    '01/10/2022':[('22/09/2022','21/10/2022 23:59:59')],
    '01/09/2022':[('24/08/2022','21/09/2022 23:59:59')],
    '01/11/2022':[('22/10/2022','18/11/2022 23:59:59')],
    '01/12/2022':[('19/11/2022','22/12/2022 23:59:59')],
    '01/01/2023':[('23/12/2022','20/01/2023 23:59:59')],
    '01/02/2023':[('21/01/2023','17/02/2023 23:59:59')],
    '01/03/2023':[('18/02/2023','21/03/2023 23:59:59')],
    '01/04/2023':[('22/03/2023','02/05/2023 23:59:59')],
    '01/05/2023':[('03/05/2023','22/05/2023 23:59:59')],
    '01/06/2023':[('23/05/2023','22/06/2023 23:59:59')],
    '01/07/2023':[('23/06/2023','23/07/2023 23:59:59')],
    '01/08/2023':[('24/07/2023','22/08/2023 23:59:59')],
    '01/09/2023':[('23/08/2023','22/09/2023 23:59:59')],
    '01/10/2023':[('23/09/2023','23/10/2023 23:59:59')],
    '01/11/2023':[('24/10/2023','22/11/2023 23:59:59')],
    '01/12/2023':[('23/11/2023','15/12/2023 23:59:59')],
    '01/01/2024':[('16/12/2023','22/01/2024 23:59:59')],
    '01/02/2024':[('23/01/2024','22/02/2024 23:59:59')],
    '01/03/2024':[('23/03/2024','22/03/2024 23:59:59')],
    '01/04/2024':[('22/03/2024','22/04/2024 23:59:59')]
}

# COMMAND ----------

from pyspark.sql.functions import col

garantias_filter_civel = [
    'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL',
    'DEPÓSITO',
    'DEPÓSITO GARANTIA DE JUIZO - CÍVEL',
    'DEPÓSITO JUDICIAL',
    'DEPÓSITO JUDICIAL - CÍVEL',
    'DEPÓSITO JUDICIAL - CÍVEL ESTRATÉGICO',
    'DEPÓSITO JUDICIAL - CÍVEL MASSA',
    'DEPÓSITO JUDICIAL - REGULATÓRIO',
    'DEPÓSITO JUDICIAL TRIBUTÁRIO',
    'DEPÓSITO RECURSAL - AIRR',
    'DEPÓSITO RECURSAL - CÍVEL MASSA',
    'DEPÓSITO RECURSAL - EMBARGOS TST',
    'DEPÓSITO RECURSAL AIRR',
    'DEPÓSITO RECURSAL RO',
    'PENHORA - GARANTIA',
    'PENHORA - REGULATÓRIO'
]

tb_garantias_mes_a_civel = garantias_civel.where(
    (col('STATUS_DA_GARANTIA').isNotNull()) &
    (~col('STATUS_DA_GARANTIA').like('%PAGAMENTO DEVOLVIDO%')) &
    (col('STATUS') != 'REMOVIDO') &
    (col('TIPO_GARANTIA').isin(garantias_filter_civel))
)



# COMMAND ----------

display(tb_garantias_mes_a_civel)

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, col, when, date_format

# Convertendo a lista mes_contabil para uma lista
mes_contabil_list = list(mes_contabil_dict.keys())

# Traduzindo o código SAS para Pyspark
df_filtered_garantias = tb_garantias_mes_a_civel.filter(col("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA").isNotNull()) \
    .groupBy("PROCESSO_ID", "STATUS_DA_GARANTIA", "DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA") \
    .agg(spark_sum("VALOR_LEVANTADO_PARTE_CONTRÁRIA").alias("VALOR_LEVANTADO_PARTE_CONTRÁRIA")) \
    .withColumn("MES_CONTABIL", date_format("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA", "dd/MM/yyyy"))

# Aplicando a lógica para atualizar a coluna MES_CONTABIL
for mes, intervalos in mes_contabil_dict.items():
    for intervalo in intervalos:
        inicio, fim = intervalo
        df_filtered_garantias = df_filtered_garantias.withColumn("MES_CONTABIL",
                                             when((col("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA") >= inicio) & 
                                                  (col("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA") <= fim),
                                                  mes)
                                             .otherwise(col("MES_CONTABIL")))

# Exibindo o resultado
df_filtered_garantias.show()


# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, col, when, lit, to_date, concat, year, month

# Lógica atualizada para ajustar MES_CONTABIL

# Considerando que a coluna DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA já é uma data, caso contrário, converter usando to_date

tb_garantias_f_civel = tb_garantias_mes_a_civel.filter(col("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA").isNotNull()) \
    .groupBy("PROCESSO_ID", "STATUS_DA_GARANTIA", "DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA") \
    .agg(spark_sum("VALOR_LEVANTADO_PARTE_CONTRÁRIA").alias("GARANTIAS")) \
    .withColumn("MES_CONTABIL", to_date(concat(year("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA"), lit("-"), month("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA"), lit("-01")), "yyyy-MM-dd"))

# Ajuste na lógica para atualizar a coluna MES_CONTABIL conforme o intervalo, alterando o resultado para o primeiro dia do mês
for mes, intervalos in mes_contabil_dict.items():
    for intervalo in intervalos:
        inicio, fim = intervalo
        tb_garantias_f_civel = tb_garantias_f_civel.withColumn(
            "MES_CONTABIL",
            when(
                (col("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA") >= to_date(lit(inicio), "yyyy-MM-dd")) &
                (col("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA") <= to_date(lit(fim), "yyyy-MM-dd")),
                to_date(concat(lit(mes[4:]), lit("-"), lit(mes[:2]), lit("-01")), "yyyy-MM-dd")
            ).otherwise(col("MES_CONTABIL"))
        )

# Exibindo o resultado, note que .show() pode ser retirado em produção para não interromper fluxos de execução longos


# COMMAND ----------

# Corrected Python code to include necessary columns for join condition
hist_pagamentos_garantias_f_civel =tb_pagamentos_f_civel.join(
    tb_garantias_f_civel.select("PROCESSO_ID", "DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA", "GARANTIAS"),
    (tb_pagamentos_f_civel["PROCESSO_ID"] == tb_garantias_f_civel["PROCESSO_ID"]) & 
    (tb_pagamentos_f_civel["DATA_EFETIVA_PAGAMENTO"] == tb_garantias_f_civel["DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA"]),
    "full" # Specifies a full join
)

# If you want to only include "GARANTIAS" column from `tb_garantias_f` in the final result,
# perform a select operation after the join
hist_pagamentos_garantias_f_civel = hist_pagamentos_garantias_f_civel.select(
    tb_pagamentos_f_civel["*"], 
    "GARANTIAS"
)



# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

display(hist_pagamentos_garantias_f_civel)

# COMMAND ----------

hist_pagamentos_garantias_f_civel.write.format("delta").mode("overwrite").saveAsTable("databox.juridico_comum.hist_pagamentos_garantias_civel_f")

# COMMAND ----------


