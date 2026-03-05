# Databricks notebook source
# MAGIC %md
# MAGIC ####Importação Bases Gerenciais Cível

# COMMAND ----------

# MAGIC %md
# MAGIC ####Fechamento Cível

# COMMAND ----------

# MAGIC %md
# MAGIC ####Import da tabela de Ativos Cível

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

dbutils.widgets.text("civel_encer_historico", "", "civel_encer_historico")

# COMMAND ----------

nmtabela = dbutils.widgets.get("nmtabela")
civel_encer_historico = dbutils.widgets.get("civel_encer_historico")

# COMMAND ----------

# nmtabela antigo é 0228
# Caminho das pastas e arquivos

nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/'

arquivo_ativos = f'CIVEL_GERENCIAL-(ATIVOS)-{nmtabela}.xlsx'
arquivo_encerrados = f'CIVEL_GERENCIAL-(ENCERRADO)-{nmtabela}.xlsx'
arquivo_encerrados_historico = f'CIVEL_ENCERRADOS_HISTORICO - {civel_encer_historico}.xlsx'

path_ativos = diretorio_origem + arquivo_ativos
path_encerrados = diretorio_origem + arquivo_encerrados
path_encerrados_historico = diretorio_origem + arquivo_encerrados_historico




# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive","true")

# Carrega as planilhas em Spark Data Frames
gerencial_ativos_civel = read_excel(path_ativos, "'CÍVEL'!A6")
gerencial_encerrados_civel = read_excel(path_encerrados, "'CÍVEL'!A6")
gerencial_encerrados_civel_historico = read_excel(path_encerrados_historico, "'CÍVEL ENCERRADOS HISTÓRICO'!A6")


# COMMAND ----------

dataframes = [gerencial_ativos_civel, gerencial_encerrados_civel, gerencial_encerrados_civel_historico]  # Lista de DataFrames

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

gerencial_encerrados_civel_historico.printSchema()

# COMMAND ----------

lista_col_data_americana = ['DATA DO FATO GERADOR79','DATA DO FATO GERADOR81', 'DATA DO FATO GERADOR82', 'DATA FATO GERADOR']
gerencial_ativos_civel = convert_to_date_format_americano(gerencial_ativos_civel,lista_col_data_americana)
gerencial_encerrados_civel = convert_to_date_format_americano(gerencial_encerrados_civel,lista_col_data_americana)


# COMMAND ----------

lista_col_data_americana = ['DATA DO FATO GERADOR79', 'DATA DO FATO GERADOR81', 'DATA DO FATO GERADOR82', 'DATA FATO GERADOR']
gerencial_encerrados_civel_historico = convert_to_date_format_americano(gerencial_encerrados_civel_historico,lista_col_data_americana)

# COMMAND ----------

# Lista as colunas com datas
ativos_data_cols = find_columns_with_word(gerencial_ativos_civel, 'DATA ')
ativos_data_cols.append('DISTRIBUIÇÃO')

encerrados_data_cols = find_columns_with_word(gerencial_encerrados_civel, 'DATA ')
encerrados_data_cols.append('DISTRIBUIÇÃO')

encerrados_historico_data_cols = find_columns_with_word(gerencial_encerrados_civel_historico, 'DATA ')
encerrados_historico_data_cols.append('DISTRIBUIÇÃO')

print("ativos_data_cols")
print(ativos_data_cols)
print("\n")
print("encerrados_data_cols")
print(encerrados_data_cols)
print("\n")
print("encerrados_historico_data_cols")
print(encerrados_historico_data_cols)

# COMMAND ----------

gerencial_ativos_civel = convert_to_date_format(gerencial_ativos_civel, ativos_data_cols)
gerencial_encerrados_civel = convert_to_date_format(gerencial_encerrados_civel, encerrados_data_cols)
gerencial_encerrados_civel_historico = convert_to_date_format(gerencial_encerrados_civel_historico, encerrados_historico_data_cols)

# COMMAND ----------

gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DA COMPRA", "DATA_DA_COMPRA")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DO PEDIDO", "DATA_DO_PEDIDO")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DO FATO GERADOR79", "DATA_FATO_GERADOR_1")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA FATO GERADOR", "DATA_FATO_GERADOR_2")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DO FATO GERADOR81", "DATA_FATO_GERADOR_3")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("DATA DO FATO GERADOR82", "DATA_FATO_GERADOR_4")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("CENTRO DE CUSTO / ÁREA DEMANDANTE - CÓDIGO", "CENTRO_DE_CUSTO_AREA_DEMANDANTE")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("COMPRA DIRETA OU MARKETPLACE? - NEW ", "DIRETA_OU_MKT")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("SUB-ÁREA DO DIREITO", "SUB_AREA_DO_DIREITO")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("ASSUNTO (CÍVEL) - PRINCIPAL", "ASSUNTO_CIVEL_PRI")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("ASSUNTO (CÍVEL) - ASSUNTO", "ASSUNTO_CIVEL_ASS")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("PROCESSO - Nº. PEDIDO", "NUM_PEDIDO")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("AÇÃO", "ACAO")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("ESCRITÓRIO EXTERNO", "ESCRITORIO")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("RECLAMAÇÃO BLACK FRIDAY?", "RECLAMACAO_BLACK_FRIDAY")
gerencial_ativos_civel = gerencial_ativos_civel.withColumnRenamed("FABRICANTE DO PRODUTO", "FABRICANTE")

gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DA COMPRA", "DATA_DA_COMPRA")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO PEDIDO", "DATA_DO_PEDIDO")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO FATO GERADOR79", "DATA_FATO_GERADOR_1")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA FATO GERADOR", "DATA_FATO_GERADOR_2")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO FATO GERADOR81", "DATA_FATO_GERADOR_3")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("DATA DO FATO GERADOR82", "DATA_FATO_GERADOR_4")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("CENTRO DE CUSTO / ÁREA DEMANDANTE - CÓDIGO", "CENTRO_DE_CUSTO_AREA_DEMANDANTE")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("COMPRA DIRETA OU MARKETPLACE? - NEW ", "DIRETA_OU_MKT")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("SUB-ÁREA DO DIREITO", "SUB_AREA_DO_DIREITO")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("ASSUNTO (CÍVEL) - PRINCIPAL", "ASSUNTO_CIVEL_PRI")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("ASSUNTO (CÍVEL) - PRINCIPAL", "ASSUNTO_CIVEL_PRI")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("ASSUNTO (CÍVEL) - ASSUNTO", "ASSUNTO_CIVEL_ASS")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("PROCESSO - Nº. PEDIDO", "NUM_PEDIDO")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("AÇÃO", "ACAO")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("ESCRITÓRIO EXTERNO", "ESCRITORIO")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("RECLAMAÇÃO BLACK FRIDAY?", "RECLAMACAO_BLACK_FRIDAY")
gerencial_encerrados_civel = gerencial_encerrados_civel.withColumnRenamed("FABRICANTE DO PRODUTO", "FABRICANTE")

gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DA COMPRA", "DATA_DA_COMPRA")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO PEDIDO", "DATA_DO_PEDIDO")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO FATO GERADOR79", "DATA_FATO_GERADOR_1")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA FATO GERADOR", "DATA_FATO_GERADOR_2")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO FATO GERADOR81", "DATA_FATO_GERADOR_3")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("DATA DO FATO GERADOR82", "DATA_FATO_GERADOR_4")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("CENTRO DE CUSTO / ÁREA DEMANDANTE - CÓDIGO", "CENTRO_DE_CUSTO_AREA_DEMANDANTE")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("COMPRA DIRETA OU MARKETPLACE? - NEW ", "DIRETA_OU_MKT")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("SUB-ÁREA DO DIREITO", "SUB_AREA_DO_DIREITO")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("ASSUNTO (CÍVEL) - PRINCIPAL", "ASSUNTO_CIVEL_PRI")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("ASSUNTO (CÍVEL) - ASSUNTO", "ASSUNTO_CIVEL_ASS")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("PROCESSO - Nº. PEDIDO", "NUM_PEDIDO")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("AÇÃO", "ACAO")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("ESCRITÓRIO EXTERNO", "ESCRITORIO")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("RECLAMAÇÃO BLACK FRIDAY?", "RECLAMACAO_BLACK_FRIDAY")
gerencial_encerrados_civel_historico = gerencial_encerrados_civel_historico.withColumnRenamed("FABRICANTE DO PRODUTO", "FABRICANTE")

# COMMAND ----------

columns = gerencial_encerrados_civel_historico.columns

for i, col in enumerate(columns):
    print(str(i) + " : " + str(col))

# COMMAND ----------

#Encerrar spark.conf.set

spark.conf.set("spark.sql.caseSensitive","false")


# COMMAND ----------

#Encontrar a Data Máxima do Fato Gerador
from pyspark.sql.functions import greatest, to_date

gerencial_ativos_civel_1 = gerencial_ativos_civel.withColumn("DATA_FATO_GERADOR", to_date(greatest("DATA_DO_PEDIDO", "DATA_DA_COMPRA", "DATA_FATO_GERADOR_1", "DATA_FATO_GERADOR_2", "DATA_FATO_GERADOR_3", "DATA_FATO_GERADOR_4"))
)
gerencial_encerrados_civel_1 = gerencial_encerrados_civel.withColumn("DATA_FATO_GERADOR", to_date(greatest("DATA_DO_PEDIDO", "DATA_DA_COMPRA", "DATA_FATO_GERADOR_1", "DATA_FATO_GERADOR_2", "DATA_FATO_GERADOR_3", "DATA_FATO_GERADOR_4"))
)
gerencial_encerrados_civel_historico_1 = gerencial_encerrados_civel_historico.withColumn("DATA_FATO_GERADOR", to_date(greatest("DATA_DO_PEDIDO", "DATA_DA_COMPRA", "DATA_FATO_GERADOR_1", "DATA_FATO_GERADOR_2", "DATA_FATO_GERADOR_3", "DATA_FATO_GERADOR_4"))
)

# COMMAND ----------

# Ajusta os nomes das colunas
gerencial_ativos_civel_2 = adjust_column_names(gerencial_ativos_civel_1)
gerencial_encerrados_civel_2 = adjust_column_names(gerencial_encerrados_civel_1)
gerencial_encerrados_civel_historico_2 = adjust_column_names(gerencial_encerrados_civel_historico_1)

# COMMAND ----------

selected_columns_civel_ativos = ['PROCESSO_ID', 'ÁREA_DO_DIREITO','PARTE_CONTRÁRIA_CPF', 'PARTE_CONTRÁRIA_NOME',  'STATUS', 'GRUPO',
                    'MOTIVO_DE_ENCERRAMENTO', 'CENTRO_DE_CUSTO_AREA_DEMANDANTE',
                    'EMPRESA', 'VALOR_DA_CAUSA', 'ESFERA', 'ACAO', 'ESCRITORIO',
                    'FASE', 'DATA_AUDIENCIA_INICIAL', 'LISTA_LOJISTA_NOME',
                    'LISTA_LOJISTA_ID', 'FABRICANTE', 'PRODUTO',
                    'RECLAMACAO_BLACK_FRIDAY',  'DATA_DO_PEDIDO',
                     'TRANSPORTADORA', 'ESTEIRA', 'ASSUNTO_CIVEL_PRI',
                    'ASSUNTO_CIVEL_ASS', 'ESTADO',
                    'DIRETA_OU_MKT', 'INDIQUE_A_FILIAL',
                    'DATA_DA_COMPRA', 'DISTRIBUIÇÃO',
                    'DATA_DE_RECEBIMENTO', 'DATA_REGISTRADO','DATA_FATO_GERADOR','NUM_PEDIDO','COMARCA','SUB_AREA_DO_DIREITO','CÍVEL_MASSA_RESPONSABILIDADE','JUSTIFICATIVA_DEFINIR_ESTRATÉGIA_DE_ATUAÇÃO_E_JUSTIFICATIVA']

gerencial_ativos_civel_final = gerencial_ativos_civel_2.select(*selected_columns_civel_ativos)

# COMMAND ----------

selected_columns_civel_encerrado =['PROCESSO_ID',  'ÁREA_DO_DIREITO', 'PARTE_CONTRÁRIA_CPF','PARTE_CONTRÁRIA_NOME',  'STATUS', 'GRUPO',
                    'MOTIVO_DE_ENCERRAMENTO', 'CENTRO_DE_CUSTO_AREA_DEMANDANTE',
                    'EMPRESA', 'VALOR_DA_CAUSA', 'ESFERA', 'ACAO', 'ESCRITORIO',
                    'FASE', 'DATA_AUDIENCIA_INICIAL', 'LISTA_LOJISTA_NOME',
                    'LISTA_LOJISTA_ID', 'FABRICANTE', 'PRODUTO',
                    'RECLAMACAO_BLACK_FRIDAY',  'DATA_DO_PEDIDO',
                     'TRANSPORTADORA', 'ESTEIRA', 'ASSUNTO_CIVEL_PRI',
                    'ASSUNTO_CIVEL_ASS', 'ESTADO',
                    'DIRETA_OU_MKT', 'INDIQUE_A_FILIAL', 
                    'DATA_DA_COMPRA', 'DISTRIBUIÇÃO', 
                    'DATA_DE_RECEBIMENTO', 'DATA_REGISTRADO','DATA_FATO_GERADOR','NUM_PEDIDO','COMARCA','SUB_AREA_DO_DIREITO','CÍVEL_MASSA_RESPONSABILIDADE','JUSTIFICATIVA_DEFINIR_ESTRATÉGIA_DE_ATUAÇÃO_E_JUSTIFICATIVA']
                    
gerencial_encerrados_civel_final = gerencial_encerrados_civel_2.select(*selected_columns_civel_encerrado)

# COMMAND ----------

selected_columns_civel_historico =['PROCESSO_ID',  'ÁREA_DO_DIREITO', 'PARTE_CONTRÁRIA_CPF','PARTE_CONTRÁRIA_NOME', 'STATUS', 'GRUPO',
                    'MOTIVO_DE_ENCERRAMENTO', 'CENTRO_DE_CUSTO_AREA_DEMANDANTE',
                    'EMPRESA', 'VALOR_DA_CAUSA', 'ESFERA', 'ACAO', 'ESCRITORIO',
                    'FASE', 'DATA_AUDIENCIA_INICIAL', 'LISTA_LOJISTA_NOME',
                    'LISTA_LOJISTA_ID', 'FABRICANTE', 'PRODUTO', 
                    'RECLAMACAO_BLACK_FRIDAY',  'DATA_DO_PEDIDO',
                    'TRANSPORTADORA', 'ESTEIRA', 'ASSUNTO_CIVEL_PRI',
                    'ASSUNTO_CIVEL_ASS', 'ESTADO',
                    'DIRETA_OU_MKT', 'INDIQUE_A_FILIAL', 
                    'DATA_DA_COMPRA', 'DISTRIBUIÇÃO', 
                    'DATA_DE_RECEBIMENTO', 'DATA_REGISTRADO','DATA_FATO_GERADOR','NUM_PEDIDO','COMARCA','SUB_AREA_DO_DIREITO','CÍVEL_MASSA_RESPONSABILIDADE']
gerencial_encerrados_civel_historico_final = gerencial_encerrados_civel_historico_2.select(*selected_columns_civel_historico)

# COMMAND ----------

# Registrando DataFrames como tabelas temporárias
gerencial_ativos_civel_final.createOrReplaceTempView("ATIVOS_CIVEL_GERENCIAL")
gerencial_encerrados_civel_final.createOrReplaceTempView("ENCERRADOS_CIVEL_GERENCIAL")
gerencial_encerrados_civel_historico_final.createOrReplaceTempView("ENCERRADOS_CIVEL_GERENCIAL_HISTORICO")



# COMMAND ----------

# Executando consulta SQL para empilhar os DataFrames com UNION ALL
tb_ger_civel = spark.sql("""
    SELECT * FROM ATIVOS_CIVEL_GERENCIAL
    UNION ALL
    SELECT * FROM ENCERRADOS_CIVEL_GERENCIAL

""")

tb_ger_civel.createOrReplaceTempView('tb_ger_civel_')

# COMMAND ----------

#Filtro com a remoção de ID_PROCESSO nulo
from pyspark.sql import functions as F

tb_ger_civel_filtered = tb_ger_civel.filter(F.col('PROCESSO_ID').isNotNull())

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Ordenar o DataFrame
tb_ger_civel_sorted = tb_ger_civel_filtered.orderBy(
    "PROCESSO_ID",
    desc("ASSUNTO_CIVEL_ASS"),
    desc("DATA_FATO_GERADOR")
)


# COMMAND ----------

tb_ger_civel_1 = tb_ger_civel_sorted.dropDuplicates(["PROCESSO_ID"])

# COMMAND ----------

# Criar uma visão temporária para executar consultas SQL
tb_ger_civel_1.createOrReplaceTempView("TB_CIVEL_TESTE")

# COMMAND ----------

# Calcula a frequência da coluna 'sua_coluna' usando SQL corrigido para lidar com caracteres especiais
frequencia = spark.sql("""
    SELECT `DISTRIBUIÇÃO`, COUNT(*) AS Frequencia
    FROM TB_CIVEL_TESTE
    GROUP BY  `DISTRIBUIÇÃO`
    ORDER BY Frequencia DESC
""")

display(frequencia)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gravação da Tabela Gerencial do Databox (Delta Table)

# COMMAND ----------

tb_ger_civel_1.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("databox.juridico_comum.TB_GER_CIVEL_1")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Criação da Tabela Cível Responsabilidade

# COMMAND ----------

# Seleciona as colunas necessárias do DataFrame 'gerencial_ativos_civel'
ativos_civel = gerencial_ativos_civel_final.select('PROCESSO_ID', 'CÍVEL_MASSA_RESPONSABILIDADE')

# Seleciona as colunas necessárias do DataFrame 'gerencial_encerrados_civel'
encerrados_civel = gerencial_encerrados_civel_final.select('PROCESSO_ID', 'CÍVEL_MASSA_RESPONSABILIDADE')

# Une os DataFrames 'ativos_civel' e 'encerrados_civel'
tb_responsabilidade = ativos_civel.union(encerrados_civel)

# Remove registros duplicados com base na coluna 'PROCESSO - ID'
tb_responsabilidade = tb_responsabilidade.dropDuplicates(['PROCESSO_ID'])

# COMMAND ----------

display(tb_responsabilidade)

# COMMAND ----------

from pyspark.sql.functions import col, count

# Usando groupBy e count para obter a contagem de ocorrências de cada valor na coluna 'CÍVEL MASSA - RESPONSABILIDADE'
freq_table = tb_responsabilidade.groupBy('CÍVEL_MASSA_RESPONSABILIDADE').agg(count('*').alias('FREQUENCY'))

# Ordenando a tabela pela coluna 'FREQUENCY' em ordem decrescente
freq_table = freq_table.orderBy(col('FREQUENCY').desc())

# Exibindo a tabela de frequências
freq_table.show()

# COMMAND ----------

tb_responsabilidade.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("databox.juridico_comum.TB_RESPONSABILIDADE_CIVEL")

# COMMAND ----------

df_tb_ger_civel_union = spark.sql("SELECT * FROM tb_ger_civel_")

# COMMAND ----------

#Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/tb_fecham_financ_civel_202409.xlsx# Entradas
nmtabela = dbutils.widgets.get("nmtabela")
table_name = f'df_tb_ger_civel_union_{nmtabela}'

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_tb_ger_civel_union.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/{table_name}.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='ENTRADAS')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_gerenciais/{table_name}.xlsx'

copyfile(local_path, volume_path)
