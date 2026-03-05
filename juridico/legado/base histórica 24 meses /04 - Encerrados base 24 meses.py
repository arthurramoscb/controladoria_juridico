# Databricks notebook source
# Cria widgets para receber variaveis
dbutils.widgets.text("ano_arq", "2024")

dbutils.widgets.text("mes_arq", "06.2024")

dbutils.widgets.text("nm_arq", "20240620")

dbutils.widgets.text("nm_arq_pgto", "20240620")

dbutils.widgets.text("nm_arq_garantia", "20240620")

dbutils.widgets.text("data_corte_24meses", "2024-06-20")


# COMMAND ----------

# MAGIC %run /Workspace/Jurídico/funcao_tratamento_fechamento/common_functions

# COMMAND ----------

# Abaixo obtenção das variaveis definidas em widgets
ano_arq = dbutils.widgets.get("ano_arq")
mes_arq = dbutils.widgets.get("mes_arq")
nm_arq = dbutils.widgets.get("nm_arq")
nm_arq_pgto = dbutils.widgets.get("nm_arq_pgto")
nm_arq_garantia = dbutils.widgets.get("nm_arq_garantia")
data_corte_24meses = dbutils.widgets.get("data_corte_24meses")

# Caminhos de arquivos DexPara
path_terceiro_insolvente = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/DE_PARA_TERCEIRO_INSOLVENTE_V1.xlsx'

path_centro_custro_bu = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/De para Centro de Custo e BU_v2 - ago-2022.xlsx'

path_centro_custo_area_funcional = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/Centro de Custo e Área Funcional_OKENN_280623.xlsx'

df_terceiro_insolvente = read_excel(path_terceiro_insolvente)
df_terceiro_insolvente = adjust_column_names(df_terceiro_insolvente)
df_terceiro_insolvente.createOrReplaceTempView('tb_terceiro_insolvente')

df_centro_custo_bu = read_excel(path_centro_custro_bu)
df_centro_custo_bu = adjust_column_names(df_centro_custo_bu)
df_centro_custo_bu.createOrReplaceTempView('tb_centro_custo_bu')

df_centro_custo_area_funcional = read_excel(path_centro_custo_area_funcional)
df_centro_custo_area_funcional = adjust_column_names(df_centro_custo_area_funcional)
df_centro_custo_area_funcional = df_centro_custo_area_funcional.withColumnRenamed("CENTRO", "CENTRO_DE_CUSTO")
df_centro_custo_area_funcional.createOrReplaceTempView('tb_centro_custo_area_funcional')

# COMMAND ----------

# Cria dataframes de aging (Tempo entre incio e fim)
AgingEstoque_data = [
    (0, 6, 'até 6 meses'),
    (6, 12, '7 - 12 meses'),
    (12, 18, '13 - 18 meses'),
    (18, 24, '19 - 24 meses'),
    (24, 999999, '+24 meses')
]

AgingTrab_data = [
    (0, 360, 'até 1 ano'),
    (361, 1080, '1 - 3 anos'),
    (1081, 2160, '3 - 6 anos'),
    (2161, 3240, '6 - 9 anos'),
    (3241, 999999, '+9 anos')
]

AgingNovoModelo_data = [
    (0, 360, 'até 1 ano'),
    (361, 720, '1 - 2 anos'),
    (721, 1080, '2 - 3 anos'),
    (1081, 1440, '3 - 4 anos'),
    (1441, 1800, '4 - 5 anos'),
    (1801, 2160, '5 - 6 anos'),
    (2161, 2520, '6 - 7 anos'),
    (2521, 999999, '+7 anos')
    ]

AgingTempoEmpresa_data = [
    (0, 360, 'até 1 ano'),
    (361, 720, '1 - 2 anos'),
    (721, 1080, '2 - 3 anos'),
    (1081, 1440, '3 - 4 anos'),
    (1441, 1800, '4 - 5 anos'),
    (1801, 2160, '5 - 6 anos'),
    (2161, 3600, '6 - 10 anos'),
    (3601, 999999, '+10 anos')
]

# Create a DataFrame from the list
AgingEstoque_columns = ['low', 'high', 'AgingEstoque']
AgingTrab_columns = ['low', 'high', 'AgingTrab']
AgingNovoModelo_columns = ['low', 'high', 'AgingNovoModelo']
AgingTempoEmpresa_columns = ['low', 'high', 'AgingTempoEmpresa']

df_AgingEstoque = spark.createDataFrame(AgingEstoque_data, AgingEstoque_columns)
df_AgingTrab = spark.createDataFrame(AgingTrab_data, AgingTrab_columns)
df_AgingNovoModelo = spark.createDataFrame(AgingNovoModelo_data, AgingNovoModelo_columns)
df_AgingTempoEmpresa = spark.createDataFrame(AgingTempoEmpresa_data, AgingTempoEmpresa_columns)

# COMMAND ----------

# Função para unir dataframes
# Lista arquivos excel de fechamento dentro do periodo de 24 meses em relação a data_corte_24meses, filtra as colunas que serão utilizadas, converte tipo de colunas e faz a união dos dataframes

from pyspark.sql.functions import *

df_tabelas = spark.sql('''
    SHOW TABLES IN databox.juridico_comum       
''')

df_fechamento_trab = df_tabelas.filter(df_tabelas["tableName"].like("%tb_fecham_trab_%"))

df_fechamento_trab = df_fechamento_trab.withColumn("DATA", substring(df_fechamento_trab["tableName"], -6, 6))

df_fechamento_trab = df_fechamento_trab.withColumn("DATA", to_date(substring(col("DATA"), 1, 6), "yyyyMM"))

data_corte_24meses = dbutils.widgets.get("data_corte_24meses")

data_24_meses_antes = spark.sql(f"SELECT DATE_SUB(DATE_FORMAT('{data_corte_24meses}', 'yyyy-MM-dd'), 25 * 30)").collect()[0][0]

df_fechamento_trab = df_fechamento_trab.filter((col("DATA") >= data_24_meses_antes) & (col("DATA") <= data_corte_24meses))

df_fechamento_24_meses = None

tables = df_fechamento_trab.select("tableName").collect()

colunas_df_final = []

colunas_manter = ['LINHA', 'ID_PROCESSO', 'PASTA', 'AREA_DO_DIREITO', 'SUB_AREA_DO_DIREITO', 'GRUPO_M_1', 'EMPRESA_M_1', 'GRUPO_M', 'EMPRESA_M', 'STATUS_M_1', 'OBJETO_ASSUNTO_CARGO_M_1', 'SUB_OBJETO_ASSUNTO_CARGO_M_1', 'OBJETO_ASSUNTO_CARGO_M', 'SUB_OBJETO_ASSUNTO_CARGO_M', 'ORGAO_OFENSOR_FLUXO_M_1', 'ORGAO_OFENSOR_FLUXO_M', 'NATUREZA_OPERACIONAL_M_1', 'NATUREZA_OPERACIONAL_M', '%_RISCO', 'No_PROCESSO', 'ESCRITORIO', 'VLR_CAUSA', '%_SOCIO_M_1', '%_EMPRESA_M_1', '%_SOCIO_M', '%_EMPRESA_M', 'PROVISAO_M_1', 'CORRECAO_M_1', 'PROVISAO_TOTAL_M_1', 'CLASSIFICACAO_MOV_M', 'PROVISAO_MOV_M', 'CORRECAO_MOV_M', 'PROVISAO_MOV_TOTAL_M', 'PROVISAO_TOTAL_M', 'CORRECAO_M', 'PROVISAO_TOTAL_PASSIVO_M', 'SOCIO:_PROVISAO_M_1', 'SOCIO:_CORRECAO_M_1', 'SOCIO:_PROVISAO_TOTAL_M_1', 'SOCIO:_CLASSIFICACAO_MOV_M', 'SOCIO:_PROVISAO_MOV_M', 'SOCIO:_CORRECAO_MOV_M', 'SOCIO:_PROVISAO_MOV_TOTAL_M', 'SOCIO:_PROVISAO_TOTAL_M', 'SOCIO:_CORRECAO_M_1_0001', 'SOCIO:_CORRECAO_M', 'SOCIO:_PROV_TOTAL_PASSIVO_M', 'EMPRESA:_PROVISAO_M_1', 'EMPRESA:_CORRECAO_M_1', 'EMPRESA:_PROVISAO_TOTAL_M_1', 'EMPRESA:_CLASSIFICACAO_MOV_M', 'EMPRESA:_PROVISAO_MOV_M', 'EMPRESA:_CORRECAO_MOV_M', 'EMPRESA:_PROVISAO_MOV_TOTAL_M', 'EMPRESA:_PROVISAO_TOTAL_M', 'EMPRESA:_CORRECAO_M_1_0001', 'EMPRESA:_CORRECAO_M', 'EMPRESA:_PROV_TOTAL_PASSIVO_M', 'DEMITIDO_POR_REESTRUTURACAO', 'INDICACAO_PROCESSO_ESTRATEGICO', 'NOVOS', 'REATIVADOS', 'ENCERRADOS', 'ESTOQUE', 'SUB_TIPO', 'TIPO_PGTO', 'PARCELAMENTO_CONDENACAO', 'PARCELAMENTO_ACORDO', 'SUM_OF_VALOR', 'OUTRAS_ADICOES', 'OUTRAS_REVERSOES', 'DOC', 'VALOR_INSS_DECAIDO', 'ESTRATEGIA_M_1', 'ELEGIVEL_M_1', 'FASE_M_1', 'TIPO_DE_CALCULO_M_1', 'ELEGIVEL_M', 'TIPO_DE_CALCULO_M', 'MOTIVO_MOVIMENTACAO', 'MOTIVO_PAGAMENTO', 'REABERTURA', 'DISTRIBUICAO', 'MES_FECH', 'MEDIA_DE_PAGAMENTO', 'CENTRO_DE_CUSTO_M_1', 'CENTRO_DE_CUSTO_M', 'DATACADASTRO', 'STATUS_M', 'CARTEIRA', 'ESTRATEGIA', 'DP_FASE', 'PROCESSO_COM_DOCUMENTO', 'DT_ULT_PGTO', 'ACORDOS', 'CONDENACAO', 'PENHORA', 'GARANTIA', 'IMPOSTO', 'OUTROS_PAGAMENTOS', 'TOTAL_PAGAMENTOS', 'FASE_M', 'OBSERVACOES', 'CLUSTER_AGING_TEMPO_DE_EMPRESA', 'CLUSTER_AGING', 'SAFRA_DE_RECLAMACAO', 'CLUSTER_VALOR', 'CARGO_TRATADO', 'CLUSTER_AGING_TEMPO_DE_EMPRESA_1', 'CLUSTER_AGING_1', 'SAFRA_DE_RECLAMACAO_1', 'CLUSTER_VALOR_1', 'CARGO_TRATADO_1', 'CLUSTER_AGING_TEMPO_DE_EMPRESA_2', 'CLUSTER_AGING_2', 'SAFRA_DE_RECLAMACAO_2', 'CLUSTER_VALOR_2', 'CARGO_TRATADO_2', 'CLASSIFICACAO_POR_POSICAO', 'TERCEIRO','CONDENAÇÃO']

for x, table in enumerate(tables):
    table_name = table["tableName"]
    
    # Ler a Delta table
    df_delta = spark.read.format("delta").table(f'databox.juridico_comum.{table_name}')
    df_delta_columns = df_delta.columns

    for columns in colunas_manter:
        if columns not in df_delta_columns:
             df_delta = df_delta.withColumn(columns, lit(0))

    df_delta = df_delta.select(*[col(col_name) for col_name in colunas_manter])

    df_delta = df_delta.withColumn("ENCERRADOS", col("ENCERRADOS").cast("int"))
    df_delta = df_delta.withColumn("ID_PROCESSO", col("ID_PROCESSO").cast("int"))
    df_delta = convert_to_float(df_delta,
        ['VLR_CAUSA','PROVISAO_TOTAL_M_1','SOCIO:_PROVISAO_TOTAL_M_1','EMPRESA:_PROVISAO_TOTAL_M_1','EMPRESA:_PROV_TOTAL_PASSIVO_M','SUM_OF_VALOR','OUTRAS_REVERSOES','CONDENACAO','TOTAL_PAGAMENTOS','OUTRAS_ADICOES','VALOR_INSS_DECAIDO','GARANTIA','CLUSTER_AGING_TEMPO_DE_EMPRESA_2','CLUSTER_AGING_2','SAFRA_DE_RECLAMACAO_2','CLUSTER_VALOR_2'])
    df_delta = convert_to_date_format(df_delta,
        ['DISTRIBUICAO','REABERTURA','DT_ULT_PGTO'])

    df_delta = df_delta.filter(col("ENCERRADOS") == 1)

    df_columns = df_delta.columns

    if '%_RISCO' not in df_columns:
        df_delta = df_delta.withColumn("%_RISCO", lit(0))

    if 'MEDIA_DE_PAGAMENTO' not in df_columns:
        df_delta = df_delta.withColumn("MEDIA_DE_PAGAMENTO", lit(0))

    if 'CONDENACAO' not in df_columns:
        df_delta = df_delta.withColumn("CONDENACAO", lit(0))

    df_columns = df_delta.columns

    for c in df_columns:
        column_type = dict(df_delta.dtypes)[c]

        if c not in colunas_df_final:
            colunas_df_final.append(c)
    
    for cf in colunas_df_final:
        if cf not in df_columns:
            df_delta = df_delta.withColumn(cf, lit(''))

    try:
        if df_fechamento_24_meses is None:
            df_fechamento_24_meses = df_delta
        else:
            df_fechamento_columns = df_fechamento_24_meses.columns

            for c in df_fechamento_columns:
                if c not in colunas_df_final:
                    colunas_df_final.append(c)
        
            for cf in colunas_df_final:
                if cf not in df_fechamento_columns:
                    df_fechamento_24_meses = df_fechamento_24_meses.withColumn(cf, lit(''))

            df_fechamento_24_meses = df_fechamento_24_meses.withColumn("ENCERRADOS", col("ENCERRADOS").cast("int"))
            df_fechamento_24_meses = df_fechamento_24_meses.withColumn("ID_PROCESSO", col("ID_PROCESSO").cast("int"))
            df_fechamento_24_meses = convert_to_float(df_fechamento_24_meses,
                ['VLR_CAUSA','PROVISAO_TOTAL_M_1','SOCIO:_PROVISAO_TOTAL_M_1','EMPRESA:_PROVISAO_TOTAL_M_1','EMPRESA:_PROV_TOTAL_PASSIVO_M','SUM_OF_VALOR','OUTRAS_REVERSOES','CONDENACAO','TOTAL_PAGAMENTOS','OUTRAS_ADICOES','VALOR_INSS_DECAIDO'])
            df_fechamento_24_meses = convert_to_date_format(df_fechamento_24_meses,
                ['DISTRIBUICAO','REABERTURA','DT_ULT_PGTO'])

            df_fechamento_columns = df_fechamento_24_meses.columns
            df_delta_columns = df_delta.columns

            df_fechamento_24_meses = df_fechamento_24_meses.union(df_delta)

    except Exception as erro:
        print(f'{x}|Erro: {erro}')
        print('-' * 50)
        print(f'{df_fechamento_24_meses.dtypes} \n VS \n {df_delta.dtypes}')

df_fechamento_24_meses.createOrReplaceTempView('tb_fechamento_24_meses')
# display(df_fechamento_24_meses)

# COMMAND ----------

# Cria dataframe adicionando informações de BU, VP, DIRETORIA, RESPONSAVEL_AREA e AREA_FUNCIONAL com base no join entre a tabela de fechamento unificada e as bases auxiliares de centro de custo
df_fechamento_24_meses_1 = spark.sql("""
    SELECT A.*
    ,CASE 
        WHEN A.EMPRESA_M_1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA' ELSE B.BU
    END AS BU
    ,CASE
        WHEN A.EMPRESA_M_1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA' ELSE C.NOME2
    END AS VP
    ,CASE 
        WHEN A.EMPRESA_M_1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA' ELSE C.NOME3
    END AS DIRETORIA
    ,CASE
        WHEN A.EMPRESA_M_1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA' ELSE C.NOME_RESPONSAVEL
    END AS RESPONSAVEL_AREA
    ,CASE
        WHEN A.EMPRESA_M_1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BART' ELSE C.AREA_FUNCIONAL
    END AS AREA_FUNCIONAL

    FROM tb_fechamento_24_meses A 
    LEFT JOIN tb_centro_custo_bu B 
    ON A.CENTRO_DE_CUSTO_M_1 = B.CENTRO_DE_CUSTO
    LEFT JOIN tb_centro_custo_area_funcional C
    ON A.CENTRO_DE_CUSTO_M_1 = C.CENTRO_DE_CUSTO
    WHERE A.ID_PROCESSO IS NOT NULL
""")

df_fechamento_24_meses_1.createOrReplaceTempView('tb_fechamento_24_meses_1')

# COMMAND ----------

df_fechamento_24_meses_3 = spark.sql("""
    SELECT A.*
    ,DATE_FORMAT(DATE_TRUNC('MM', B.DATA_REGISTRADO), 'ddMMyyyy') AS MES_CADASTRO
    ,B.ADVOGADO_RESPONSAVEL
    ,B.ESCRITORIO AS ESCRITORIO_RESPONSAVEL
    ,B.NUMERO_DO_PROCESSO
    ,B.PARTE_CONTRARIA_CPF
    ,B.PARTE_CONTRARIA_NOME
    ,B.ADVOGADO_DA_PARTE_CONTRARIA
    ,B.MATRICULA
    ,B.PROCESSO_ESTADO AS ESTADO
    ,B.PROCESSO_COMARCA AS COMARCA
    ,B.CLASSIFICACAO
    ,B.NATUREZA_OPERACIONAL
    ,B.TERCEIRO_PRINCIPAL
    ,B.NOVO_TERCEIRO
    ,B.PARTE_CONTRARIA_DATA_ADMISSAO AS DATA_ADMISSAO
    ,B.PARTE_CONTRARIA_DATA_DISPENSA AS DATA_DISPENSA
    ,B.PARTE_CONTRARIA_CARGO_CARGO_GRUPO AS PARTE_CONTRARIA_CARGO_GRUPO
    ,B.PARTE_CONTRARIA_CARGO AS CARGO
    ,B.PARTE_CONTRARIA_MOTIVO_DO_DESLIGAMENTO AS MOTIVO_DESLIGAMENTO
    ,B.FASE AS FASE_ATUAL
    ,B.MOTIVO_DE_ENCERRAMENTO AS MOTIVO_ENCERRAMENTO 
    ,B.FILIAL
    ,B.BANDEIRA
    ,B.CENTRO_DE_CUSTO_AREA_DEMANDANTE_NOME AS NOME_DA_LOJA
    ,B.DATA_REGISTRADO AS DATA_REGISTRADO
    ,(CASE WHEN A.NOVOS IS NULL THEN A.REATIVADOS ELSE A.NOVOS END) AS NOVOS_AJUSTADO

    
    FROM tb_fechamento_24_meses_1 A
    LEFT JOIN databox.juridico_comum.trab_ger_24m_20240621 B
    ON A.ID_PROCESSO = B.PROCESSO_ID                               
""")

df_fechamento_24_meses_3.createOrReplaceTempView('tb_fechamento_24_meses_3')

# COMMAND ----------

# Tratamento do campo cargo
import pyspark.sql.functions as F

df_fechamento_24_meses_3 = df_fechamento_24_meses_3.withColumn(
    "PARTE_CONTRARIA_CARGO_GRUPO_",
    F.when(
        F.instr(F.col("PARTE_CONTRARIA_CARGO_GRUPO"), " PARA") > 0,
        F.expr("substring(PARTE_CONTRARIA_CARGO_GRUPO, 1, length(PARTE_CONTRARIA_CARGO_GRUPO) - 5)")
    ).otherwise(F.col("PARTE_CONTRARIA_CARGO_GRUPO"))
)

df_fechamento_24_meses_3 = df_fechamento_24_meses_3.drop("PARTE_CONTRARIA_CARGO_GRUPO", "NOVOS")

df_fechamento_24_meses_3 = df_fechamento_24_meses_3.withColumnRenamed("PARTE_CONTRARIA_CARGO_GRUPO_", "PARTE_CONTRARIA_CARGO_GRUPO")
df_fechamento_24_meses_3 = df_fechamento_24_meses_3.withColumnRenamed("NOVOS_AJUSTADO", "NOVOS")

df_fechamento_24_meses_3.createOrReplaceTempView('tb_fechamento_24_meses_4')

# COMMAND ----------

df_fechamento_24_meses_44 = spark.sql("""
    SELECT *
    ,(CASE WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR DE VENDAS II') THEN 'VENDEDOR II'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'VENDEDOR INTERNO II') THEN 'VENDEDOR II'

        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR TECNICO') THEN 'ASSESSOR PROD TECNOLOGIA'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR PROD TECNOLOGIA') THEN 'ASSESSOR PROD TECNOLOGIA'

        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ATENDENTE LOJA') THEN 'ASSESSOR DE ATENDIMENTO'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSISTENTE DE LOJAS MOBILE') THEN 'ASSESSOR DE ATENDIMENTO'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'AUX ESTOQUE') THEN 'ASSESSOR DE ATENDIMENTO'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'CAIXA') THEN 'ASSESSOR DE ATENDIMENTO'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR DE ATENDIMENTO') THEN 'ASSESSOR DE ATENDIMENTO'

        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'COORD ADM LOJA') THEN 'CONS ADM LOJA'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'COORD ADMINISTRATIVO LOJA') THEN 'CONS ADM LOJA'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'COORDENADOR ADM LOJA I') THEN 'CONS ADM LOJA'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'COORDENADOR ADM LOJA II') THEN 'CONS ADM LOJA'

        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'VENDEDOR MOBILE') THEN 'VENDEDOR'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'VENDEDOR MOVEIS PLANEJADOS') THEN 'VENDEDOR'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'VENDEDOR INTERNO') THEN 'VENDEDOR'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'CONS VENDAS') THEN 'VENDEDOR'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR DE VENDAS') THEN 'VENDEDOR'

        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'AUX ESTOQUE 130') THEN 'BALCÃO 130'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'CAIXA 130') THEN 'BALCÃO 130'
        WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSISTENTE DE LOJAS MOBILE130') THEN 'BALCÃO 130'

            ELSE PARTE_CONTRARIA_CARGO_GRUPO END) AS PARTE_CONTRARIA_CARGO_GRUPO_
FROM tb_fechamento_24_meses_4                               
""")

df_fechamento_24_meses_44.createOrReplaceTempView('df_fechamento_24_meses_44')

# COMMAND ----------

# /* TRATA O CAMPO CARGO - PARTE 2 */
df_fechamento_24_meses_5 = df_fechamento_24_meses_44.withColumn(
    "CARGO_AJUSTADO",
    F.when(
        F.col("NATUREZA_OPERACIONAL") == 'TERCEIRO INSOLVENTE', 'TERCEIRO INSOLVENTE'
    ).when(
        (F.col("NATUREZA_OPERACIONAL") != 'TERCEIRO INSOLVENTE') & 
        (F.col("PARTE_CONTRARIA_CARGO_GRUPO_").isin(
            'AJUDANTE', 'AJUDANTE EXTERNO', 'ANALISTA', 'AUXILIAR', 
            'ASSESSOR DE ATENDIMENTO', 'GERENTE', 'MONTADOR', 
            'MOTORISTA', 'OPERADOR', 'VENDEDOR'
        )), F.col("PARTE_CONTRARIA_CARGO_GRUPO_")
    ).otherwise('OUTROS')
)

df_fechamento_24_meses_44 = df_fechamento_24_meses_44.drop("PARTE_CONTRARIA_CARGO_GRUPO")
df_fechamento_24_meses_44 = df_fechamento_24_meses_44.withColumnRenamed("PARTE_CONTRARIA_CARGO_GRUPO_", "PARTE_CONTRARIA_CARGO_GRUPO")

df_fechamento_24_meses_5.createOrReplaceTempView('df_fechamento_24_meses_5')

# COMMAND ----------

# /* FAZ O "DE PARA" DO CAMPO FASE */
from pyspark.sql.functions import *

df_fechamento_24_meses_6 = df_fechamento_24_meses_5.withColumn(
    "FASE_M",
    when(
        (instr(col("PARTE_CONTRARIA_NOME"), 'MINISTERIO') > 0) | 
        (instr(col("PARTE_CONTRARIA_NOME"), 'MINISTÉRIO') > 0),
        "N/A"
    ).otherwise(col("FASE_M"))
)

df_fechamento_24_meses_6 = df_fechamento_24_meses_6.drop("FASE")
df_fechamento_24_meses_6 = df_fechamento_24_meses_6.withColumnRenamed("FASE_M", "FASE")

df_fechamento_24_meses_6.createOrReplaceTempView('df_fechamento_24_meses_6')

# COMMAND ----------

# /* PERIODO RECLAMADO */
from pyspark.sql.functions import col, expr, when, concat_ws, year, month

# Calculate PERIODO_RECLAMADO
df_fechamento_24_meses_7 = df_fechamento_24_meses_6.withColumn(
    "PERIODO_RECLAMADO", expr("DISTRIBUICAO - INTERVAL 1800 DAYS")
)

# Calculate PERIODO1
df_fechamento_24_meses_7 = df_fechamento_24_meses_7.withColumn(
    "PERIODO1",
    when(col("PERIODO_RECLAMADO") >= col("DATA_ADMISSAO"), col("PERIODO_RECLAMADO"))
    .otherwise(col("DATA_ADMISSAO"))
)

# Calculate PERIODO2
df_fechamento_24_meses_7 = df_fechamento_24_meses_7.withColumn(
    "PERIODO2",
    when(col("DATA_DISPENSA").isNull(), col("DISTRIBUICAO"))
    .otherwise(col("DATA_DISPENSA"))
)

# Create PER_1 and PER_2
df_fechamento_24_meses_7 = df_fechamento_24_meses_7.withColumn(
    "PER_1", concat_ws("-", month(col("PERIODO1")), year(col("PERIODO1")))
).withColumn(
    "PER_2", concat_ws("-", month(col("PERIODO2")), year(col("PERIODO2")))
)

# Calculate PERIODO_RECL_GRUPO
df_fechamento_24_meses_7 = df_fechamento_24_meses_7.withColumn(
    "PERIODO_RECL_GRUPO",
    when(
        col("PERIODO1").isNotNull() & col("PERIODO2").isNotNull(),
        concat_ws(" a ", col("PER_1"), col("PER_2"))
    )
)

# Drop PER_1 and PER_2
df_fechamento_24_meses_7 = df_fechamento_24_meses_7.drop("PER_1", "PER_2")

df_fechamento_24_meses_7.createOrReplaceTempView('df_fechamento_24_meses_7')

# COMMAND ----------

df_fechamento_24_meses_77 = spark.sql("""
    SELECT 
    A.*
    ,B.DATA_EFETIVA_PAGAMENTO AS DT_ULT_PGTO
                ,B.ACORDO
                ,B.CONDENACAO
                ,B.PENHORA
                ,B.OUTROS_PAGAMENTOS
                ,B.IMPOSTO
                ,B.GARANTIA
    FROM df_fechamento_24_meses_7 A
    LEFT JOIN databox.juridico_comum.trab_pagamentos_24m_20240624 B
    ON A.ID_PROCESSO = B.PROCESSO_ID

""")

