# Databricks notebook source
# MAGIC %md
# MAGIC # Exportação Bases Analíticas 

# COMMAND ----------

# MAGIC %md
# MAGIC ###1-Base Processos Novos 

# COMMAND ----------

tb_fecham_financ_civel_f = spark.sql("select * from databox.juridico_comum.tb_fecham_financ_civel_202412")

# COMMAND ----------

df_24_civel = spark.sql('''
    select * from databox.juridico_comum.tb_fecham_financ_civel_202412
    WHERE MES_FECH >= '2024-01-01'
''')

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Filtra o DataFrame para criar um novo onde NOVOS é igual a 1
tb_fecham_financ_civel_f_novos_1 = tb_fecham_financ_civel_f.filter(col("NOVOS") == 1)

# Remove as colunas ENCERRADOS e ESTOQUE
tb_fecham_financ_civel_f_novos_2 = tb_fecham_financ_civel_f_novos_1.drop("ENCERRADOS", "ESTOQUE")

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

nmmes = dbutils.widgets.get("NMES")

# Converter PySpark DataFrame para Pandas DataFrame
tb_fecham_financ_civel_f_novos_2_pandas = tb_fecham_financ_civel_f_novos_2.toPandas()
# df_24_civel_pand = df_24_civel.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/tb_fecham_financ_civel_f_novos_{nmmes}_2.xlsx'
tb_fecham_financ_civel_f_novos_2_pandas.to_excel(local_path, index=False, sheet_name='NOVOS_CIVEL')
# df_24_civel_pand.to_excel(local_path, index=False, sheet_name='NOVOS_CIVEL')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_auxiliares/tb_fecham_financ_civel_f_novos_{nmmes}_2.xlsx'

# volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_auxiliares/df_24_civel.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###2- Base Processos Encerrados

# COMMAND ----------

# Filtra o DataFrame para criar um novo onde ENCERRADOS é igual a 1
tb_fecham_financ_civel_f_encerrados_1 = tb_fecham_financ_civel_f.filter(col("ENCERRADOS") == 1)

# Remove as colunas NOVOS e ESTOQUE
tb_fecham_financ_civel_f_encerrados_2 = tb_fecham_financ_civel_f_encerrados_1.drop("NOVOS", "ESTOQUE")

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

nmmes = dbutils.widgets.get("NMES")

# Converter PySpark DataFrame para Pandas DataFrame
tb_fecham_financ_civel_f_encerrados_2_pandas = tb_fecham_financ_civel_f_encerrados_2.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/tb_fecham_financ_civel_f_encerrados_{nmmes}_2.xlsx'
tb_fecham_financ_civel_f_encerrados_2_pandas.to_excel(local_path, index=False, sheet_name='ENCERRADOS_CIVEL')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_auxiliares/tb_fecham_financ_civel_f_encerrados_{nmmes}_2.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3- Base Processos em Estoque

# COMMAND ----------

# Filtra o DataFrame para criar um novo onde ESTOQUE  é igual a 1
tb_fecham_financ_civel_f_estoque_1 = tb_fecham_financ_civel_f.filter((col("ESTOQUE") == 1) & (col("MES_FECH") == "2024-11-01"))
# Remove as colunas  e ENCERRADOS e NOVOS
tb_fecham_financ_civel_f_estoque_2 = tb_fecham_financ_civel_f_estoque_1.drop("NOVOS", "ENCERRADOS")

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

nmmes = dbutils.widgets.get("NMES")

# Converter PySpark DataFrame para Pandas DataFrame
tb_fecham_financ_civel_f_estoque_2_pandas = tb_fecham_financ_civel_f_estoque_2.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/tb_fecham_financ_civel_f_estoque_{nmmes}_2.xlsx'
tb_fecham_financ_civel_f_estoque_2_pandas.to_excel(local_path, index=False, sheet_name='ESTOQUE_CIVEL')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_auxiliares/tb_fecham_financ_civel_f_estoque_{nmmes}_2.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

nmmes = dbutils.widgets.get("NMES")

# Converter PySpark DataFrame para Pandas DataFrame
tb_fecham_financ_civel_f_pandas = tb_fecham_financ_civel_f.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/tb_fecham_financ_civel_f_{nmmes}_2.xlsx'
tb_fecham_financ_civel_f_pandas.to_excel(local_path, index=False, sheet_name='ESTOQUE_CIVEL')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/cível/bases_auxiliares/tb_fecham_financ_civel_f_{nmmes}_2.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###4- Produto Final
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####***O Resultado final das bases deve ser encaminhado na geração dos indicadores de Prévia e Fechamento para o Time de Operações Jurídicas (Dados)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Mensalmente as Bases de Novos, Encerrados e Estoque salvar o históricos dos dados destinado a carteira Cível:
# MAGIC *****R:\01 PLANEJ FINANCEIRO JURIDICO\137.ANALYTICS\6 Bases de Dados MIS\Cível****

# COMMAND ----------

from pyspark.sql.functions import col

# Lista de colunas numéricas que precisam de substituição
numeric_columns = ["ACORDOS", "CONDENAÇÃO", "PENHORA", "GARANTIA","OUTROS_PAGAMENTOS", "TOTAL_PAGAMENTOS",]

# Substituir NaN ou null por 0 nas colunas especificadas
#tb_fechamento_civel_20205 = tb_fechamento_civel_20205.na.fill(0, subset=numeric_columns)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Substituir o texto '#NÚM!' por 0 nas colunas numéricas
for col_name in numeric_columns:
    tb_fechamento_civel_202209 = tb_fechamento_civel_202209.withColumn(
        col_name,
        regexp_replace(col(col_name), "NaN", "0").cast("double")  # Substitui '#NÚM!' por 0 e converte para tipo numérico
    )

# Verificar se há outros NaN ou null e substituir por 0
tb_fechamento_civel_202209 = tb_fechamento_civel_202209.na.fill(0, subset=numeric_columns)




# COMMAND ----------

tb_fechamento_civel_202209 = tb_fechamento_civel_202209.withColumnRenamed("CONDENAÇÃO", "CONDENACAO")
