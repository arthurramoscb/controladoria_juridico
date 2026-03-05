# Databricks notebook source
!pip install openpyxl

# COMMAND ----------

!pip install xlsxwriter

# COMMAND ----------

!pip install unidecode

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType, LongType
from typing import List

# COMMAND ----------

import unidecode

# COMMAND ----------

def read_excel(path: str, start_cell: str = "A1", header: bool = True, infer_schema: bool = True,
               ignore_leading_whitespace: bool = True, treat_empty_values_as_nulls: bool = True,
               ignore_trailing_whitespace: bool = True, max_rows_in_memory: int = 2000) -> DataFrame:
    """
    Reads an Excel file into a PySpark DataFrame.

    Args:
    - path (str): The file path of the Excel file.
    - start_cell (str): The starting cell address of the data in the Excel sheet. Default is "A1".
    - header (bool): Whether the Excel file has a header row. Default is True.
    - infer_schema (bool): Whether to infer the schema from the Excel file. Default is True.
    - ignore_leading_whitespace (bool): Whether to ignore leading whitespace in column names. Default is True.
    - treat_empty_values_as_nulls (bool): Whether to treat empty cells as null values. Default is True.
    - ignore_trailing_whitespace (bool): Whether to ignore trailing whitespace in column names. Default is True.
    - max_rows_in_memory (int): Maximum number of rows to read into memory. Default is 2000.

    Returns:
    - PySpark DataFrame: A DataFrame containing the data from the Excel file.
    """
    df_excel = spark.read.format("com.crealytics.spark.excel") \
        .option("header", header) \
        .option("inferSchema", infer_schema) \
        .option("ignoreLeadingWhiteSpace", ignore_leading_whitespace) \
        .option("treatEmptyValuesAsNulls", treat_empty_values_as_nulls) \
        .option("ignoreTrailingWhiteSpace", ignore_trailing_whitespace) \
        .option("MaxRowsInMemory", max_rows_in_memory) \
        .option("dataAddress", start_cell) \
        .load(path)

    return df_excel

# COMMAND ----------

def find_columns_with_word(df: DataFrame, word: str) -> List:
    """
    Returns a list of column names in the PySpark DataFrame containing the specified word.

    Args:
    - df: PySpark DataFrame
    - word: Substring to search for in column names

    Returns:
    - List of column names containing the specified word
    """
    columns_with_word = [col_name for col_name in df.columns if word in col_name]
    return columns_with_word

# COMMAND ----------

from pyspark.sql.functions import col, to_date, when, length

def convert_to_date_format(df, columns):
    """
    Converts columns in a PySpark DataFrame to date format.

    Args:
    - df: PySpark DataFrame
    - columns: List of column names to convert to date format

    Returns:
    - PySpark DataFrame with specified columns converted to date format
    """
    # Set timeParserPolicy to LEGACY to handle two-digit year conversion
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    for column in columns:
        column_type = dict(df.dtypes)[column]
        try:
            if "timestamp" in column_type:
                df = df.withColumn(column, to_date(col(column), 'yyyy-MM-dd'))
            else:
                df = df.withColumn(column, when(
                                                length(col(column)) == 10,
                                                to_date(col(column), 'dd/MM/yyyy')
                                            ).otherwise(
                                                when(
                                                    length(col(column)) == 8,  # Checks for format with m/dd/yy
                                                    to_date(col(column), 'MM/dd/yy')
                                                ).otherwise(
                                                    when(
                                                    length(col(column)) == 7,  # Checks for format with m/dd/yy
                                                    to_date(col(column), 'M/dd/yy')
                                                ).otherwise(
                                                when(
                                                    length(col(column)) == 6,  # Checks for format with m/dd/yy
                                                    to_date(col(column), 'M/d/yy')
                                                ).otherwise(
                                                    when(
                                                        col(column).like('%/%/%/%'),  # Checks for format with m/dd/yy
                                                        to_date(col(column), 'M/dd/yy')
                                                    ).otherwise(
                                                        to_date(col(column), 'd/M/yy')
                                                    )
                                                )
                                            ))))
        except:
            print(f"Column '{column}' does not exist. Skipping...")
    
    # Revert timeParserPolicy to default (commented out, but should be used if needed)
    # spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    return df

# COMMAND ----------

# Substitui caracteres das colunas especificadas
def replace_characters(df: DataFrame, columns: List[str], char_to_replace: str, replacement_char: str) -> DataFrame:
    """
    Replace characters in specified columns of a Spark DataFrame.

    Args:
    - df (DataFrame): The input Spark DataFrame.
    - columns (List[str]): List of column names where replacement should be performed.
    - char_to_replace (str): The character(s) to replace.
    - replacement_char (str): The character(s) to replace with.

    Returns:
    - DataFrame: The Spark DataFrame with characters replaced in specified columns.
    """
    for column in columns:
        try:
            df = df.withColumn(column, regexp_replace(col(column), char_to_replace, replacement_char))
        except:
            print(f"Column '{column}' does not exist. Skipping...")
    return df

# COMMAND ----------

# Ajuste dos valores percetuais
def convert_to_float(df: DataFrame, columns: List[str]):
    """
    Convert specified columns in a PySpark DataFrame to floating-point numbers.

    Args:
    - df (DataFrame): The input PySpark DataFrame.
    - columns (List[str]): List of column names to convert to float.

    Returns:
    - DataFrame: The PySpark DataFrame with specified columns converted to float.
    """
    for column in columns:
        try:
            df = df.withColumn(column, col(column).cast("float"))
        except:
            print(f"Column '{column}' does not exist. Skipping...")
    return df

# COMMAND ----------

def adjust_column_names(df: DataFrame) -> DataFrame:
    """
    Adjusts column names of a DataFrame by replacing spaces and special characters with underscores
    and converting the column names to upcase.
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: DataFrame with adjusted column names
    """
    adjusted_df = df
    for column in df.columns:
        
        adjusted_column_name = column.replace('(', ' ').replace(')', ' ').replace('-', ' ').replace('.', ' ').replace('_', ' ').replace('/', ' ').replace('|',' ').replace(';',' ').replace('?',' ')
        adjusted_column_name = " ".join(adjusted_column_name.split())
        adjusted_column_name = adjusted_column_name.replace(' ', '_').upper()
        
        adjusted_df = adjusted_df.withColumnRenamed(column, adjusted_column_name)
    return adjusted_df

def adjust_list_names(lista: List[str]) -> List:
    """
    Adjusts column names of a DataFrame by replacing spaces and special characters with underscores
    and converting the column names to upcase.
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: DataFrame with adjusted column names
    """

    adjusted_list = []
    for name in lista:
        
        adjusted_column_name = name.replace('(', ' ').replace(')', ' ').replace('.', ' ').replace('_', ' ').replace('/', ' ').replace('-', ' ')
        adjusted_column_name = " ".join(adjusted_column_name.split())
        adjusted_column_name = adjusted_column_name.replace(' ', '_').upper()
        adjusted_column_name = adjusted_list.append(adjusted_column_name)
    return adjusted_list

# COMMAND ----------

def compress_values(df: DataFrame, columns: list) -> DataFrame:
    """
    Trim and compress multiple columns in a PySpark DataFrame.
    
    Args:
    - df (DataFrame): Input PySpark DataFrame.
    - columns (list): List of column names to be compressed.
    
    Returns:
    - DataFrame: DataFrame with trimmed and compressed values in specified columns.
    """
    for column in columns:
        df = df.withColumn(column, regexp_replace(trim(df[column]), "\\s+", " "))
    return df

# COMMAND ----------

def compress_column_names(df: DataFrame) -> DataFrame:
    """
    Trim and compress the column names of a PySpark DataFrame.
    
    Args:
    - df (DataFrame): Input PySpark DataFrame.
    
    Returns:
    - DataFrame: DataFrame with trimmed and compressed column names.
    """
    # Create a dictionary to rename columns
    new_column_names = {col_name: ' '.join(col_name.split()) for col_name in df.columns}
    
    # Apply the new column names to the DataFrame
    df = df.select([col(old_name).alias(new_name) for old_name, new_name in new_column_names.items()])
    
    return df

# COMMAND ----------

# df = spark.createDataFrame([(1, 2)], ["  col 1  ", " col   2 "])
# df = compress_column_names(df)
# df.show()

# COMMAND ----------

def remove_acentos(df: DataFrame) -> DataFrame:
    """
    Remove acentos das colunas do DataFrane
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returna:
        DataFrame: DataFrame com os nomes de colunas ajustados
    """
    adjusted_df = df
    for column in df.columns:
        
        adjusted_df = adjusted_df.withColumnRenamed(column, unidecode.unidecode(column))

    return adjusted_df

# COMMAND ----------

def deduplica_cols(df: DataFrame) -> DataFrame:
    """
    Remove colunas com mesmo nome do DataFrane
    Mantem a primeira encontrada

    Args:
        df (DataFrame): Input DataFrame
        
    Returna:
        DataFrame: DataFrame com colunas únicas
    """
    columns = df.columns
    renamed_columns = [f"{col}@{i}" for i, col in enumerate(columns)]
    
    # Rename columns to make them unique
    df_renamed = df.toDF(*renamed_columns)
    
    # Identify and keep the first occurrence of each column
    unique_columns = []
    seen = set()
    for i, col in enumerate(columns):
        if col not in seen:
            unique_columns.append(renamed_columns[i])
            seen.add(col)
    
    # Select only the unique columns
    df_unique = df_renamed.select(*unique_columns)
    
    # Rename columns back to original names
    original_names = [col.split('@')[0] for col in unique_columns]
    df_final = df_unique.toDF(*original_names)
    
    return df_final

# COMMAND ----------

def merge_dfs(source: DataFrame, new: DataFrame, join_cols: List[str], how: str='left') -> DataFrame:
    """
    Um substituto ao MERGE do SAS.
    Realiza junção entre dois DataFrames do Spark e aplica coalesce em todas as colunas, 
    exceto nas colunas de junção.

    Parâmetros:
    source (DataFrame): DataFrame de origem.
    new (DataFrame): Novo DataFrame para a junção.
    join_cols (List[str]): Lista de nomes das colunas pelas quais os DataFrames serão unidos.
    how (str): Indica tipo de junção (left | right | inner | outer) [Padrão = left]

    Retorna:
    DataFrame: Resultado da junção e aplicação do coalesce.
    """
    # Generate the join condition dynamically
    join_condition = [source[col_name] == new[col_name] for col_name in join_cols]
    
    # Perform the left join
    joined_df = source.alias("a").join(new.alias("b"), on=join_condition, how=how)
    
    # Apply coalesce to all columns except the joining columns
    coalesce_exprs = [coalesce(col("b." + col_name), col("a." + col_name)).alias(col_name) for col_name in source.columns if col_name not in join_cols]
    
    # Select the required columns
    result_df = joined_df.select(*[col("a." + col_name) for col_name in join_cols], *coalesce_exprs)
    
    return result_df

# COMMAND ----------

def write_excel(df: DataFrame, path: str, sheetName: str = 'Sheet1', useHeader: bool = True, mode: str = 'overwrite',
                maxRowsPerSheet: int = 1048576, maxColumns: int = 16384 ) -> DataFrame:
    """
    Write an Excel file into a path.

    Args:
    - DataFrame: Nome do dataframe que será utilizado para criar o arquivo excel.
    - path (str): Caminho onde o arquivo será salvo.
    - sheetName (str): Nome da planilha do arquivo excel. Padrão: Sheet1
    - useHeader (bool): True indica que utilizará a primeira linha como cabeçalho. Padrão: True
    - mode (str): overwrite: irá substituir o arquivo se existir | append: Caso exista um arquivo irá adicionar as informações | 
       ignore: Caso exista um arquivo não irá fazer nada | error: Caso exista um arquivo irá retornar um erro! Padrão: overwrite
    - maxRowsPerSheet: Especifica o número máximo de linhas por planilha. Se o número de linhas no DataFrame exceder esse limite, ele será dividido em várias planilhas no mesmo arquivo Excel. Padrão: 1048576
    - maxColumns: Define o número máximo de colunas que serão escritas no Excel. Se o DataFrame tiver mais colunas do que esse limite, algumas colunas podem ser truncadas. Padrão: 16384
    
    Returns:
    - Retorna o caminho do arquivo gerado
    """
    df.write.format("com.crealytics.spark.excel") \
        .option("sheetName", sheetName) \
        .option("Header", str(useHeader)) \
        .option("maxRowsPerSheet", maxRowsPerSheet) \
        .option("maxColumns", maxColumns) \
        .mode(mode) \
        .save(path)

    return path
