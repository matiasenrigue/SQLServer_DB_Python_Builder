import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd
import re
import numpy as np

from Utils.params import *

from collections import namedtuple




def generate_creation_date() -> str:
    """
    Generates the current date and time in a specific format.

    Returns:
        str: The formatted current date and time.
    """
    
    current_date = datetime.now()
    formatted_date = current_date.strftime('%Y-%m-%d %I:%M:%S %p')
    return formatted_date






def get_pks(pks_series: str) -> str:
    """
    Cleans the PKs column from the Excel file, removing extra spaces and commas.
    Returns the PKs properly formatted for our queries
    """
            
    pks_series = pks_series.strip()
    pks_series = " ".join(pks_series.split())
    pks_series = pks_series.replace(" ", ",") 
    pks_series = re.sub(r',+', ',', pks_series)
        
    if pks_series:
        if "," in pks_series:
            pks = [pk.strip() for pk in pks_series.split(',')]
            pks_concat = ' || '.join(pks)
        
        else:
            pks_concat = pks_series.strip()
        
    return pks_concat
    
    
    

def clean_column(df: pd.DataFrame, column_name: str, replace_comma: bool=False):
    """
    Cleans a column in the DataFrame, ensuring all values are strings,
    removing white spaces, handling null values, and optionally replacing commas.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        column_name (str): The name of the column to clean.
        replace_comma (bool): If True, replaces commas in the values with empty strings.

    Returns:
        pd.Series: The cleaned column.
    """
    if df[column_name].dtype != 'object':
        df[column_name] = df[column_name].astype(str)
    
    df[column_name] = df[column_name].str.strip()
    
    if replace_comma:
        df[column_name] = df[column_name].str.replace(",", "", regex=False)
    
    df[column_name] = df[column_name].replace(['', pd.NA], None)
    
    return df[column_name]




def clean_columns(df: pd.DataFrame, replace_comma_columns=None) -> pd.DataFrame:
    """
    Cleans all columns of a DataFrame, ensuring that:
    - Values are converted to strings.
    - White spaces are removed.
    - 'None', 'nan', and values considered null by Python are replaced with pd.NA.
    - Optionally replaces commas in specific columns.

    Args:
        df (pd.DataFrame): DataFrame to clean.
        replace_comma_columns (list): List of columns where commas should be replaced.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    clean_df = df.copy()
    
    if replace_comma_columns is None:
        replace_comma_columns = []

    for column in clean_df.columns:
        replace_comma = column in replace_comma_columns # True or False
        clean_df[column] = clean_column(clean_df, column, replace_comma)
    
    return clean_df




def load_table_info_df() -> pd.DataFrame:
    """
    Loads the table information from the Excel file and cleans the columns.
    """
    table_info_df = pd.read_excel('data/Info_Pks.xlsx')
    df_clean = clean_columns(table_info_df)
    return df_clean
    



def obtain_table_info(nombre_tabla: str, df_tipo_tabla: pd.DataFrame) -> namedtuple:
    
    """
    Retrieves the type of table ("DIM" or "FACT") based on a reference table file.

    This function compares the name of a table with a list of tables and their types 
    from an Excel file and determines if the table is of type "DIM" or "FACT".
    
    Args:
        nombre_tabla (str): The name (of STG!!) of the table you want to verify.
        df_tipo_tabla (pd.DataFrame): DataFrame that contains the names of the tables and their associated type, from an Excel file.

    Returns:
        namedtuple: A namedtuple with the following attributes:
            - tabla_origen (str): Name of the source table.
            - tipo_tabla (str): Type of the table ("DIM" or "FACT").
            - incremental_ORACLE_a_STG (str): Incremental from Oracle to STG.
            - incremental_STG_a_ODS (str): Incremental from STG to ODS.
            - pks (str): Primary keys associated with the table.
    
    Note:
        Ensure that you have imported the Excel file previously with the following line:
        df_tipo_tabla = pd.read_excel('----/Info_Pks.xlsx')
        
        Important: Avoid placing the import inside a loop.
    """
    
    InfoTabla = namedtuple('InfoTabla', ['tabla_origen', 'tipo_tabla', 'incremental_ORACLE_a_STG', 'incremental_STG_a_ODS', 'pks'])
    
    nombre_tabla_estandarizado = process_table_name_short(nombre_tabla)
    
    # Recorrer el dataframe y encontrar coincidencias con la tabla actual
    for index, row in df_tipo_tabla.iterrows():
        tabla_origen = row['TABLAS ORIGEN']
        tipo_tabla_ = str(row['TIPO TABLA'])
        incremental_ORACLE_a_STG_ = str(row['INCREMENTAL ORACLE STG'])
        incremental_STG_a_ODS_ = str(row['INCREMENTAL STG ODS'])
        row_pks = str(row['PK'])
        
        tabla_origen_estandarizado = process_table_name_short(tabla_origen)
        
        # Si los nombres coinciden, devolvemos el tipo de tabla
        if nombre_tabla_estandarizado == tabla_origen_estandarizado:
            
            if "FACT" in tipo_tabla_.upper():
                tipo_tabla =  "FACT"
                print(f'+1 Fact ({nombre_tabla_estandarizado}) - {tipo_tabla_}')
            
            else:
                tipo_tabla = "DIM"
            
            incremental_ORACLE_a_STG = incremental_ORACLE_a_STG_
            incremental_STG_a_ODS = incremental_STG_a_ODS_
            pks = get_pks(row_pks)
   
            return InfoTabla(nombre_tabla_estandarizado, tipo_tabla, incremental_ORACLE_a_STG, incremental_STG_a_ODS, pks)
    
    # Si no se encuentra ninguna coincidencia, devolver "DIM" por defecto
    return InfoTabla(nombre_tabla_estandarizado, "DIM", "", "", "")




    
def process_table_name_short(table_name):
    """
    OJO!! Comentario importante!!
    
    Usada para objetos SQL Server, por problemas de la anterior
    
    Conflictive table names: 
        - Table names que al quitarles el esquema 2 tablas se llaman igual.
        - Solución: Dejarles el esquema
    """
    
    list_conflictive_names = [
        f'{UNIVERISTY_NAME}.IDIOMAS', 'MDL_GOLDENRECORD.IDIOMAS', # Common name: Idiomas 
        f'{UNIVERISTY_NAME}.PERSONAS_{UNIVERISTY_NAME}', f'MDL_GR_TRANSVERSAL.PERSONAS_{UNIVERISTY_NAME}', # Common name: Personas_{UNIVERISTY_NAME}   E
        f'{UNIVERISTY_NAME}.PERSONAS', 'APL_SOLICITUDES.PERSONAS' # Common name: Personas  
    ]
    
    if table_name in list_conflictive_names:
        return table_name.replace(".", "_").strip()
    
    if "." in table_name:
        return table_name.split('.')[-1].strip()
    else:
        return table_name.strip()
    
    


def get_STG_table_name(table_name:str, abreviatura_origen:str) -> str:
    """
    Formats the STG table name using the origin abbreviation.
    """
    
    Data_base_name = process_table_name_short(table_name)
    nuevo_name_tabla = f"[stg].[{abreviatura_origen}_{Data_base_name}]"
    
    return nuevo_name_tabla



def get_ODS_table_name(table_name:str, stg_name:str, abreviatura_origen:str, df_tipo_tabla: pd.DataFrame) -> str:
    """
    Get the correct name for the ODS tables
    
    Args:
        table_name (str): ODS table name according to the excel data dict
        stg_name (str): STG table name, for comparing with the information of the pks / dim dict files   
        abreviatura_origen (str): first 3 letters of the origin name
        df_tipo_tabla (dataframe): df of the tables types (DIM / FACT) & pks

    Returns:
        str: ODS table name
    """
       
    Data_base_name = process_table_name_short(table_name)
    info_tabla = obtain_table_info(stg_name, df_tipo_tabla)
    tipo_tabla = info_tabla.tipo_tabla
    nuevo_name_tabla = f"[ods].[{abreviatura_origen}_{tipo_tabla}_{Data_base_name}]"
    
    return nuevo_name_tabla  





    

def convert_oracle_to_ssis_data_type(oracle_data_type: str) -> str:
    """
    Converts Oracle data types to SSIS-compatible data types.
    
    Args:
        oracle_data_type (str): The Oracle data type.

    Returns:
        str: The corresponding SSIS data type.
    """
    
    db_prefix = "DB_TYPE_"
    
    conversion_dict = {
        f'{db_prefix}VARCHAR2': 'wstr',   # Wide string
        f'{db_prefix}NVARCHAR2': 'wstr',  # Wide string
        f'{db_prefix}VARCHAR': 'wstr',   # Wide string
        f'{db_prefix}NVARCHAR': 'wstr',  # Wide string
        F'{db_prefix}CHAR': 'wstr',       # Wide string
        f'{db_prefix}NCHAR': 'wstr',      # Wide string
        f'{db_prefix}NUMBER': 'numeric',  # Numeric
        f'{db_prefix}FLOAT': 'float',     # Floating point
        f'{db_prefix}DATE': 'dbTimeStamp',# Date and time
        f'{db_prefix}TIMESTAMP': 'dbTimeStamp',  # Date and time
        f'{db_prefix}RAW': 'binary',      # Binary data
        f'{db_prefix}LONG': 'long',       # Long string
        f'{db_prefix}CLOB': 'nText',       # Large text
        f'{db_prefix}NCLOB': 'nText',      # Large text
        f'{db_prefix}BLOB': 'binary',     # Binary data
        f'{db_prefix}BFILE': 'file',      # File reference
        f'{db_prefix}XMLTYPE': 'xml',     # XML data
        f'{db_prefix}BOOLEAN': 'bool',    # Boolean
        f'{db_prefix}INT': 'i4',          # Integer
        f'{db_prefix}INTEGER': 'i4',      # Integer
        f'{db_prefix}DECIMAL': 'numeric', # Numeric
        f'{db_prefix}DOUBLE PRECISION': 'float' # Floating point
    }
    
    return conversion_dict.get(oracle_data_type.upper(), 'unknown')  





def add_length_or_precision(attributes: dict, data_type: str, length:str, precision:str, scale:str, cached_names:bool = False)-> dict:
    
    """
    There are errors in the data dictionary, to be fixed:
        - For Numeric: precision below 0 --> convert to 2.
        - For Numeric: scale below 0, convert to 0 (if added).
        
    Args:
        cached_names (bool): In DFT destination, the input columns have another name with 'cached' prefix and a capital letter.
    """
    
    if cached_names: # Para el caso de las input columns DFT
        precision_name = "cachedPrecision"
        scale_name = "cachedScale"
        lenght_name = "cachedLength"
        data_type_name = "cachedDataType"
    
    else:
        precision_name = "precision"
        scale_name = "scale"
        lenght_name = "length" 
        data_type_name = "dataType"       
    
    
    
    if data_type == 'numeric':
        
        if int(length) == 127: # Caso datos que viene mal de Oracle --> Por algún motivo hay un bug que todos los que están mal tienen esta lenght
            attributes[data_type_name] = 'wstr' # Cambiar el Data Type a nvarchar (38)
            attributes[lenght_name] = "38"
        
        
        else: # Caso de los que vienen 'Okay' de Oracle
            
            if pd.notna(precision):
                if int(precision) > 0:
                    attributes[precision_name] = str(precision)
                else: 
                    attributes[precision_name] = "2"
            
            if pd.notna(scale):
                if int(scale) >= 0:
                    attributes[scale_name] = str(scale)
                else: 
                    attributes[scale_name] = "0"
            
        
            
    elif data_type in ['wstr', 'bytes']:
        if pd.notna(length):
            if int(length) > 0:
                attributes[lenght_name] = str(length)
            else: 
                attributes[lenght_name] = "1"
                
    return attributes





