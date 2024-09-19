import pandas as pd
from Utils.Utils import get_ODS_table_name, clean_columns, load_table_info_df
from Utils.params import *

"""
Python Script to generate SQL Server code to create tables in the ODS database.
"""


def clean_df_ODS(df):
    
    columnas_stg = [STG_CAMPOS, STG_TABLAS, STG_TIPO, STG_SIZE, STG_PRECISION, STG_SCALE]
    columnas_ods = [ODS_CAMPOS, ODS_TABLAS, ODS_TIPO, ODS_SIZE, ODS_PRECISION, ODS_SCALE]
    
    # Limpiar columnas, especificando que STG/ODS_CAMPOS debe reemplazar comas
    df = clean_columns(df, replace_comma_columns=[STG_CAMPOS])
    df = clean_columns(df, replace_comma_columns=[ODS_CAMPOS])

    # Eliminar duplicados en las columnas STG_CAMPOS y STG_TABLAS
    df = df.drop_duplicates(subset=[STG_CAMPOS, STG_TABLAS])

    # Lógica: Si la columna ODS_TABLAS está vacía, se llena con el valor de STG_TABLAS
    df[ODS_TABLAS] = df[ODS_TABLAS].fillna(df[STG_TABLAS]) 
   
    return df




def format_with_precision_and_scale(tipo, precision, scale, default_precision=2, default_scale=0):
    # Validar y asignar valores por defecto si es necesario
    if pd.notna(precision) and pd.notna(scale):
        if int(precision) <= 0:
            precision = default_precision  # Valor predeterminado
        if int(scale) < 0:
            scale = default_scale  # Valor predeterminado
        
        return f"{tipo}({int(precision)}, {int(scale)})"
    else:
        return tipo




# Function to create SQL data type string
def create_data_type(row):
    tipo = row[ODS_TIPO] if pd.notna(row[ODS_TIPO]) else row[STG_TIPO]
    size = row[ODS_SIZE] if pd.notna(row[ODS_SIZE]) else row[STG_SIZE]
    precision = row[ODS_PRECISION] if pd.notna(row[ODS_PRECISION]) else row[STG_PRECISION]
    scale = row[ODS_SCALE] if pd.notna(row[ODS_SCALE]) else row[STG_SCALE]    
    
    def is_convertible_to_int(value):
        try:
            return int(float(value))  # Convierte primero a float y luego a int
        except (ValueError, TypeError):
            return None  # Si no es convertible, devuelve None
        
    
    size = is_convertible_to_int(size)
    precision = is_convertible_to_int(precision)
    scale = is_convertible_to_int(scale)
    
    if tipo == 'DB_TYPE_INT':
        return "int"
    
    elif tipo in ['DB_TYPE_DATE', 'DB_TYPE_TIMESTAMP', 'DB_TYPE_DATETIME']:
        return "datetime"
    
    elif tipo == 'DB_TYPE_CLOB':
        return "nvarchar(max)"
    

    elif tipo in ['DB_TYPE_NVARCHAR', 'DB_TYPE_VARCHAR', 'DB_TYPE_CHAR']:
        tipo = "nvarchar"

        if pd.notna(size):
            return f"{tipo}({int(size)})"
        else:
            return tipo
    
    
    elif tipo == 'DB_TYPE_NUMBER':
        tipo = "numeric"
        
        if pd.notna(size):
            if int(size) == 127: # Bug del 127
                return "nvarchar(38)"
    
        return format_with_precision_and_scale(tipo, precision, scale)
    
    elif tipo == 'DB_TYPE_DECIMAL':
        tipo = "decimal"
        return format_with_precision_and_scale(tipo, precision, scale)
    


    
    else:
        return tipo





    
    

def ODS_Tables_creation_Logic(df, df_tipo_tabla):
    # Group by "TABLA ORIGEN"
    grouped = df.groupby(ODS_TABLAS)

    # Initialize the result DataFrame
    result_df = pd.DataFrame(columns=["ORIGEN", "TABLA ORIGEN", "QUERY CREATE"])

    # Process each group
    for table_name, group in grouped:
        # Extract the origin abbreviation
        origen = str(group['ORIGEN'].iloc[0])
        abreviatura_origen = origen[:3] if origen else "UNK"  # Default to "UNK" if origin is empty
        
        # New table name
        stg_name = str(group[STG_TABLAS].iloc[0])
        nuevo_name_tabla = get_ODS_table_name(table_name, stg_name, abreviatura_origen, df_tipo_tabla)

        # Create table fields
        fields = []
        for _, row in group.iterrows():
            field_name = row[ODS_CAMPOS] if pd.notna(row[ODS_CAMPOS]) else row[STG_CAMPOS]
            field = f"[{field_name}] {create_data_type(row)} NULL"
            fields.append(field)
        
        len_origen = len(origen)
        len_table_name = len(table_name)
        
        additional_fields = [
            "[HSH_PK0] binary(16) NOT NULL",
            "[FCH_CAR] datetime NULL",
            f"[DES_ORG] nvarchar({len_table_name}) NULL",
            f"[CON_ORG] nvarchar({len_origen}) NULL",
            "[REG_ACT] nvarchar(1) NULL"
        ]
        
        # Combine all fields
        all_fields = fields + additional_fields
        
        # Create the full CREATE TABLE query
        query_create = f"CREATE TABLE {nuevo_name_tabla} (\n" + ",\n".join(all_fields) + ",\nPRIMARY KEY(HSH_PK0)\n) ON [PRIMARY]"
        
        # Append to the result DataFrame
        result_df = pd.concat([result_df, pd.DataFrame({
            "ORIGEN": [origen],
            "TABLA ORIGEN": [table_name],
            "QUERY CREATE": [query_create]
        })], ignore_index=True)
        
    
    return result_df





def ODS_tables_creation(data_dict_path: str, output_path: str) -> None:

    df = pd.read_excel(data_dict_path)
    
    df_tipo_tabla = load_table_info_df()
    
    cleaned_df = clean_df_ODS(df)
    result_df = ODS_Tables_creation_Logic(cleaned_df, df_tipo_tabla)

    # Save the result to a new Excel file
    result_df.to_excel(output_path, index=False)
    print(f'Archivo generado en: {output_path}')