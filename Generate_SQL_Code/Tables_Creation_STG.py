import pandas as pd

from Utils.Utils import get_STG_table_name


"""
Python Script to generate SQL Server code to create tables in the STG database.

!! Warning: poor written code, but don't have time to improve it.
"""



def STG_tables_creation(data_dict_path: str, output_path: str) -> None:
    """
    Function to generate SQL Server code to create tables in the STG database.
    
    !! Warning: poor written code, but don't have time to improve it.
    """
    
    df = pd.read_excel(data_dict_path)


    # Column Names
    STG_CAMPOS = 'STG CAMPO ORIGEN'
    STG_TABLAS = 'STG TABLA ORIGEN'
    STG_TIPO = 'STG TIPO'
    STG_SIZE = 'STG SIZE'
    STG_PRECISION = 'STG PRECISION'
    STG_SCALE = 'STG SCALE'



    # Clean 'CAMPO ORIGEN' and 'TABLA ORIGEN'
    df[STG_CAMPOS] = df[STG_CAMPOS].str.strip().str.replace(",", "")
    df[STG_TABLAS] = df[STG_TABLAS].str.strip()
    df = df.drop_duplicates(subset=[STG_CAMPOS, STG_TABLAS])



    # Function to create SQL data type string
    def create_data_type(row):
        
        if row[STG_TIPO] in ['DB_TYPE_NVARCHAR', 'DB_TYPE_VARCHAR', 'DB_TYPE_CHAR']:    
            
            tipo = "nvarchar"
            if pd.notna(row[STG_SIZE]):
                size = int(row[STG_SIZE])  # Default size to 255 if NaN
                return f"{tipo}({size})"
                
            else:
                return tipo
            
        
        elif row[STG_TIPO] == 'DB_TYPE_NUMBER':
            tipo = "numeric"
            
            if int(row[STG_SIZE]) == 127: # Bug del 127
                return "nvarchar(38)"
            
            if pd.notna(row[STG_PRECISION]) and pd.notna(row[STG_SCALE]):
                precision = int(row[STG_PRECISION])
                scale = int(row[STG_SCALE])
                
                if precision <= 0:
                    precision = 2
                if scale <= 0:
                    scale = 0
                return f"{tipo}({precision}, {scale})"
            else:
                return f"{tipo}"
        
        elif row[STG_TIPO] in ['DB_TYPE_DATE', 'DB_TYPE_TIMESTAMP']:
            return "datetime"
        
            
        elif row[STG_TIPO] in ['DB_TYPE_CLOB']:
            return "nvarchar(max)"
        
        
        else:
            return row[STG_TIPO]
        
        


    # Group by "TABLA ORIGEN"
    grouped = df.groupby(STG_TABLAS)

    # Initialize the result DataFrame
    result_df = pd.DataFrame(columns=["ORIGEN", "TABLA ORIGEN", "QUERY CREATE"])


    # Process each group
    for table_name, group in grouped:
        # Extract the origin abbreviation
        origen = str(group['ORIGEN'].iloc[0])
        abreviatura_origen = origen[:3] if origen else "UNK"  # Default to "UNK" if origin is empty
        
        # New table name
        nuevo_name_tabla = get_STG_table_name(table_name, abreviatura_origen)
        
        # Create table fields
        fields = []
        for _, row in group.iterrows():
            field = f"{row[STG_CAMPOS]} {create_data_type(row)}"
            fields.append(field)
        
        
        len_origen = len(origen)
        len_table_name = len(table_name)
        
        additional_fields = [
            "[HSH_PK0] binary(16)",
            "[FCH_CAR] datetime DEFAULT getdate()",
            f"[DES_ORG] nvarchar({len_table_name}) DEFAULT '{table_name}'",
            f"[CON_ORG] nvarchar({len_origen}) DEFAULT '{origen}'",
            "[REG_ACT] nvarchar(1) DEFAULT '1'"
        ]
        
        # Combine all fields
        all_fields = fields + additional_fields
        
        # Create the full CREATE TABLE query
        query_create = f"CREATE TABLE {nuevo_name_tabla} (\n" + ",\n".join(all_fields) + "\n);"
        
        # Append to the result DataFrame
        result_df = pd.concat([result_df, pd.DataFrame({
            "ORIGEN": [origen],
            "TABLA ORIGEN": [table_name],
            "QUERY CREATE": [query_create]
        })], ignore_index=True)

    # Save the result to a new Excel file


    result_df.to_excel(output_path, index=False)
    print(f'Archivo generado en: {output_path}')