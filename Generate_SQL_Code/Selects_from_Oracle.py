import pandas as pd
from Utils.params import *
from Utils.Utils import load_table_info_df, obtain_table_info, convert_oracle_to_ssis_data_type



"""
Python Script to generate the Select Statements from Oracle for every table
- Those selects statements will be used to extract the data from Oracle to SSIS
- The SSIS package will then load the data to SQL Server
"""



def apply_query(group):
    
    tabla_origen = group.name[1]
    origen = group.name[0]
    campo_origen_series = group[COLUMNA_CAMPO]
    campos = ',\n'.join(campo_origen_series)
    
    InfoTabla = obtain_table_info(tabla_origen, info_pks_df)
    
    tipo_tabla = InfoTabla.tipo_tabla 
    pks_names = InfoTabla.pks
    incremental_oracle = InfoTabla.incremental_ORACLE_a_STG
        
    query_content = f"""SELECT 
    {campos}

    ,standard_hash(TO_CHAR({pks_names}), 'MD5') as HSH_PK0

    FROM {tabla_origen}"""   
    
    query = '\n'.join(line.strip() for line in query_content.split("\n")) 

    if incremental_oracle:
        query += f"\n\n{incremental_oracle}"
                 
    return query






def prepare_data_frame(input_path_file, output_path_file):
    
    
    # Import and Clean
    df = pd.read_excel(input_path_file)    
    df[COLUMNA_CAMPO] = df[COLUMNA_CAMPO].str.replace(";", "", regex=False)
    df[COLUMNA_CAMPO] = df[COLUMNA_CAMPO].str.replace(",", "", regex=False)
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df = df.drop_duplicates(subset=[COLUMNA_TABLA, COLUMNA_CAMPO])
    
    # Limpiar el archivo de PKs
    global info_pks_df
    info_pks_df = load_table_info_df()
    
    
    # Crear un DataFrame auxiliar con las queries
    filtered_df = df[[COLUMNA_ORIGEN, COLUMNA_TABLA, COLUMNA_CAMPO]]
    query_df = filtered_df.groupby([COLUMNA_ORIGEN, COLUMNA_TABLA]).apply(apply_query).reset_index()
    query_df.rename(columns={0: COLUMNA_QUERY}, inplace=True)
    
    # Limpiar el query_df
    query_df = query_df.drop_duplicates(subset=[COLUMNA_TABLA])
    query_df[COLUMNA_QUERY] = query_df[COLUMNA_QUERY].str.replace(',,', ',') # evitar bug que ponía doble coma
    query_df[COLUMNA_QUERY] = query_df[COLUMNA_QUERY].str.replace(' ,', ',')
    
    # Merge del DataFrame original con el query_df
    final_df = pd.merge(df, query_df, on=[COLUMNA_ORIGEN, COLUMNA_TABLA], how="left")
    
    # Convert Data types de Oracle a SSIS
    final_df[COLUMNA_TIPO] = final_df[COLUMNA_TIPO].apply(convert_oracle_to_ssis_data_type)
    
    # Identify duplicates
    duplicates = final_df[final_df.duplicated(subset=[COLUMNA_TABLA, COLUMNA_CAMPO], keep=False)]
    if duplicates.empty:
        print(f"Duplicates en df Datos: {duplicates}")
    final_df = final_df.drop_duplicates(subset=[COLUMNA_TABLA, COLUMNA_CAMPO])
    
    # Exportar el DataFrame final a Excel
    final_df.to_excel(output_path_file, index=False)
        
    return final_df



def create_dictionary_from_dataframe(df):
    """
    Returns a dictionary with the following structure:
    
    {
        'ORIGIN_1': {
            'T1': [row_info_1, row_info_2, ...],
            'T2': [row_info_3, row_info_4, ...]
        },
        'ORIGIN_2': {
            'T3': [row_info_5, row_info_6, ...],
            'T4': [row_info_7, row_info_8, ...]
        }
    }
    """
    result_dict = {}
    
    for index, row in df.iterrows():
        origin = row[COLUMNA_ORIGEN]
        table = row[COLUMNA_TABLA]
        
        # Create a new dictionary entry for the origin if it doesn't exist
        if origin not in result_dict:
            result_dict[origin] = {}
        
        # Create a list for the table in the origin if it doesn't exist
        if table not in result_dict[origin]:
            result_dict[origin][table] = []
        
        # Append the full row info to the table list
        result_dict[origin][table].append(row.to_dict())
            
    return result_dict