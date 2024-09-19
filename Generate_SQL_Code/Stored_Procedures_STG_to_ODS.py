import pandas as pd
from Utils.params import *
from Utils.Utils import load_table_info_df, get_ODS_table_name, get_STG_table_name, obtain_table_info
from Generate_SQL_Code.Tables_Creation_ODS import clean_df_ODS, create_data_type


"""
Python Script to generate SQL Server code to create the stored procedures that take the data from the STG database to the ODS database.
"""


def get_date_incremental(date_incremental):
    
    if date_incremental.strip() == 'None': # Borrar solo los no nulos bien, sin lios
        date_incremental = None

    if date_incremental:
        return f" {date_incremental}"
    
    else:
        return ""





def generate_stored_procedure(df, df_tipo_tabla):
       
    # Agrupar por la tabla de ODS
    grouped = df.groupby(ODS_TABLAS)

    # Inicializar DataFrame de resultados
    result_df = pd.DataFrame(columns=["ORIGEN", "TABLA ORIGEN", "QUERY CREATE"])

    # Procesar cada grupo
    for table_name, group in grouped:
        origen = str(group['ORIGEN'].iloc[0])
        abreviatura_origen = origen[:3] if origen else "UNK"
        
        table_name_stg = str(group[STG_TABLAS].iloc[0]) # Info de la columna de STG
        table_name_ods = str(group[ODS_TABLAS].iloc[0]) if pd.notna(group[ODS_TABLAS].iloc[0]) else table_name_stg # Info de la columna de ODS
                
        # Nombre de la tabla ODS
        STG_table_name = get_STG_table_name(table_name_stg, abreviatura_origen)
        ODS_table_name = get_ODS_table_name(table_name_ods, table_name_stg, abreviatura_origen, df_tipo_tabla)
        stored_procedure_name = ODS_table_name.replace("[ods].[", "[ods].[SP_")

        # Crear campos para el SELECT y el MERGE
        fields_cambio_formato = []
        fields_update_set = []
        fields_not_matched_INSERT = []
        fields_not_matched_VALUES = []
        
        count_datos_que_difieren_de_STG_a_ODS = 0 # Si hay al menos un dato diferente al pasar a ODS, hay que hacer el procedimiento largo
        for _, row in group.iterrows():
            field_name_stg = row[STG_CAMPOS]
            field_name_ods = row[ODS_CAMPOS] if pd.notna(row[ODS_CAMPOS]) else field_name_stg
            tipo_dato = create_data_type(row)
            
            if pd.notna(row[ODS_TIPO]) or pd.notna(row[ODS_SIZE]) or pd.notna(row[ODS_PRECISION]) or pd.notna(row[ODS_SCALE]):  # Hay al menos un dato que difiere
                count_datos_que_difieren_de_STG_a_ODS += 1
                fields_cambio_formato.append(f"CAST({field_name_stg} as {tipo_dato}) as {field_name_ods}")
            else:
                fields_cambio_formato.append(field_name_stg)

            fields_update_set.append(f"ODS.{field_name_ods} = STG.{field_name_ods}")  # Contra intuitivamente aquí se pone el nombre ods donde podría pensarse que va el nombre stg
            fields_not_matched_INSERT.append(f"{field_name_ods}")       # Esto es porque: Si el nombre STG = ODS se queda igual, y si es diferente en la CTE se habrña cambiado, y por ende hay que pillar el nuevo del cambio (ODS)
            fields_not_matched_VALUES.append(f"STG.{field_name_ods}")
            

        # Campos adicionales
        additional_fields = ["HSH_PK0", "FCH_CAR", "DES_ORG", "CON_ORG", "REG_ACT"]
        fields_cambio_formato += additional_fields
        fields_update_set += [f"ODS.{col} = STG.{col}" for col in additional_fields]
        fields_not_matched_INSERT += [f"{col}" for col in additional_fields]
        fields_not_matched_VALUES += [f"STG.{col}" for col in additional_fields]
        
        # Obtener Date Incremental
        info_tabla = obtain_table_info(table_name_stg, df_tipo_tabla)
        date_incremental = get_date_incremental(info_tabla.incremental_STG_a_ODS)
        
        
        
        if count_datos_que_difieren_de_STG_a_ODS > 0: #Hay al menos un dato diferente
            Merged_table = "CAMBIO_FORMATO"
            fields_joined = ',\n           '.join(fields_cambio_formato)
            Merge_Code = f"""
            -- Creación del bloque CTE con el formato adecuado
            WITH CAMBIO_FORMATO AS (
                SELECT 
                    {fields_joined}
                FROM [ACQ].{STG_table_name}
            )"""
            
        else:
            Merged_table = STG_table_name
            Merge_Code = ""  

        fileds_update_set_joined = ',\n        '.join(fields_update_set)
        fields_not_matched_INSERT_joined = ',\n        '.join(fields_not_matched_INSERT)
        fields_not_matched_VALUES_joined = ',\n        '.join(fields_not_matched_VALUES)

            # Crear el cuerpo del procedimiento almacenado
        stored_procedure = f"""
        CREATE PROCEDURE {stored_procedure_name}
        AS
        BEGIN {Merge_Code}

            -- Instrucción MERGE
            MERGE {ODS_table_name} ODS
            USING {Merged_table} STG
            ON ODS.[HSH_PK0] = STG.[HSH_PK0]
            
            -- Actualización de registros cuando coinciden
            WHEN MATCHED THEN
            UPDATE SET
                {fileds_update_set_joined}
            
            -- Inserción de nuevos registros cuando no coinciden
            WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                {fields_not_matched_INSERT_joined}
                )
                
            VALUES (
                {fields_not_matched_VALUES_joined}
                )
            
            -- Actualización de registros no encontrados en STG
            WHEN NOT MATCHED BY SOURCE{date_incremental} THEN
            Update set  REG_ACT = 0; 

            END
            """
        # Añadir al DataFrame de resultados
              
        result_df = pd.concat([result_df, pd.DataFrame({
            "ORIGEN": [origen],
            "TABLA ORIGEN": [stored_procedure_name],
            "QUERY CREATE": [stored_procedure.strip()]
        })], ignore_index=True)
            
 
    return result_df




def Stored_Procedures_STG_to_ODS(data_dict_path: str, output_path: str) -> None:
    
    # Cargar archivos Excel
    df = pd.read_excel(data_dict_path)
    
    df_tipo_tabla = load_table_info_df()

    # Limpiar el DataFrame
    cleaned_df = clean_df_ODS(df)

    # Generar los procedimientos almacenados
    result_sp_df = generate_stored_procedure(cleaned_df, df_tipo_tabla)
    

    # Guardar los procedimientos generados en un archivo Excel
    result_sp_df.to_excel(output_path, index=False)
    print(f'Stored Procedures generados en: {output_path}')