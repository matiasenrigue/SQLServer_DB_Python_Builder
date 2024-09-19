import cx_Oracle
import pandas as pd
import time
import os

from Utils.params import *


"""
Python Script to get the types and lenght of the fields in a Oracle Database
Objective: Fill the columns TIPO, SIZE, PRECISION and SCALE in the Excel file

This python script isn't part of the package, it's a standalone script to be run in the remote computer
"""

# Load the Excel file
file_path = DATA_DICT_FILE
output_file_path = F'updated_{file_path}'


# Functions to connect to Oracle databases
def get_origin_1_connection():
    dsn_tns = cx_Oracle.makedsn(ORIGIN_1_HOST_NAME, ORIGIN_1_PORT, service_name=ORIGIN_1_SERVICE_NAME)
    conexion = cx_Oracle.connect(user=ORIGIN_1_USER, password=ORIGIN_1_PASSWORD, dsn=dsn_tns)
    return conexion

def get_origin_2_connection(): 
    dsn_tns = cx_Oracle.makedsn(ORIGIN_2_HOST_NAME, ORIGIN_2_PORT, service_name=ORIGIN_2_SERVICE_NAME)
    conexion = cx_Oracle.connect(user=ORIGIN_2_USER, password=ORIGIN_2_PASSWORD, dsn=dsn_tns)
    return conexion

def execute_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall(), cursor.description


df = pd.read_excel(file_path)

# Ensure the TIPO and Longitud columns are of type object to store strings
df[COLUMNA_TIPO] = df[COLUMNA_TIPO].astype('object')
df[COLUMNA_LONGITUD] = df[COLUMNA_LONGITUD].astype('object')
df[COLUMNA_PRECISION] = df[COLUMNA_PRECISION].astype('object')
df[COLUMNA_ESCALA] = df[COLUMNA_ESCALA].astype('object')

print(df.head())




# Dictionary to cache query results
query_cache = {}

# Process each row in the DataFrame
total_rows = len(df)
for index, row in df.iterrows():
    
    # Save the file every X iterations to avoid losing progress
    if (index + 1) % 600 == 0:
        interim_file_path = f"{output_file_path[:-5]}_v{(index + 1) // 10}.xlsx"
        df.to_excel(interim_file_path, index=False)
        print(f"💾 Interim save: {interim_file_path}")
        print("⏳ Taking a short break...\n")
        time.sleep(2)
    
    
    match = 0
    
    tipo = row[COLUMNA_TIPO]
    origen = row['ORIGEN']
    query = row['QUERY']
    campo_origen = row['CAMPO ORIGEN']
    
    size = row[COLUMNA_LONGITUD]
    precision = row[COLUMNA_PRECISION]
    scale = row[COLUMNA_ESCALA]
    
    
    
    
    if pd.notna(tipo):
        
        if 'CLOB'in str(tipo):
            continue 
    
        if pd.notna(size): # If the size is already filled...
                
            if not 'NUMBER' in str(tipo).upper(): # If it's not a number, skip to the next row
                continue

            else:
                if pd.notna(precision) and pd.notna(scale): # If precision and scale are already filled, skip to the next row
                    continue
                
                if size == 127: # Bug: of size is 127, skip to the next row
                    continue
       


    
    print(f"Processing {campo_origen} at row {index} of {total_rows}...")
    
    if query not in query_cache:
        # Connect to the appropriate database
        if origen == ORIGIN_1_NAME:
            connection = get_origin_1_connection()
        elif origen == ORIGIN_2_NAME:
            connection = get_origin_2_connection()

        
        cursor = connection.cursor()
        
        # Execute the query
                    # Execute the query
        try:
            results, description = execute_query(cursor, query)
        except Exception as e:
            print(f"⚠️ Database error occurred for query at row {index + 1}: {e}")
            continue
        print(description)
        
        # Cache the results
        query_cache[query] = description
        
        cursor.close()
        connection.close()
        
        # Pause to avoid overwhelming the database
        time.sleep(5)
        
    else:
        description = query_cache[query]
  
    try:
    # Iterate over all rows with the same query
        query_rows = df[df['QUERY'] == query].copy()

        for query_index, query_row in query_rows.iterrows():
            campo_origen = query_row['CAMPO ORIGEN'].replace(",","").lower().strip()

            # Find the column in the description
            for col in description:
                field_name = col[0].replace(",","").lower().strip()
                field_type = col[1].name 
                field_size = col[2] if col[2] is not None else ''  
                field_precision = col[4] if col[4] is not None else ''
                field_scale = col[5] if col[5] is not None else ''

                if field_name == campo_origen:
                    query_rows.at[query_index, COLUMNA_TIPO] = field_type
                    query_rows.at[query_index, COLUMNA_LONGITUD] = field_size 
                    
                    if 'NUMBER' in field_type.upper() and field_precision is not None:
                        query_rows.at[query_index, COLUMNA_PRECISION] = field_precision
                        query_rows.at[query_index, COLUMNA_ESCALA] = field_scale
                        
                        print(f"📊 Field '{field_name}' - Type: {field_type}, Size: {field_size}, Precision: {field_precision}, Scale: {field_scale}")
                    
                    else:
                        query_rows.at[query_index, COLUMNA_PRECISION] = ''
                        query_rows.at[query_index, COLUMNA_ESCALA] = ''
                    
                    break


        # Perform the left join to update the original DataFrame
        df.update(query_rows)
    
    except Exception as e:
        print(f"⚠️ Error processing query results for row {index + 1}: {e}")
        continue



# Save the final output file
df.to_excel(output_file_path, index=False)
print(f"✅ Final output saved to {output_file_path}")