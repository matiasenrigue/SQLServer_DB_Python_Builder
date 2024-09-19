
"""
Python script to create tables or stored procedures in SQL Server.
Better if executed in a Jupyter Notebook.

The columns 'SQL Server message' and 'ACTION' in the output Excel file will tell us what happened

This python script isn't part of the package, it's a standalone script to be run in the remote computer
"""

from Utils.params import *


########## Cell 1
USED_DB = "STG"    # STG , ODS



########## Cell 2
print("Kernel Works")
import pandas as pd
import pyodbc
import time




########## Cell 3
# Parámetros de conexión
server = SQL_SERVER_HOST_NAME
database = DATA_BASE_DESTINO


def execute_query(server: str, database: str, query: str) -> str:
    """
    Function to execute a query in SQL Server and return the messages from the server
    """
    driver = '{ODBC Driver 13 for SQL Server}'  
    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    try:
        cursor.execute(query)
        conn.commit()
        messages = "\n".join([msg[1] for msg in cursor.messages])
    except pyodbc.ProgrammingError as e:
        messages = str(e)
    except AttributeError as e:
        messages = str(e)
    finally:
        conn.close()
    
    return messages




def determine_action_done_by_SQL_Server(mensaje_sql):
    """
    Function to determine the action taken based on the SQL Server message
    """
    if "'pyodbc.Cursor' object has no attribute 'messages'" in mensaje_sql:
        return "Table Created"
    elif "There is already an object named" in mensaje_sql and "(2714)" in mensaje_sql:
        return "No action: Table already existed"
    else:
        return "ERROR"


def create_tables_in_SQL_Server(queries_df, base_file_path):
    """
    Function to create tables in SQL Server and register the returning messages of the server
    """
    messages_list = []
    queries_df['SQL Server message'] = ""
    queries_df['ACTION'] = ""
    
    for index, row in queries_df.iterrows():
        
        if row['SQL Server message'] != "":
            continue
        
        table_name = row['TABLA ORIGEN'].split('.')[-1]
        query = row['QUERY CREATE']

        print(f"Executing query for table: {table_name}")
        messages = execute_query(server, database, query)
        queries_df.at[index, 'SQL Server message'] = messages
        action = determine_action_done_by_SQL_Server(messages)
        queries_df.at[index, 'ACTION'] = action
        print(f"Table {table_name} checked/created. SQL Server Messages: {messages}")
        
        time.sleep(3)
        
        if (index + 1) % 10 == 0:
            iteration_file_path = f"{base_file_path.rsplit('.', 1)[0]}_part_{(index + 1) // 10}.xlsx"
            queries_df.to_excel(iteration_file_path, index=False)
            print(f"Saved progress at iteration {index + 1} as {iteration_file_path}")

    final_file_path = f"{base_file_path.rsplit('.', 1)[0]}_final_{USED_DB}.xlsx"
    queries_df.to_excel(final_file_path, index=False)
    print(f"Final save completed as {final_file_path}.")





########## Cell 4
file_path = f'Created_Tables_Queries_{USED_DB}.xlsx'  
base_output_file_path = 'Updated_Created_Tables_Queries.xlsx'
excel_data = pd.ExcelFile(file_path)
df_queries = excel_data.parse('Sheet1')

print(df_queries.head(30))



########## Cell 5
create_tables_in_SQL_Server(df_queries, base_output_file_path)

