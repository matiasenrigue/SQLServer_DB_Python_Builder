from Utils.params import *
from Utils.Utils import process_table_name_short, get_STG_table_name, get_ODS_table_name
from Utils.class_SSIS_Object import SSIS_Object

import pandas as pd



"""
This script defines a Table class that extends the SSIS_Object class. 
It is used in the context of SQL Server Integration Services (SSIS) to handle table-related operations, including setting up connections, generating task names and paths, and creating containers.

Functions and their purposes:
    -  __init__: Initializes a Table object with parent object, origin database, table name, and query.
    - set_connections: Sets up the connection information based on the origin database.
    - set_table_object_info: Sets up the table-related object information such as task names and paths.
    - create_lower_level_container: Creates a lower-level container for the table and returns the sequence container and its executables.
"""


class Table(SSIS_Object):
    """
    Class representing a table in an SSIS (SQL Server Integration Services) context.
    Inherits from SSIS_Object and handles table-related operations such as setting up
    connections and generating task names and paths.
    """
    

    def __init__(self, parent_object: list, origin_DB: str, table_name: str, all_info_rows: dict, df_dim_facts:pd.DataFrame) -> None:
        """
        Initializes a Table object with the given parameters.

        Args:
            parent_object (list): The parent SSIS object.
            origin_DB (str): The origin database name.
            table_name (str): The name of the table.
            all_info_rows (dict): The info of all the wors associated with the table.
            df_dim_facts (df): The info concerning DIM / Facts
        """
        
        super().__init__(parent_object)
        self.origin_DB = origin_DB 
        self.table_name = table_name
        
        self.reference_df = pd.DataFrame(all_info_rows) 
        self.df_dim_fact = df_dim_facts
        
        self.first_row_of_reference_df = all_info_rows[0]            
        self.query = self.first_row_of_reference_df["QUERY"]      
    
    
    
    def set_connections(self) -> None:
        """
        Sets up the connection information based on the origin database.
        Should be called after initiating the loop to determine which connection to use.
        """

        if self.origin_DB == ORIGIN_1_NAME:
            self.origin_connection_info = (ORIGIN_1_NAME, self.connection_info_Origin_1[0], self.connection_info_Origin_1[1])
        
        elif self.origin_DB == ORIGIN_2_NAME:
            self.origin_connection_info = (ORIGIN_2_NAME, self.connection_info_Origin_2[0], self.connection_info_Origin_2[1])
            
        else:
            print(f"\nProblemas para la conexión con {self.origin_DB}\n")
        
        # Origin Connections
        self.origin_connection_full_name = self.origin_connection_info[0]
        self.origin_connection_display_name = self.origin_connection_full_name[:3] # Para guardar en nombres tabla solo las primeras 3 letras de la conexión
        self.origin_connection_ref_ID = self.origin_connection_info[1]
        self.origin_connection_unique_id = self.origin_connection_info[2]
        
        # Destination Connections
        self.destination_connection_ref_ID = self.connection_info_SqlServer[0]
        self.destination_connection_unique_id = self.connection_info_SqlServer[1]
                
     
     
    def set_table_object_info(self)-> None:
        """
        Sets up the table-related object information such as task names and paths.
        Should be called after initiating the loop to determine which connection to use.
        """   
        self.diplay_name_SSIS = process_table_name_short(self.table_name)
        self.display_name_SqlServer = process_table_name_short(self.table_name)
        
        # SQL Server Namings
        name_in_stg_according_to_data_dict = self.first_row_of_reference_df[STG_TABLAS]
        temp_ods_name_data_dict = self.first_row_of_reference_df[ODS_TABLAS]
        if isinstance(temp_ods_name_data_dict, float):
            temp_ods_name_data_dict = ""
        if temp_ods_name_data_dict and temp_ods_name_data_dict not in ("", "nan"):
            name_in_ods_according_to_data_dict = temp_ods_name_data_dict  
        else: 
            name_in_ods_according_to_data_dict = name_in_stg_according_to_data_dict
                        
        self.table_STG_name = get_STG_table_name(name_in_stg_according_to_data_dict, self.origin_connection_display_name)
        self.table_ODS_name = get_ODS_table_name(name_in_stg_according_to_data_dict, name_in_ods_according_to_data_dict, self.origin_connection_display_name, self.df_dim_fact)
        self.Stored_Procedure_name = self.table_ODS_name.replace("[ods].[", "[ods].[SP_")
        
        # SEQ Container
        self.SEQ_container_name = f"SEQ | {self.diplay_name_SSIS}"
        self.SEQ_container_reference_path = f"{self.parent_reference_path}\\{self.SEQ_container_name}"
        
        # DFT Task
        self.DFT_task_name = f"DFT | STG_{self.diplay_name_SSIS}"
        self.DFT_task_reference_path = f"{self.SEQ_container_reference_path}\\{self.DFT_task_name}"
        
        # DFT Task --> Origin
        self.DFT_Origin_task_name = f"ORIGEN | {self.origin_connection_display_name} | {self.diplay_name_SSIS}"
        self.DFT_Origin_task_reference_path = f"{self.DFT_task_reference_path}\\{self.DFT_Origin_task_name}"
        
        # DFT Task --> Destination
        self.DFT_Destination_task_name = f"DESTINO | {DATA_BASE_DESTINO} | {self.diplay_name_SSIS}"
        self.DFT_Destination_task_reference_path = f"{self.DFT_task_reference_path}\\{self.DFT_Destination_task_name}"
        
        # SQL Task 1 --> Truncate        
        self.SQL_task_1_TRUNCATE_name = f"EST | TRUNCATE STG_{self.origin_connection_display_name}_{self.diplay_name_SSIS}"
        self.SQL_task_1_TRUNCATE_reference_path = f"{self.SEQ_container_reference_path}\\{self.SQL_task_1_TRUNCATE_name}"
        
        # SQL Task 1 --> Future
        self.SQL_task_2_EXEC_SP_name = f"EST | SP_{self.origin_connection_display_name}_{self.diplay_name_SSIS}"
        self.SQL_task_2_EXEC_SP_reference_path = f"{self.SEQ_container_reference_path}\\{self.SQL_task_2_EXEC_SP_name}"
        
        # SQL Server Name
        self.SqlServer_Table_Name = f"[stg].[{self.origin_connection_display_name}_{self.display_name_SqlServer}]"
       
    
    
    
    def create_lower_level_container(self)-> tuple:
        """
        Creates a lower-level container for the table.

        Returns:
            tuple: The sequence container and its executables.
        """
        
        container_name = self.SEQ_container_name
        ruta_reference = self.SEQ_container_reference_path 
        
        seq_container, seq_executables = self.create_container(container_name, ruta_reference)
        
        return seq_container, seq_executables         
    
        