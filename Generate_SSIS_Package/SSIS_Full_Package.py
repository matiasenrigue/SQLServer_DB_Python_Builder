import xml.etree.ElementTree as ET
import pandas as pd
import os

from Utils.class_Table import Table
from Utils.class_SSIS_Object import SSIS_Object
from Utils.params import *
from Utils.Utils import load_table_info_df
from Generate_SSIS_Package.SSIS_Elements_SEQ_Structure import add_table_block_to_container
from Generate_SSIS_Package.SSIS_Structure_Functions import register_SSIS_package



"""
This script defines a function create_dtsx that generates a DTSX (Data Transformation Services XML) package file based on a given data dictionary and saves it to a specified file path. 
It handles reading and processing a reference Excel file, removing duplicates, and setting up SSIS (SQL Server Integration Services) package structure and connections.
"""




def create_dtsx(data_dictionary: dict, output_file_path: str) -> None:
    """
    Creates a DTSX package file based on the provided data dictionary and saves it to the specified output file path.

    Args:
        data_dictionary (dict): A dictionary where keys are origin database names and values are lists of tables and queries.
        output_file_path (str): The file path where the DTSX package will be saved.
    """
    
    # Register SSIS package and connections
    root, root_executables, Origin_1_connection_LIST, Origin_2_connection_LIST, SqlServer_connection_LIST = register_SSIS_package()
    
    SSIS_Object.connection_info_Origin_1 = Origin_1_connection_LIST
    SSIS_Object.connection_info_Origin_2 = Origin_2_connection_LIST
    SSIS_Object.connection_info_SqlServer = SqlServer_connection_LIST
    
    # Create the main sequence container
    main_container = SSIS_Object(parent_object = [root_executables, None])
    main_seq_executables, _ = main_container.create_upper_level_container(level = 1)
    
    # Df with DIM / FACT info
    df_dim_facts = load_table_info_df()
    
    
    for origin_DB, table_list in data_dictionary.items():
                        
        # Create container for each origin database
        origin_container = SSIS_Object(parent_object = [main_seq_executables, None])
        origin_seq_executables, origin_seq_path = origin_container.create_upper_level_container(level = 2, origin_DB = origin_DB)       
        parent_object = [origin_seq_executables, origin_seq_path]  
            
            
        for table, all_info_rows in table_list.items():
                       
            table_info = Table(parent_object, origin_DB, table, all_info_rows, df_dim_facts)
            table_info.set_connections()
            
            add_table_block_to_container(                
                table_info = table_info
                )


    # Create the tree and write to the file
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")  # Pretty print the XML
    tree.write(output_file_path, encoding='utf-8', xml_declaration=True)












