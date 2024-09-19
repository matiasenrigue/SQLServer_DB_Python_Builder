import xml.etree.ElementTree as ET
import pandas as pd

from Utils.params import *
from Utils.class_Table import Table
from Utils.class_SSIS_Object import SSIS_Object

from Generate_SSIS_Package.SSIS_Elements_DFT_SOURCE import add_source_to_data_flow_task
from Generate_SSIS_Package.SSIS_Elements_DFT_DESTINATION import add_destination_to_data_flow_task


"""
Script to manage the creation of the Data Flow Task, including:
    - Creation of the "Source DFT" to receive data
    - Creation of the "Destination DFT" to send data
"""



def create_data_flow_task(parent_executables: ET.Element, table_info: Table, reference_df_fields: pd.DataFrame) -> None:
    """
    Creates a Data Flow Task in SSIS, including source and destination components.

    Args:
        parent_executables (ET.Element): The parent XML element where the task will be added.
        table_info (Table): An instance of the Table class containing table-specific information.
        reference_df_fields (pd.DataFrame): DataFrame containing reference data for the table fields.
    """
    
    dft_id = table_info.generate_unique_id()
    
    dft_task = ET.SubElement(parent_executables, "DTS:Executable", {
        "DTS:refId": table_info.DFT_task_reference_path,
        "DTS:CreationName": "Microsoft.Pipeline",
        "DTS:Description": "Data Flow Task",
        "DTS:DTSID": f"{{{dft_id}}}",
        "DTS:ExecutableType": "Microsoft.Pipeline",
        "DTS:LocaleID": "-1",
        "DTS:ObjectName": table_info.DFT_task_name,
        "DTS:TaskContact" : "Performs high-performance data extraction, transformation and loading;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation; All Rights Reserved;http://www.microsoft.com/sql/support/default.asp;1"
    })
    
    
    ET.SubElement(dft_task, "DTS:Variables")
    Object_subelemnt = ET.SubElement(dft_task, "DTS:ObjectData")
    pipeline = ET.SubElement(Object_subelemnt, "pipeline", {"version": "1"})
    components = ET.SubElement(pipeline, "components")
    
    
    
    # Add metadata columns to the queries that need it      
    if "standard_hash(TO_CHAR" in table_info.query:
        first_row = reference_df_fields.iloc[0]
        metadata_columns = [
            {COLUMNA_ORIGEN: first_row[COLUMNA_ORIGEN], COLUMNA_TABLA: first_row[COLUMNA_TABLA], COLUMNA_CAMPO: 'HSH_PK0', COLUMNA_TIPO: 'bytes', COLUMNA_LONGITUD: 16, COLUMNA_PRECISION: None},
        ]
        metadata_df = pd.DataFrame(metadata_columns)
        reference_df_fields = pd.concat([reference_df_fields, metadata_df], ignore_index=True)
        
            
    add_destination_to_data_flow_task(
        parent_executable = components, 
        reference_df_fields = reference_df_fields,
        table_info = table_info
        )
    
    
    add_source_to_data_flow_task(
        parent_executable = components, 
        reference_df_fields = reference_df_fields,
        table_info = table_info
    )
    
    
    paths = ET.SubElement(pipeline, "paths")
    ET.SubElement(paths, "path", {
        "refId": f"{table_info.DFT_task_reference_path}.Paths[OLE DB Source Output]",
        "endId": f"{table_info.DFT_Destination_task_reference_path}.Inputs[OLE DB Destination Input]",
        "name": "OLE DB Source Output",
        "startId": f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Output]"
    })