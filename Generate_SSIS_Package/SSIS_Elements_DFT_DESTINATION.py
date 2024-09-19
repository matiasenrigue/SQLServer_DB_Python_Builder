import xml.etree.ElementTree as ET
import pandas as pd
from Utils.Utils import add_length_or_precision
from Utils.params import *
from Utils.class_Table import Table



"""
Script to Manage Destination Data Sending with SSIS
"""



def generate_input_columns(parent_executable: ET.Element, df: pd.DataFrame, table_info: Table) -> None:
    """
    Generates input columns for the destination component in the data flow task in SSIS.

    Args:
        parent_executable (ET.Element): The parent XML element where the columns will be added.
        df (pd.DataFrame): DataFrame containing the table fields information.
        table_info (Table): An instance of the Table class containing table-specific information.
    """
    
    for index, row in df.iterrows():
        column_name = row[COLUMNA_CAMPO]
        data_type = row[COLUMNA_TIPO]
        length = row[COLUMNA_LONGITUD]
        precision = row[COLUMNA_PRECISION]
        scale = row[COLUMNA_ESCALA]

        input_column_attributes = {
            'refId': f"{table_info.DFT_Destination_task_reference_path}.Inputs[OLE DB Destination Input].Columns[{column_name}]",
            'cachedDataType': data_type,
            'cachedName': column_name,
            'externalMetadataColumnId': f"{table_info.DFT_Destination_task_reference_path}.Inputs[OLE DB Destination Input].ExternalColumns[{column_name}]",
            'lineageId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Output].Columns[{column_name}]" # OJO aquí hay que usar la de Origen
        }
        
        # Cannot use the default function because of 'cached' named attributes
        input_column_attributes = add_length_or_precision(input_column_attributes, data_type, length, precision, scale, cached_names=True)
        
        ET.SubElement(parent_executable, "inputColumn", input_column_attributes)




def generate_external_metadata_columns(parent_executable: ET.Element, df: pd.DataFrame, table_info: Table) -> None:
    """
    Generates external metadata columns for the destination component in the data flow task in SSIS.

    Args:
        parent_executable (ET.Element): The parent XML element where the columns will be added.
        df (pd.DataFrame): DataFrame containing the table fields information.
        table_info (Table): An instance of the Table class containing table-specific information.
    """
    for index, row in df.iterrows():
        column_name = row[COLUMNA_CAMPO]
        data_type = row[COLUMNA_TIPO]
        length = row[COLUMNA_LONGITUD]
        precision = row[COLUMNA_PRECISION]
        scale = row[COLUMNA_ESCALA]

        external_metadata_column_attributes = {
            'refId': f"{table_info.DFT_Destination_task_reference_path}.Inputs[OLE DB Destination Input].ExternalColumns[{column_name}]",
            'dataType': data_type,
            'name': column_name
        }
        
        external_metadata_column_attributes = add_length_or_precision(external_metadata_column_attributes, data_type, length, precision, scale)
        ET.SubElement(parent_executable, "externalMetadataColumn", external_metadata_column_attributes)






def add_destination_to_data_flow_task(parent_executable: ET.Element, reference_df_fields: pd.DataFrame, table_info: Table) -> None:
    """
    Adds the destination component to the data flow task in SSIS.

    Args:
        parent_executable (ET.Element): The parent XML element where the destination component will be added.
        reference_df_fields (pd.DataFrame): DataFrame containing the table fields information.
        table_info (Table): An instance of the Table class containing table-specific information.
    """
    
        
    # Create the destination component
    component_id = table_info.generate_unique_id()
    component = ET.SubElement(parent_executable, "component", {
        "refId": table_info.DFT_Destination_task_reference_path,
        "componentClassID": "Microsoft.OLEDBDestination",
        "contactInfo": "OLE DB Destination;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation; All Rights Reserved; http://www.microsoft.com/sql/support;4",
        "description": "OLE DB Destination",
        "name": table_info.DFT_Destination_task_name,
        "usesDispositions": "true",
        "version": "4"
    })

    # Add component properties
    properties = ET.SubElement(component, "properties")
    
    ET.SubElement(properties, "property", {
        "dataType": "System.Int32",
        "description": "The number of seconds before a command times out.  A value of 0 indicates an infinite time-out.",
        "name": "CommandTimeout"
    }).text = "0"

    ET.SubElement(properties, "property", {
        "dataType": "System.String",
        "description": "Specifies the name of the database object used to open a rowset.",
        "name": "OpenRowset"
    }).text = table_info.SqlServer_Table_Name #[stg].[NEP_table_name] (NEP 0 PSC)

    ET.SubElement(properties, "property", {
        "dataType": "System.String",
        "description": "Specifies the variable that contains the name of the database object used to open a rowset.",
        "name": "OpenRowsetVariable"
    }).text=""

    
    ET.SubElement(properties, "property", {
        "dataType": "System.String",
        "description": "The SQL command to be executed.",
        "name": "SqlCommand",
        "UITypeEditor": "Microsoft.DataTransformationServices.Controls.ModalMultilineStringEditor"
    }).text = ""

    ET.SubElement(properties, "property", {
        "dataType": "System.Int32",
        "description": "Specifies the column code page to use when code page information is unavailable from the data source.",
        "name": "DefaultCodePage"
    }).text = "1252"

    ET.SubElement(properties, "property", {
        "dataType": "System.Boolean",
        "description": "Forces the use of the DefaultCodePage property value when describing character data.",
        "name": "AlwaysUseDefaultCodePage"
    }).text = "false"

    ET.SubElement(properties, "property", {
        "dataType": "System.Int32",
        "description": "Specifies the mode used to access the database.",
        "name": "AccessMode",
        "typeConverter": "AccessMode"
    }).text = "0"

    ET.SubElement(properties, "property", {
        "dataType": "System.Boolean",
        "description": "Indicates whether the values supplied for identity columns will be copied to the destination. If false, values for identity columns will be auto-generated at the destination. Applies only if fast load is turned on.",
        "name": "FastLoadKeepIdentity"
    }).text = "false"

    ET.SubElement(properties, "property", {
        "dataType": "System.Boolean",
        "description": "Indicates whether the columns containing null will have null inserted in the destination. If false, columns containing null will have their default values inserted at the destination. Applies only if fast load is turned on.",
        "name": "FastLoadKeepNulls"
    }).text = "false"

    ET.SubElement(properties, "property", {
        "dataType": "System.String",
        "description": "Specifies options to be used with fast load.  Applies only if fast load is turned on.",
        "name": "FastLoadOptions"
    }).text = ""

    ET.SubElement(properties, "property", {
        "dataType": "System.Int32",
        "description": "Specifies when commits are issued during data insertion.  A value of 0 specifies that one commit will be issued at the end of data insertion.  Applies only if fast load is turned on.",
        "name": "FastLoadMaxInsertCommitSize"
    }).text = "2147483647"


    # Add connection
    connections = ET.SubElement(component, "connections")
    ET.SubElement(connections, "connection", {  
        "refId": f"{table_info.DFT_Destination_task_reference_path}.Connections[OleDbConnection]",
        "connectionManagerID": table_info.destination_connection_ref_ID,
        "connectionManagerRefId": table_info.destination_connection_ref_ID,
        "description": "The OLE DB runtime connection used to access the database.",
        "name": "OleDbConnection"
    })
    
    
    ## Inputs:

    inputs = ET.SubElement(component, "inputs")
    input_element = ET.SubElement(inputs, "input", {
        "refId": f"{table_info.DFT_Destination_task_reference_path}.Inputs[OLE DB Destination Input]",
        "errorOrTruncationOperation": "Insert",
        "errorRowDisposition": "FailComponent",
        "hasSideEffects": "true",
        "name": "OLE DB Destination Input"
    })
    
    input_columns = ET.SubElement(input_element, "inputColumns")
    generate_input_columns(parent_executable = input_columns, df= reference_df_fields, table_info=table_info)

    
    # Metadata
    external_metadata_columns = ET.SubElement(input_element, "externalMetadataColumns", {"isUsed": "True"})
    generate_external_metadata_columns(parent_executable = external_metadata_columns, df= reference_df_fields, table_info=table_info)
    
    
    # Error Output
    outputs = ET.SubElement(component, "outputs")
    error_output = ET.SubElement(outputs, "output", {
        "refId": f"{table_info.DFT_Destination_task_reference_path}.Outputs[OLE DB Destination Error Output]",
        "exclusionGroup": "1",
        "isErrorOut": "true",
        "name": "OLE DB Destination Error Output",
        "synchronousInputId": f"{table_info.DFT_Destination_task_reference_path}.Inputs[OLE DB Destination Input]"
    })

    error_output_columns = ET.SubElement(error_output, "outputColumns")
    
    ET.SubElement(error_output_columns, "outputColumn", {
        'refId': f"{table_info.DFT_Destination_task_reference_path}.Outputs[OLE DB Destination Error Output].Columns[ErrorCode]",
        'dataType': "i4",
        'lineageId': f"{table_info.DFT_Destination_task_reference_path}.Outputs[OLE DB Destination Error Output].Columns[ErrorCode]", # Ojo!! Aquí Lineage ID es de DESTINO, no de ORIGEN
        'name': "ErrorCode",
        'specialFlags': "1"
    })
    
    ET.SubElement(error_output_columns, "outputColumn", {
        'refId': f"{table_info.DFT_Destination_task_reference_path}.Outputs[OLE DB Destination Error Output].Columns[ErrorColumn]",
        'dataType': "i4",
        'lineageId': f"{table_info.DFT_Destination_task_reference_path}.Outputs[OLE DB Destination Error Output].Columns[ErrorColumn]",  # Ojo!! Aquí Lineage ID es de DESTINO, no de ORIGEN
        'name': "ErrorColumn",
        'specialFlags': "2"
    })
    
    ET.SubElement(error_output, "externalMetadataColumns")