import xml.etree.ElementTree as ET
import pandas as pd

from Utils.params import *
from Utils.Utils import add_length_or_precision
from Utils.class_Table import Table

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)



"""
Script to Manage Source Data Extraction with SSIS
"""




def generate_output_columns(parent_executable: ET.Element, df: pd.DataFrame, table_info: Table) -> None:
    """
    Generates output columns for the data flow task in SSIS.

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

        output_column_attributes = {
            'refId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Output].Columns[{column_name}]",
            'dataType': data_type,
            'errorOrTruncationOperation': 'Conversion',
            'errorRowDisposition': 'FailComponent',
            'externalMetadataColumnId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Output].ExternalColumns[{column_name}]",
            'lineageId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Output].Columns[{column_name}]",
            'name': column_name,
            'truncationRowDisposition': 'FailComponent'
        }
        
        output_column_attributes = add_length_or_precision(output_column_attributes, data_type, length, precision, scale)
        
        ET.SubElement(parent_executable, "outputColumn", output_column_attributes)





def generate_external_metadata_columns(parent_executable: ET.Element, df: pd.DataFrame, table_info: Table) -> None:
    """
    Generates external metadata columns for the data flow task in SSIS.

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
            'refId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Output].ExternalColumns[{column_name}]",
            'dataType': data_type,
            'name': column_name
        }
        
        external_metadata_column_attributes = add_length_or_precision(external_metadata_column_attributes, data_type, length, precision, scale)
        ET.SubElement(parent_executable, "externalMetadataColumn", external_metadata_column_attributes)





def generate_error_output_columns(parent_executable: ET.Element, df: pd.DataFrame, table_info: Table) -> None:
    """
    Generates error output columns for the data flow task in SSIS.

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

        output_column_attributes = {
            'refId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Error Output].Columns[{column_name}]",
            'dataType': data_type,
            'lineageId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Error Output].Columns[{column_name}]",
            'name': column_name
        }
        
        output_column_attributes = add_length_or_precision(output_column_attributes, data_type, length, precision, scale)
        ET.SubElement(parent_executable, "outputColumn", output_column_attributes)

    ET.SubElement(parent_executable, "outputColumn", {
        'refId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Error Output].Columns[ErrorCode]",
        'dataType': "i4",
        'lineageId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Error Output].Columns[ErrorCode]",
        'name': "ErrorCode",
        'specialFlags': "1"
    })
    
    ET.SubElement(parent_executable, "outputColumn", {
        'refId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Error Output].Columns[ErrorColumn]",
        'dataType': "i4",
        'lineageId': f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Error Output].Columns[ErrorColumn]",
        'name': "ErrorColumn",
        'specialFlags': "2"
    })







def add_source_to_data_flow_task(parent_executable: ET.Element, reference_df_fields: pd.DataFrame, table_info: Table) -> None:
    """
    Adds the source component to the data flow task in SSIS.

    Args:
        parent_executable (ET.Element): The parent XML element where the source component will be added.
        reference_df_fields (pd.DataFrame): DataFrame containing the table fields information.
        table_info (Table): An instance of the Table class containing table-specific information.
    """
    
    # Create component OLE DB Source
    component_id = table_info.generate_unique_id()
    component = ET.SubElement(parent_executable, "component", {
        "refId": table_info.DFT_Origin_task_reference_path,
        "componentClassID": "Microsoft.OLEDBSource",
        "contactInfo": "OLE DB Source;Microsoft Corporation; Microsoft SQL Server; (C) Microsoft Corporation; All Rights Reserved; http://www.microsoft.com/sql/support;7",
        "description": "OLE DB Source",
        "name": table_info.DFT_Origin_task_name,
        "usesDispositions": "true",
        "version": "7"
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
    }).text=""

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
    }).text = table_info.query

    ET.SubElement(properties, "property", {
    "dataType": "System.String",
    "description": "The variable that contains the SQL command to be executed.",
    "name": "SqlCommandVariable"
    }).text=""

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
    }).text = "2"

    ET.SubElement(properties, "property", {
        "dataType": "System.String",
        "description": "The mappings between the parameters in the SQL command and variables.",
        "name": "ParameterMapping"
    }).text=""

    # Add connection
    connections = ET.SubElement(component, "connections")
    ET.SubElement(connections, "connection", {  
        "refId": f"{table_info.DFT_Origin_task_reference_path}.Connections[OleDbConnection]",
        "connectionManagerID": table_info.origin_connection_ref_ID,
        "connectionManagerRefId": table_info.origin_connection_ref_ID,
        "description": "The OLE DB runtime connection used to access the database.",
        "name": "OleDbConnection"
    })

    # Add outputs
    outputs = ET.SubElement(component, "outputs")
    output = ET.SubElement(outputs, "output", {
        "refId": f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Output]",
        "name": "OLE DB Source Output"
    })
    
    output_columns = ET.SubElement(output, "outputColumns")
    generate_output_columns(parent_executable= output_columns, df = reference_df_fields, table_info = table_info)


    # Metadata
    external_metadata_columns = ET.SubElement(output, "externalMetadataColumns", {"isUsed": "True"})
    generate_external_metadata_columns(parent_executable= external_metadata_columns, df = reference_df_fields, table_info = table_info)
    
    # Error Outputs
    error_output = ET.SubElement(outputs, "output", {
        "refId": f"{table_info.DFT_Origin_task_reference_path}.Outputs[OLE DB Source Error Output]",
        "isErrorOut": "true",
        "name": "OLE DB Source Error Output"
    })

    error_output_columns = ET.SubElement(error_output, "outputColumns")
    generate_error_output_columns(parent_executable= error_output_columns, df = reference_df_fields, table_info = table_info)

    ET.SubElement(error_output, "externalMetadataColumns")