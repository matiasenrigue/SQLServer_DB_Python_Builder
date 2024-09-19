import xml.etree.ElementTree as ET
import pandas as pd

from Utils.params import *
from Utils.class_Table import Table
from Utils.class_SSIS_Object import SSIS_Object

from Generate_SSIS_Package.SSIS_Elements_DFT import create_data_flow_task
from Generate_SSIS_Package.SSIS_Elements_EST import create_execute_sql_task_TRUNCATE, create_execute_sql_task_EXECUTE_SP


"""
Script to structure a SEQ (Sequence Container in SSIS):
    - Define precedence constraints
    - Order the Data Flow Task (DFT)
    - Order the Execute SQL Task (EST)
"""


def create_precedence_constraint(precedence_constraints: ET.Element, precedence_name: str, from_task: str, to_task: str) -> None:
    """
    Creates a precedence constraint between two tasks in SSIS.

    Args:
        precedence_constraints (ET.Element): The XML element to add the precedence constraint to.
        precedence_name (str): The name of the precedence constraint.
        from_task (str): The path of the originating task.
        to_task (str): The path of the destination task.
    """
    
    pc_id = SSIS_Object.generate_unique_id()
    
    ET.SubElement(precedence_constraints, "DTS:PrecedenceConstraint", {
        "DTS:refId": f"{from_task}.PrecedenceConstraints[{precedence_name}]",
        "DTS:CreationName": "",
        "DTS:DTSID": f"{{{pc_id}}}",
        "DTS:From": f"{from_task}",
        "DTS:LogicalAnd": "True",
        "DTS:ObjectName": precedence_name,
        "DTS:To": f"{to_task}"
    })
       




def add_table_block_to_container(table_info: Table) -> None:
    """
    Adds a table block to the container, including Data Flow Task and Execute SQL Tasks with precedence constraints.

    Args:
        reference_df (pd.DataFrame): DataFrame containing reference data for the table fields.
        table_info (Table): An instance of the Table class containing table-specific information.
    """
    
    # Registar naming Tabla
    table_info.set_table_object_info()
    
    # Create the cointaner of the element 
    seq_container, seq_executables = table_info.create_lower_level_container()
    
    # Add Data Flow Task
    create_data_flow_task(
        parent_executables = seq_executables,  
        table_info = table_info,
        reference_df_fields = table_info.reference_df   # Solución temporal: Al limpiar código buscar donde se llamar eso y llamarlo según la clase
    )   
    
    # Add Execute SQL Task # 1 --> Truncate
    create_execute_sql_task_TRUNCATE(
        parent_executables = seq_executables, 
        table_info = table_info
    )
    
    # Add Execute SQL Task # 2 --> No Definida aún
    create_execute_sql_task_EXECUTE_SP(
        parent_executables = seq_executables, 
        table_info = table_info
    )   

    
    # Add Precedence Constraints
    precedence_constraints = ET.SubElement(seq_container, "DTS:PrecedenceConstraints")
    
    # EST 1 -> DFT 1
    create_precedence_constraint(
        precedence_constraints,  
        precedence_name = "Constraint",
        from_task = table_info.SQL_task_1_TRUNCATE_reference_path, 
        to_task = table_info.DFT_task_reference_path 
    ) 
    
    # DFT 1 -> EST 2
    create_precedence_constraint(
        precedence_constraints,  
        precedence_name = "Constraint 1",
        from_task = table_info.DFT_task_reference_path, 
        to_task = table_info.SQL_task_2_EXEC_SP_reference_path 
    ) 