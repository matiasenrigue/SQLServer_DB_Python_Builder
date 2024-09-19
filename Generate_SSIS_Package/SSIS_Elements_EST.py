
import xml.etree.ElementTree as ET
import pandas as pd

from Utils.params import *
from Utils.class_Table import Table


"""
Script to manage the creation of the Execute SQL Tasks (EST)
"""


def create_execute_sql_task_TRUNCATE(parent_executables: ET.Element, table_info: Table) -> None:
    """
    Creates an Execute SQL Task to truncate a table in SSIS.

    Args:
        parent_executables (ET.Element): The parent XML element where the task will be added.
        table_info (Table): An instance of the Table class containing table-specific information.
    """
    est_id = table_info.generate_unique_id()
    
    est_task = ET.SubElement(parent_executables, "DTS:Executable", {
        "DTS:refId": table_info.SQL_task_1_TRUNCATE_reference_path,
        "DTS:CreationName": "Microsoft.ExecuteSQLTask",
        "DTS:Description": "Execute SQL Task",
        "DTS:DTSID": est_id,
        "DTS:ExecutableType": "Microsoft.ExecuteSQLTask",
        "DTS:LocaleID": "-1",
        "DTS:ObjectName": table_info.SQL_task_1_TRUNCATE_name,
        "DTS:TaskContact" : "Execute SQL Task; Microsoft Corporation; SQL Server 2019; © 2019 Microsoft Corporation; All Rights Reserved;http://www.microsoft.com/sql/support/default.asp;1"
    })
    
    ET.SubElement(est_task, "DTS:Variables")
    Object_subelemnt = ET.SubElement(est_task, "DTS:ObjectData")
    ET.SubElement(Object_subelemnt, "SQLTask:SqlTaskData", {
        "SQLTask:Connection": table_info.destination_connection_unique_id,
        "SQLTask:SqlStatementSource" : f"Truncate Table {table_info.SqlServer_Table_Name}",
        "xmlns:SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask"
    })
    




def create_execute_sql_task_EXECUTE_SP(parent_executables: ET.Element, table_info: Table) -> None:
    """
    Creates an Execute SQL Task in SSIS to execute an Stored Procedure

    Args:
        parent_executables (ET.Element): The parent XML element where the task will be added.
        table_info (Table): An instance of the Table class containing table-specific information.
    """
    
    est_id = table_info.generate_unique_id()
    
    est_task = ET.SubElement(parent_executables, "DTS:Executable", {
        "DTS:refId": table_info.SQL_task_2_EXEC_SP_reference_path,
        "DTS:CreationName": "Microsoft.ExecuteSQLTask",
        "DTS:Description": "Execute SQL Task",
        "DTS:DTSID": est_id,
        "DTS:ExecutableType": "Microsoft.ExecuteSQLTask",
        "DTS:LocaleID": "-1",
        "DTS:ObjectName": table_info.SQL_task_2_EXEC_SP_name,
        "DTS:TaskContact" : "Execute SQL Task; Microsoft Corporation; SQL Server 2019; © 2019 Microsoft Corporation; All Rights Reserved;http://www.microsoft.com/sql/support/default.asp;1"
    })
    
    ET.SubElement(est_task, "DTS:Variables")
    Object_subelemnt = ET.SubElement(est_task, "DTS:ObjectData")
    ET.SubElement(Object_subelemnt, "SQLTask:SqlTaskData", {
        "SQLTask:Connection": table_info.destination_connection_unique_id,
        "SQLTask:SqlStatementSource" : f"EXEC {table_info.Stored_Procedure_name}",
        "xmlns:SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask"
    })
    