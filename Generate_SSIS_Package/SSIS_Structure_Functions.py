import xml.etree.ElementTree as ET

from Utils.params import *

from Utils.Utils import generate_creation_date
from Utils.class_SSIS_Object import SSIS_Object



def register_SSIS_package() -> tuple:
    """
    Registers the SSIS package by creating the root XML element and adding the necessary attributes and connections.

    Returns:
        tuple: A tuple containing the root XML element, root executables element, and connection lists for origin_1, origin_2, and SQL Server.
    """
    
    # Define the namespace
    ns = {'DTS': 'www.microsoft.com/SqlServer/Dts'}
    ET.register_namespace('', ns['DTS'])
    
    # Create the root element with required attributes
    root_id = SSIS_Object.generate_unique_id()
    version_id = SSIS_Object.generate_unique_id()
    
    fecha_creacion = generate_creation_date()
    
    root = ET.Element("DTS:Executable", {
        "xmlns:DTS": ns['DTS'],  
        "DTS:refId": "Package",  
        "DTS:CreationDate": f"{fecha_creacion}",   
        "DTS:CreationName": "Microsoft.Package",   
        "DTS:CreatorComputerName": "WSRVPS9507",  
        "DTS:CreatorName": "Matias Enrigue",   
        "DTS:DTSID": f"{{{root_id}}}",  
        "DTS:ExecutableType": "Microsoft.Package",  
        "DTS:LastModifiedProductVersion": "15.0.2000.180",
        "DTS:LocaleID": "3082",    # España
        "DTS:ObjectName": "New_object",   
        "DTS:PackageType": "5",    # SSIS standard package
        "DTS:VersionBuild": "2",    # Sube con guardados
        "DTS:VersionGUID":f"{{{version_id}}}"  
    })
    
    # Add PackageFormatVersion property
    package_format_version = ET.SubElement(root, "DTS:Property", {"DTS:Name": "PackageFormatVersion"})
    package_format_version.text = "8"
    
    # Conections
    root, origin_1_connection_ID, origin_2_connection_LIST, SqlServer_connection_LIST = register_connections(root)
    
    # Add Variables element
    variables = ET.SubElement(root, "DTS:Variables")
    
    # Add Executables element
    root_executables = ET.SubElement(root, "DTS:Executables")
    
    
    return root, root_executables, origin_1_connection_ID, origin_2_connection_LIST, SqlServer_connection_LIST
    


def register_connections(root: ET.Element) -> tuple:
    """
    Registers the connections required for the SSIS package.

    Args:
        root (ET.Element): The root XML element of the SSIS package.

    Returns:
        tuple: A tuple containing the updated root XML element and connection lists for origin_1, origin_2, and SQL Server.
    """
    
    
    def register_connection(connections: ET.Element, Data_source: str, DB_User: str, dtsid: str, provider: str, password: str = None) -> str:
        """
        Registers a single connection manager in the SSIS package.

        Args:
            connections (ET.Element): The XML element for connection managers.
            Data_source (str): The data source for the connection.
            DB_User (str): The database user for the connection.
            dtsid (str): The unique ID for the connection.
            provider (str): The provider for the connection (Oracle or SQLServer).
            password (str, optional): The password for the connection. Defaults to None.

        Returns:
            str: The reference ID for the connection.
        """        
        if provider == "Oracle":
            conn_string = f"Data Source={Data_source};User ID={DB_User};Provider=OraOLEDB.Oracle.1;"
        elif provider == "SQLServer":
            conn_string = f"Data Source={Data_source};Initial Catalog={DB_User};Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False;"
        
        connection_ref_id = f"Package.ConnectionManagers[{Data_source}.{DB_User}]"
        
        conn_mgr = ET.SubElement(connections, "DTS:ConnectionManager", {
            "DTS:refId": connection_ref_id,
            "DTS:CreationName": "OLEDB",
            "DTS:DTSID": dtsid,
            "DTS:ObjectName": f"{Data_source}.{DB_User}"
        })
        conn_mgr_data = ET.SubElement(conn_mgr, "DTS:ObjectData")
        conn_mgr_details = ET.SubElement(conn_mgr_data, "DTS:ConnectionManager", {
            "DTS:ConnectRetryCount": "1",
            "DTS:ConnectRetryInterval": "5",
            "DTS:ConnectionString": conn_string
        })
        
        if password:
            password_elem = ET.SubElement(conn_mgr_details, "DTS:Password", {
                "DTS:Name": "Password",
                "Sensitive": "1",
                "Encrypted": "0"
            })
            password_elem.text = password
    
        return connection_ref_id


    connections = root.find("DTS:ConnectionManagers")
    if connections is None:
        connections = ET.SubElement(root, "DTS:ConnectionManagers")

    # Stablish Connection 1 --> origin_1
    origin_1_connection_unique_id = SSIS_Object.generate_unique_id()
    Data_source_conn1 = f"{ORIGIN_1_HOST_NAME}/{ORIGIN_1_SERVICE_NAME}"
    DB_User_conn1 = ORIGIN_1_USER
    DB_pass_conn1 = ORIGIN_1_PASSWORD
    
    origin_1_connection_ref_ID = register_connection(connections = connections, 
                        Data_source = Data_source_conn1, 
                        DB_User = DB_User_conn1, 
                        dtsid = origin_1_connection_unique_id, 
                        provider = "Oracle",
                        password=DB_pass_conn1)
    
    origin_1_connection_LIST = [origin_1_connection_ref_ID, origin_1_connection_unique_id]
    
    
    
    # # Stablish Connection 2 --> origin_2
    origin_2_connection_unique_id = SSIS_Object.generate_unique_id()
    Data_source_conn2 = f"{ORIGIN_2_HOST_NAME}/{ORIGIN_2_SERVICE_NAME}"
    DB_User_conn2 = ORIGIN_2_USER
    DB_pass_conn2 = ORIGIN_2_PASSWORD
    
    origin_2_connection_ref_ID = register_connection(connections = connections, 
                        Data_source = Data_source_conn2, 
                        DB_User = DB_User_conn2, 
                        dtsid = origin_2_connection_unique_id,
                        provider = "Oracle", 
                        password=DB_pass_conn2)
    
    origin_2_connection_LIST = [origin_2_connection_ref_ID, origin_2_connection_unique_id]
    
    
    
    # Stablish Connection 3 --> SQLServer
    SqlServer_connection_unique_id = SSIS_Object.generate_unique_id()
    Data_source_conn3 = SQL_SERVER_HOST_NAME
    DB_User_conn3 = DATA_BASE_DESTINO

    SqlServer_connection_ref_ID = register_connection(connections = connections, 
                        Data_source = Data_source_conn3, 
                        DB_User = DB_User_conn3, 
                        dtsid = SqlServer_connection_unique_id,
                        provider = "SQLServer")
    
    SqlServer_connection_LIST = [SqlServer_connection_ref_ID, SqlServer_connection_unique_id]
       
    return root, origin_1_connection_LIST, origin_2_connection_LIST, SqlServer_connection_LIST