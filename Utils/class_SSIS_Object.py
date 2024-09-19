import uuid
import xml.etree.ElementTree as ET


class SSIS_Object:
    """
    Class to manage the creation and management of SSIS objects in an XML file.
    """
    
    existing_ids = set()
    
    connection_info_Origin_1 : list = None
    connection_info_Origin_2 : list = None
    connection_info_SqlServer : list = None
    
    
    def __init__(self, parent_object:list):
        
        self.parent_executable = parent_object[0]
        self.parent_reference_path = parent_object[1]



    @classmethod
    def generate_unique_id(cls) -> str:
        """
        Class method allows us to call it without having to create an instance of the object.
        Thanks to this method we can keep track of all the IDs generated in the class.

        Generates a unique ID for an SSIS object.

        Returns:
            str: Generated unique ID.
        """
        
        new_id = str(uuid.uuid4()).upper()
        while new_id in cls.existing_ids:
            new_id = str(uuid.uuid4()).upper()
        cls.existing_ids.add(new_id)
        return new_id
    
    
    
    def create_container(self, container_name: str, ruta_reference: str)-> tuple[ET.Element, ET.Element]:
        """
        Creates a sequence container in the SSIS XML.

        Args:
            container_name (str): Name of the container.
            ruta_reference (str): Reference path of the container.

        Returns:
            Tuple[ET.Element, ET.Element]: Sequence container and its executables element.
        """
        
        seq_id = SSIS_Object.generate_unique_id()
        
        seq_container = ET.SubElement(self.parent_executable, "DTS:Executable", {
            "DTS:refId": ruta_reference,
            "DTS:CreationName": "STOCK:SEQUENCE",
            "DTS:Description": "Sequence Container",
            "DTS:DTSID": f"{{{seq_id}}}",
            "DTS:ExecutableType": "STOCK:SEQUENCE",
            "DTS:LocaleID": "-1",
            "DTS:ObjectName": container_name
        })
        
        seq_variables = ET.SubElement(seq_container, "DTS:Variables")
        seq_executables = ET.SubElement(seq_container, "DTS:Executables")
        
        return seq_container, seq_executables
    
    
    
    def create_upper_level_container(self, level: int, origin_DB: str = None)-> tuple[ET.Element, str]:
        """
        Creates a higher-level container in the SSIS XML.

        Args:
            level (int): Level of the container (1 or 2).
            origin_DB (Optional[str]): Source database, if the level is 2.

        Returns:
            Tuple[ET.Element, str]: Executable element of the container and its reference path.
        """
            
        level1_main_container_name = "SEQ | BIG"
        level1_main_container_ruta_referencia = f"Package\\{level1_main_container_name}"
        
        level2_container_name_origin_DB = f"SEQ | {origin_DB}"
        level2_container_ruta_reference_origin_DB = f"{level1_main_container_ruta_referencia}\\{level2_container_name_origin_DB}"
        
        
        if level == 1: # Container padre proyecto --> Al pasar por aquí se registra valor variable
            container_name = level1_main_container_name
            ruta_reference = level1_main_container_ruta_referencia
        
        if level == 2: # Container de Origenes
            container_name = level2_container_name_origin_DB
            ruta_reference = level2_container_ruta_reference_origin_DB
        
        seq_container, seq_executables = self.create_container(container_name, ruta_reference)
        
        return seq_executables, level2_container_ruta_reference_origin_DB        
    