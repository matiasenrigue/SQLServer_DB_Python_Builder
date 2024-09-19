import pandas as pd
from datetime import datetime

import os

from Utils.params import *
from Generate_SSIS_Package.SSIS_Full_Package import create_dtsx
from Generate_SQL_Code.Selects_from_Oracle import prepare_data_frame, create_dictionary_from_dataframe

from Generate_SQL_Code.Tables_Creation_STG import STG_tables_creation
from Generate_SQL_Code.Tables_Creation_ODS import ODS_tables_creation
from Generate_SQL_Code.Stored_Procedures_STG_to_ODS import Stored_Procedures_STG_to_ODS


def print_menu():
    print("\n1. Generate SSIS package")
    print("2. Generate STG tables")
    print("3. Generate ODS tables")
    print("4. Generate Stored Procedures STG to ODS\n")



def get_user_choice() -> int:
    """
    Prompts the user to select a number from the menu.

    Returns:
        int: The user's menu choice, a number from 1 to 4.
    """
    print_menu() 
    while True:
        try:
            choice = int(input("\nEnter a number from 1 to 4: "))
            if 1 <= choice <= 4:
                return choice
            else:
                print("Invalid number. Please enter a number from 1 to 4.")
        except ValueError:
            print("Invalid input. Please enter a valid number.")
            


def handle_ssis_creation(data_dict_path: str, output_folder: str) -> None:
    """
    Handles SSIS file creation for two origins.

    Args:
        data_dict_path (str): Path to the data dictionary file.
        output_folder (str): The folder where output files will be saved.
    """
    current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    output_path = os.path.join(output_folder, 'Select_Queries_Oracle.xlsx')
    df = prepare_data_frame(input_path_file=data_dict_path, output_path_file=output_path)
    data_dictionary = create_dictionary_from_dataframe(df) 

    for origin in [ORIGIN_1_NAME, ORIGIN_2_NAME]:
        dict_origin = {origin: data_dictionary[origin]}
        SSIS_output = os.path.join(output_folder, f"SSIS_{current_datetime}_{origin}.dtsx")
        create_dtsx(dict_origin, SSIS_output) 



def main():

    data_dict_path = os.path.join("data", DATA_DICT_FILE)
    output_folder = "output_folder"
    os.makedirs(output_folder, exist_ok=True)

    number = get_user_choice()

    if number == 1:
        handle_ssis_creation(data_dict_path, output_folder) 

    elif number == 2:
        output_path = os.path.join(output_folder,'STG_Tables_Creation.xlsx')
        STG_tables_creation(data_dict_path, output_path)      


    elif number == 3:
        output_path = os.path.join(output_folder,'ODS_Tables_Creation.xlsx')
        ODS_tables_creation(data_dict_path, output_path)
        

    elif number == 4:
        output_path = os.path.join(output_folder,'STG_to_ODS_SPs_Creation.xlsx')
        Stored_Procedures_STG_to_ODS(data_dict_path, output_path)
        


if __name__ == "__main__":
    main()