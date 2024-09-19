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




def main():

    current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    data_dict_path = os.path.join("data", DATA_DICT_FILE)
    
    output_folder = "output_folder"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    print_menu()
    while True:
        try:
            number = int(input("\nEnter a number from 1 to 4: "))
            break
        except:
            print("Invalid number. Please enter a number from 1 to 4.")

                
    if number == 1:

        output_path = os.path.join(output_folder,'Select_Queries_Oracle.xlsx')
        df = prepare_data_frame(
                            input_path_file= data_dict_path, 
                            output_path_file= output_path
                            )

        data_dictionary = create_dictionary_from_dataframe(df)

        # Process both origins separetly separately
        dict_origin_1 = {ORIGIN_1_NAME: data_dictionary[ORIGIN_1_NAME]}
        dict_origin_2 = {ORIGIN_2_NAME: data_dictionary[ORIGIN_2_NAME]}


        SSIS_output_origin_1 = os.path.join(output_folder, f"SSIS_{current_datetime}_{ORIGIN_1_NAME}.dtsx")
        create_dtsx(dict_origin_1, SSIS_output_origin_1)

        SSIS_output_origin_2 = os.path.join(output_folder, f"SSIS_{current_datetime}_{ORIGIN_2_NAME}.dtsx")
        create_dtsx(dict_origin_2, SSIS_output_origin_2)


    elif number == 2:
        output_path = os.path.join(output_folder,'STG_Tables_Creation.xlsx')
        STG_tables_creation(data_dict_path, output_path)      


    elif number == 3:
        output_path = os.path.join(output_folder,'ODS_Tables_Creation.xlsx')
        ODS_tables_creation(data_dict_path, output_path)
        

    elif number == 4:
        output_path = os.path.join(output_folder,'STG_to_ODS_SPs_Creation.xlsx')
        Stored_Procedures_STG_to_ODS(data_dict_path, output_path)
        
    
    else:
        print("Invalid number. Please enter a number from 1 to 4.")
        main()


if __name__ == "__main__":
    main()