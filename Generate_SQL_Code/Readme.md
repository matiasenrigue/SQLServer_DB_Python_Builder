# Generate_SQL_CODE

**Note**: This section of the code is not as well-structured or optimized as it could be due to time constraints during development. While functional, it may benefit from refactoring and improvements in the future.

See: 'Areas for Potential Improvement' section of main readme

## Overview

The `Generate_SQL_CODE` folder contains scripts for generating SQL code for various database operations. These scripts are part of a larger project for migrating data from Oracle to SQL Server and setting up the necessary database structures.

## Files in this folder

1. `Getting_Data_from_Oracle.py`
2. `Stored_Procedures_STG_to_ODS.py`
3. `Tables_Creation_ODS.py`
4. `Tables_Creation_STG.py`


## File Descriptions

### Getting_Data_from_Oracle.py

This script generates SELECT statements to extract data from Oracle databases. These statements are used in the SSIS packages to move data from Oracle to SQL Server.


### Stored_Procedures_STG_to_ODS.py

Generates SQL Server stored procedures to move data from the STG (Staging) layer to the ODS (Operational Data Store) layer.


### Tables_Creation_ODS.py

Generates SQL code to create tables in the ODS database.

### Tables_Creation_STG.py

Generates SQL code to create tables in the STG (Staging) database.

