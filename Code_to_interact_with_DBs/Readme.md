# Code to interact with DBs

## Note on Code Quality

These scripts are not as well-structured or optimized as they could be due to two main factors:

1. Time constraints during development limited the ability to refine and optimize the code.
2. These scripts are not directly related to the main project and were developed for separate, specific tasks.

As a result, they may not adhere to the same coding standards and practices as the rest of the project.

## Overview

This folder contains standalone scripts that automate interactions with different databases. These scripts are not integrated into the main project but serve as utilities for specific database operations.

## Contents

### Oracle

- `Get_Types_and_Lenght.py`: This script automates the retrieval of information about each field (column) of each table in an Oracle database. It's designed to populate the columns TYPE, SIZE, PRECISION, and SCALE in an Excel file with data dictionary information.

### SQL Server

- `Create_Tables_or_SPs.py`: This script automates the execution of the over 350 queries that we would need to execute to create tables or stored procedures in SQL Server, for each layer of the DB. It maintains a log of the results, tracking which operations were successful and which encountered issues.

## Usage

These scripts are meant to be run independently on a remote computer with access to the respective databases. They are not part of the main package and require specific setup and permissions to interact with the target databases.

## Caution

Due to the nature of these scripts and their ability to make significant changes to databases, use them with caution. Always ensure you have proper backups and permissions before running these scripts in any production environment.