# Utils

## Overview

The `Utils` folder contains utility classes and functions that are used across the project for SQL Server database and SSIS package generation. These utilities provide common functionality and help maintain consistency throughout the codebase.

## Files in this folder

1. `Utils.py`
2. `class_SSIS_Object.py`
3. `class_Table.py`
4. `params.py`

## File Descriptions

### Utils.py

This file contains various utility functions used throughout the project.


### class_SSIS_Object.py

Defines the `SSIS_Object` class, which is a base class for SSIS-related objects. It includes methods for:

- Generating unique IDs
- Creating containers in the SSIS package
- Managing connection information


### class_Table.py

Defines the `Table` class, which extends `SSIS_Object`. This class represents a table in the SSIS context and includes methods for:

- Setting up connections
- Setting table object information (names, paths, etc.)
- Creating lower-level containers for individual tables


### params.py

Contains configuration parameters and constants used throughout the project, including:

- Database connection information
- Column names for data dictionaries
- File paths

## Usage

These utility files are imported and used by various scripts in the project. They provide essential functionality for data processing, SSIS package creation, and maintaining consistent naming conventions across the project.

## Note

When modifying these utilities, be mindful of their usage across the project. Changes here can have wide-reaching effects, so thorough testing is recommended after any modifications.