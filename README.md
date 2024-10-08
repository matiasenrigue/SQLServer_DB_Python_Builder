# SQL Server DB Python Builder

## Context
This project is a Python automation I built while working for a data-specialized consultancy in Spain. My task was to build the new structure of the database for one of the most prestigious universities in the country. The database had more than 350 tables that needed to be created, one by one, on multiple occasions:

1. In SSIS to send the data from Oracle to SQL Server
2. In STG (first layer of the new DB)
3. In ODS (second layer of the DB)
4. In the Stored Procedures (sending the data from STG to ODS while applying transformations)

This Python project significantly reduced the work time for this project, as creating these tables is a tedious and repetitive task (which was being done by hand previously). The automation was particularly effective when integrated with SSIS, where it vastly outperformed the original manual process. After just a few days of development, the code was able to execute the same work in seconds, cutting down the estimated time to a fraction. To put it in perspective: the project was completed seven times faster than originally expected, reducing the work time by 86%.

While most of the code has been translated to English, some parts remain in Spanish for ease of use by current users and due to the complexity of translating everything.

* *For confidentiality reasons, I can't provide the name of the university or the exact details of the time savings, but both can be fact-checked if required during the recruitment process.*

## Project Overview
The project facilitates the migration and structuring of data from Oracle databases to SQL Server, creating necessary SSIS packages, SQL statements, and stored procedures.

## Technical Challenges and Achievements

This project overcame several significant technical challenges:

1. Complex SSIS Package Generation: 
   - Programmatically created a SSIS package for 350+ tables, each containing hundreds of lines of XML code.
   - Automated a process that typically requires weeks of manual effort, reducing it to seconds of execution time.
   - Ensured precise XML structure adherence, crucial for SSIS package functionality.
   - [More on SSIS complexity and challenges](./Generate_SSIS_Package#what-is-an-ssis-package)

2. Multi-layered Database Structure:
   - Managed the creation of tables across multiple database layers (STG, ODS) while maintaining data integrity and consistency.
   - Implemented complex transformations between layers, accommodating varying data types and structures.

3. Large-Scale Data Migration:
   - Facilitated the transfer of data from Oracle to SQL Server for a massive database with over 350 tables.
   - Optimized data flow to handle large volumes of data efficiently.

4. Dynamic SQL Generation:
   - Created SQL statements and stored procedures dynamically based on the data dictionary, ensuring adaptability to changing database schemas.
   - Implemented logic to handle different data types, constraints, and transformations across hundreds of tables.

5. Cross-Database Compatibility:
   - Managed the intricacies of translating Oracle-specific features and data types to their SQL Server equivalents.
   - Ensured data integrity and consistency across different database platforms.

6. Automation of Repetitive Tasks:
   - Dramatically reduced human error and inconsistencies by automating the creation of database objects across multiple layers.
   - Implemented a solution that can be easily rerun or modified for future database changes or migrations.

7. Performance Optimization:
   - Designed the code to handle the creation of hundreds of tables and associated objects efficiently.
   - Reduced the overall project timeline by 86%, completing work 7 times faster than the original manual estimation.

These technical achievements demonstrate the project's ability to handle complex, large-scale database migrations and structures with high efficiency and accuracy. The automation not only saved significant time but also ensured consistency and reduced the potential for human error in a critical data infrastructure project for a prestigious university.

### Database Organization
1. Original data is stored in two Oracle databases: ORIGIN_1 and ORIGIN_2.
2. Data is extracted from Oracle and sent to SQL Server's STG (Staging) layer using SSIS (SQL Server Integration Services) packages generated by this code.
3. The code creates SQL statements to set up SQL Server layers: STG (1st layer) and ODS (2nd layer).
4. It also generates Stored Procedures to move data from STG to ODS while appliying transformations.

## How It Works
The code generates all information based on a data dictionary file (expected to be in the `data` folder, not included in the repository).

### Data Dictionary Structure
- One row per column (or field)
- Multiple columns per table
- Column names include:

```python
STG_TABLAS = 'STG TABLE NAME'
STG_CAMPOS = 'STG FIELD NAME' 
STG_TIPO = 'STG TYPE'
STG_SIZE = 'STG SIZE'
STG_PRECISION = 'STG PRECISION'
STG_SCALE = 'STG SCALE'

ODS_TABLAS = 'ODS TABLE NAME'
ODS_CAMPOS = 'ODS FIELD NAME'
ODS_TIPO = 'ODS TYPE'
ODS_SIZE = 'ODS SIZE'
ODS_PRECISION = 'ODS PRECISION'
ODS_SCALE = 'ODS SCALE'

COLUMNA_ORIGEN = "ORIGIN" (From Oracle: ORIGIN_1 or ORIGIN_2)
```

Note: If ODS information is empty, the code assumes the columns will have the same values in STG and ODS.

### Additional Information
There's another file in the data folder (`info_pks`) which provides:
- Primary keys
- Whether a table is a dimension or fact (affects naming and some queries)
- Temporal dimension requirements for Stored Procedures


## Areas for Potential Improvement

While the current code is functional and efficient, there are areas that could be enhanced if additional development time were allocated. However, it's important to note that these improvements would primarily affect code maintainability rather than functionality or performance, which made it impossible to justify additional development time to management.

1. **Data Import Optimization**: 
   - Create a common function to import and clean the data dict DataFrame.
   - This would reduce code repetition across multiple modules.

2. **Consistent Naming Convention**:
   - Extend the use of the Table Class to 'SQL Server' code sections.
   - This would ensure consistent naming across the entire project, enhancing readability and maintainability.

3. **Enhanced Error Handling**:
   - Implement more robust error handling and logging mechanisms.
   - This would facilitate easier debugging and maintenance in the long run.


## How to Use

1. Clone the repository:
   ```
    git clone https://github.com/matiasenrigue/SQLServer_DB_Python_Builder.git
    cd SQLServer_DB_Python_Builder
   ```

2. Install the package:
   You can install the package using the provided makefile:
   ```
   make install
   ```
   This will install the package in editable mode (-e flag) using pip.


3. Ensure you have all the required dependencies:
   The package will automatically install the required dependencies listed in the `requirements.txt` file.

4. Prepare your data:
   Place your data dictionary file in the `data` folder. Ensure it follows the structure described in the "Data Dictionary Structure" section of this README. (same for the info_pks excel)

5. Run the main script:
   ```
   python main.py
   ```

6. Check the output:
   The script will generate SSIS packages, SQL statements, and stored procedures based on your input data. Check the `output_folder` for the outputs.


---

# Instrucciones de uso para empleados de SDG:

## Preparación para su uso:

1. Actualizar el archivo `params.py`:
   - Modificar los parámetros de conexión para ORIGIN_1, ORIGIN_2 y SQL Server según sea necesario.
   - Si se ha modificado la estructura del diccionario de datos en Excel, actualizar los nombres de las columnas correspondientes.

2. Preparar el diccionario de datos:
   - Colocar el archivo Excel del diccionario de datos en la carpeta `data`.
   - Asegurarse de que el nombre del archivo coincida con el especificado en `params.py` (por defecto, `DATA_DICT_FILE = "Data_Dict.xlsx"`).

3. Verificar el archivo `info_pks`:
   - Asegurarse de que este archivo esté actualizado con la información correcta sobre claves primarias, tipos de tablas (DIM o FACT) y requisitos de dimensión temporal.


## Notas importantes:

- Asegurarse de tener los permisos necesarios para acceder a las bases de datos Oracle (ORIGIN_1 y ORIGIN_2) y a la instancia de SQL Server de destino.
- En caso de problemas o errores, verificar primero la configuración en `params.py` y la estructura del diccionario de datos.

Si tienes dudas o requieres asistencia adicional, puedes contactarme por [LinkedIn](https://www.linkedin.com/in/matiasenrigue)


