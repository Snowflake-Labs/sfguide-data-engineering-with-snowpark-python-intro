/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Snowpark Python
Script:       05_load_excel_files.sql
Author:       Jeremiah Hansen
Last Updated: 9/26/2023
-----------------------------------------------------------------------------*/

USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE SCHEMA HOL_DB.HOL_SCHEMA;


-- ----------------------------------------------------------------------------
-- Step 1: Inspect the Excel files
-- ----------------------------------------------------------------------------

-- Make sure the two Excel files show up in the stage
LIST @FROSTBYTE_RAW_STAGE/intro;

-- I've also included a copy of the Excel files in the /data folder of this repo
-- so you can look at the contents.


-- ----------------------------------------------------------------------------
-- Step 2: Create the stored procedure to load Excel files
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(file_path string, worksheet_name string, target_table string)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'openpyxl')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook
import pandas as pd
 
def main(session, file_path, worksheet_name, target_table):
 with SnowflakeFile.open(file_path, 'rb') as f:
     workbook = load_workbook(f)
     sheet = workbook.get_sheet_by_name(worksheet_name)
     data = sheet.values
 
     # Get the first line in file as a header line
     columns = next(data)[0:]
     # Create a DataFrame based on the second and subsequent lines of data
     df = pd.DataFrame(data, columns=columns)
 
     df2 = session.create_dataframe(df)
     df2.write.mode("overwrite").save_as_table(target_table)
 
 return True
$$;


-- ----------------------------------------------------------------------------
-- Step 3: Load the Excel data
-- ----------------------------------------------------------------------------

CALL LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@FROSTBYTE_RAW_STAGE, 'intro/order_detail.xlsx'), 'order_detail', 'ORDER_DETAIL');
CALL LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@FROSTBYTE_RAW_STAGE, 'intro/location.xlsx'), 'location', 'LOCATION');

DESCRIBE TABLE ORDER_DETAIL;
SELECT * FROM ORDER_DETAIL;

DESCRIBE TABLE LOCATION;
SELECT * FROM LOCATION;
