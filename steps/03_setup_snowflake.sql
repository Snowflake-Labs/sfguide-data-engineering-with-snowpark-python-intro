/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Snowpark Python
Script:       03_setup_snowflake.sql
Author:       Jeremiah Hansen
Last Updated: 9/26/2023
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Visual Studio Code Snowflake native extension (Git integration)


-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------

-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)


-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects (ACCOUNTADMIN part)
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE HOL_ROLE;
GRANT ROLE HOL_ROLE TO ROLE SYSADMIN;
GRANT ROLE HOL_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE HOL_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE HOL_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE HOL_ROLE;

-- Databases
CREATE OR REPLACE DATABASE HOL_DB;
GRANT OWNERSHIP ON DATABASE HOL_DB TO ROLE HOL_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE HOL_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE HOL_WH TO ROLE HOL_ROLE;


-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

-- Schemas
CREATE OR REPLACE SCHEMA HOL_SCHEMA;

-- External Frostbyte objects
USE SCHEMA HOL_SCHEMA;
CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
;
