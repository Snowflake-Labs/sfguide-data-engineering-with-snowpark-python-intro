/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Snowpark Python
Script:       08_teardown.sql
Author:       Jeremiah Hansen
Last Updated: 9/26/2023
-----------------------------------------------------------------------------*/


USE ROLE ACCOUNTADMIN;

DROP DATABASE HOL_DB;
DROP WAREHOUSE HOL_WH;
DROP ROLE HOL_ROLE;

-- Drop the weather share
DROP DATABASE FROSTBYTE_WEATHERSOURCE;

DROP INTEGRATION GIT_API_INTEGRATION;
DROP DATABASE GIT_REPO;
DROP ROLE SECRETS_ADMIN;
