/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Snowpark Python
Script:       06_load_daily_city_metrics.sql
Author:       Jeremiah Hansen
Last Updated: 9/26/2023
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Snowpark Python programmability
-- SNOWFLAKE ADVANTAGE: Snowpark DataFrame API


USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE SCHEMA HOL_DB.HOL_SCHEMA;


-- ----------------------------------------------------------------------------
-- Step 1: Create the stored procedure to load DAILY_CITY_METRICS
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE LOAD_DAILY_CITY_METRICS_SP()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def main(session: Session) -> str:
    schema_name = "HOL_SCHEMA"
    table_name = "DAILY_CITY_METRICS"

    # Define the tables
    order_detail = session.table("ORDER_DETAIL")
    history_day = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY")
    location = session.table("LOCATION")

    # Join the tables
    order_detail = order_detail.join(location, order_detail['LOCATION_ID'] == location['LOCATION_ID'])
    order_detail = order_detail.join(history_day, (F.builtin("DATE")(order_detail['ORDER_TS']) == history_day['DATE_VALID_STD']) & (location['ISO_COUNTRY_CODE'] == history_day['COUNTRY']) & (location['CITY'] == history_day['CITY_NAME']))

    # Aggregate the data
    final_agg = order_detail.group_by(F.col('DATE_VALID_STD'), F.col('CITY_NAME'), F.col('ISO_COUNTRY_CODE')) \
                        .agg( \
                            F.sum('PRICE').alias('DAILY_SALES_SUM'), \
                            F.avg('AVG_TEMPERATURE_AIR_2M_F').alias("AVG_TEMPERATURE_F"), \
                            F.avg("TOT_PRECIPITATION_IN").alias("AVG_PRECIPITATION_IN"), \
                        ) \
                        .select(F.col("DATE_VALID_STD").alias("DATE"), F.col("CITY_NAME"), F.col("ISO_COUNTRY_CODE").alias("COUNTRY_DESC"), \
                            F.builtin("ZEROIFNULL")(F.col("DAILY_SALES_SUM")).alias("DAILY_SALES"), \
                            F.round(F.col("AVG_TEMPERATURE_F"), 2).alias("AVG_TEMPERATURE_FAHRENHEIT"), \
                            F.round(F.col("AVG_PRECIPITATION_IN"), 2).alias("AVG_PRECIPITATION_INCHES"), \
                        )

    # If the table doesn't exist then create it
    if not table_exists(session, schema=schema_name, name=table_name):
        final_agg.write.mode("overwrite").save_as_table(table_name)

        return f"Successfully created {table_name}"
    # Otherwise update it
    else:
        cols_to_update = {c: final_agg[c] for c in final_agg.schema.names}

        dcm = session.table(f"{schema_name}.{table_name}")
        dcm.merge(final_agg, (dcm['DATE'] == final_agg['DATE']) & (dcm['CITY_NAME'] == final_agg['CITY_NAME']) & (dcm['COUNTRY_DESC'] == final_agg['COUNTRY_DESC']), \
                            [F.when_matched().update(cols_to_update), F.when_not_matched().insert(cols_to_update)])

        return f"Successfully updated {table_name}"
$$;


-- ----------------------------------------------------------------------------
-- Step 2: Load the DAILY_CITY_METRICS table
-- ----------------------------------------------------------------------------

CALL LOAD_DAILY_CITY_METRICS_SP();

SELECT * FROM DAILY_CITY_METRICS;
