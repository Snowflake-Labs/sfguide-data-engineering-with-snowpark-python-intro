from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def main(session: Session) -> str:
    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()
    schema_name = "HOL_SCHEMA"
    table_name = "DAILY_CITY_METRICS"

    # Define the tables
    order_detail = session.table("RAW_POS.ORDER_DETAIL")
    order_header = session.table("RAW_POS.ORDER_HEADER")
    history_day = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY")
    location = session.table("RAW_POS.LOCATION")

    # Join the tables
    orders = order_header.join(order_detail, order_header['ORDER_ID'] == order_detail['ORDER_ID'])
    orders = orders.join(location, orders['LOCATION_ID'] == location['LOCATION_ID'])
    order_detail = orders.join(history_day, (F.builtin("DATE")(order_header['ORDER_TS']) == history_day['DATE_VALID_STD']) & (orders['ISO_COUNTRY_CODE'] == history_day['COUNTRY']) & (orders['CITY'] == history_day['CITY_NAME']))

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
        _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL').collect()
        return f"Successfully created {table_name}"
    # Otherwise update it
    else:
        cols_to_update = {c: final_agg[c] for c in final_agg.schema.names}

        dcm = session.table(f"{schema_name}.{table_name}")
        dcm.merge(final_agg, (dcm['DATE'] == final_agg['DATE']) & (dcm['CITY_NAME'] == final_agg['CITY_NAME']) & (dcm['COUNTRY_DESC'] == final_agg['COUNTRY_DESC']), \
                            [F.when_matched().update(cols_to_update), F.when_not_matched().insert(cols_to_update)])
        _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL').collect()
        return f"Successfully updated {table_name}"
    
# For local debugging
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        main(session)