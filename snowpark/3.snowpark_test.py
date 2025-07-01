import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col


connection_parameters = {
    "user":'DEV_SSIS_INGEST_AU',
    "password":'PbIXsy$3yg7o=M[f',
    "account":'mckesson.canada-central.azure',
    "warehouse": "DEMO_WH",
    "database": "DEMO_DB",
    "schema": "PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()
session.sql("USE WAREHOUSE DEMO_WH").collect()


df_customer_info = session.table("DEMO_DB.ALF_DB_WH.QUALTRICS_SURVEY")
df_customer_filter = df_customer_info.filter(col("DEPARTMENT") == 'RBG')
df_customer_select = df_customer_info.select(col("SURVEY_ID"), col("EXECUTION_FLAG"), col('DEPARTMENT'))
df_customer_select.show()
df_customer_select.count()


df_customer_select.describe().sort("SURVEY_ID").show()


df_customer_info.describe().sort("SURVEY_ID").show()




