import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col


connection_parameters = {
    "user":'andreloicfreddy.seka@mckesson.ca',
    "authenticator":'externalbrowser',
    # "user":'DEV_SSIS_INGEST_AU',
    # "password":'PbIXsy$3yg7o=M[f',
    "account":'mckesson.canada-central.azure',
    "warehouse": "DEMO_WH",
    "database": "DEMO_DB",
    "schema": "PUBLIC"
}


test_session = Session.builder.configs(connection_parameters).create()

print(test_session.sql("select current_warehouse(), current_database(), current_schema()").collect())

session = Session.builder.configs(connection_parameters).create()