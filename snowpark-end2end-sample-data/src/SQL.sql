-- STEP 02 - CREATE SCHEMA 

SELECT CURRENT_WAREHOUSE();

create schema if not exists DEMO_DB.ALF_SOURCE; -- will have source stage etc
create schema if not exists DEMO_DB.ALF_CURATED; -- data curation and de-duplication
create schema if not exists DEMO_DB.ALF_consumption; -- fact & dimension
create schema if not exists DEMO_DB.ALF_audit; -- to capture all audit records
create schema if not exists DEMO_DB.ALF_common; -- for file formats sequence object etc


-- STEP 03 - CREATE INTERNAL STAGE IN SOURCE SCHEMA 
CREATE STAGE IF NOT EXISTS DEMO_DB.ALF_SOURCE.MY_INTERNAL_STAGE;
DESC STAGE DEMO_DB.ALF_SOURCE.MY_INTERNAL_STAGE;


-- LIST FILE IN STAGE 
LS @DEMO_DB.ALF_SOURCE.MY_INTERNAL_STAGE;



-- STEP 04 - LOAD ORDER FILES TO SATGE LOCATION

-- you can use vs code or command or script python  
-- following put command can be executed
/*
-- csv example
put file:///tmp/sales/source=IN/format=csv/date=2022-02-22/order-20220222.csv @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/date=2022-02-22 auto_compress=False overwrite=True, parallel=3 ;
put file:///tmp/sales/source=IN/format=csv/date=2021-04-26/order-20210426.csv @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/date=2021-04-26 auto_compress=False overwrite=True, parallel=3 ;

-- json example
put file:///tmp/sales/source=FR/format=json/date=2022-02-22/order-20220222.json @sales_dwh.source.my_internal_stg/sales/source=FR/format=json/date=2022-02-22 auto_compress=False overwrite=True, parallel=3 ;
put file:///tmp/sales/source=FR/format=json/date=2021-04-26/order-20210426.json @sales_dwh.source.my_internal_stg/sales/source=FR/format=json/date=2021-04-26 auto_compress=False overwrite=True, parallel=3 ;

-- parquet example
put file:///tmp/sales/source=US/format=parquet/date=2022-02-22/order-20220222.snappy.parquet @sales_dwh.source.my_internal_stg/sales/source=US/format=parquet/date=2022-02-22 auto_compress=False overwrite=True, parallel=3 ;
put file:///tmp/sales/source=US/format=parquet/date=2021-04-26/order-20210426.snappy.parquet @sales_dwh.source.my_internal_stg/sales/source=US/format=parquet/date=2021-04-26 auto_compress=False overwrite=True, parallel=3 ;
*/


