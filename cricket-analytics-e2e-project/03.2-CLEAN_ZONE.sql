USE DATABASE DEMO_DB;

USE SCHEMA ALF_CLEAN;

USE WAREHOUSE LOAD_WH;

/*
    ALF_RAW: MATCH_RAW_TBL
    ALF_CLEAN: match_detail_clean
 */

-- EXTRACT PLAYERS 

-- VERSION 1 
SELECT INFO:match_type_number::INT AS match_type_number,
INFO:players,
INFO:teams
FROM 
DEMO_DB.ALF_RAW.MATCH_RAW_TBL
;


-- VERSION 2
SELECT INFO:match_type_number::INT AS match_type_number,
INFO:players,
INFO:teams
FROM 
DEMO_DB.ALF_RAW.MATCH_RAW_TBL
WHERE match_type_number = '4670'
;

