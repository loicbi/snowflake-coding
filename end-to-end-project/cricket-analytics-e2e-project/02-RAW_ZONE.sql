/* PART 3    --  LOAD DATA FROM STAGE TO RAW */
LIST @DEMO_DB.ALF_LAND.STG_LAND;

-- CREATE EXTERNAL TABLE MATCH_RAW_TBL
CREATE OR REPLACE  TRANSIENT TABLE DEMO_DB.ALF_RAW.MATCH_RAW_TBL(
    META OBJECT NOT NULL,
    INFO VARIANT NOT NULL,
    INNINGS ARRAY NOT NULL,
    STG_FILE_NAME TEXT NOT NULL,
    STG_FILE_ROW_NUMBER INT NOT NULL,
    STG_FILE_HASHKEY TEXT NOT NULL,
    STG_FILE_MODIFIED_TS TIMESTAMP NOT NULL

) 
COMMENT = 'This is a raw table to store all the json data file with root element extracted';
;

COPY INTO DEMO_DB.ALF_RAW.MATCH_RAW_TBL FROM (

SELECT 
T.$1:meta::VARIANT AS META
,T.$1:info::VARIANT AS INFO
,T.$1:innings::ARRAY AS INNINGS,
-- 
METADATA$FILENAME,
METADATA$FILE_ROW_NUMBER,
METADATA$FILE_CONTENT_KEY, 
METADATA$FILE_LAST_MODIFIED

FROM @DEMO_DB.ALF_LAND.STG_LAND/cricket/json 
(FILE_FORMAT => 'DEMO_DB.ALF_LAND.FF_LAND_JSON') T);

SELECT COUNT(*) FROM DEMO_DB.ALF_RAW.MATCH_RAW_TBL;
SELECT * FROM DEMO_DB.ALF_RAW.MATCH_RAW_TBL;

