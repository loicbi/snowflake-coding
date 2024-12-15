-- CREATE DEMO_DB 

/* SCHEMA */


-- STAGE SCHEMA : ALF_STAGE_SCH
CREATE SCHEMA IF NOT EXISTS  DEMO_DB.ALF_STAGE_SCH;
-- CLEAN SCHEMA : CLEAN_SCH
CREATE SCHEMA IF NOT EXISTS DEMO_DB.ALF_CLEAN_SCH;
-- CONSUPTION SCHEMA : CONSUMPTION_SCH
CREATE SCHEMA IF NOT EXISTS DEMO_DB.ALF_CONSUMPTION_SCH; -- fact & dimension
-- DASHBOARD SCHEMA : COMMON
CREATE SCHEMA IF NOT EXISTS DEMO_DB.ALF_COMMON; -- for file formats sequence object etc




/* FILE FORMAT  */

CREATE OR REPLACE FILE FORMAT DEMO_DB.ALF_STAGE_SCH.CSV_FILE_FORMAT
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('\\n')
  empty_field_as_null = true
  field_optionally_enclosed_by = '\042'
  record_delimiter = '\n'
  compression = auto;


/* STAGE  */
CREATE STAGE  
    DEMO_DB.ALF_STAGE_SCH.CSV_STG 
    directory = (enable = true)
    comment = 'This is snowflake internal stage';

LIST @DEMO_DB.ALF_STAGE_SCH.CSV_STG/delta;


/* TAG */
CREATE OR REPLACE 
    TAG DEMO_DB.ALF_COMMON.PII_POLICY_TAG
    ALLOWED_VALUES 'PII', 'PRICE', 'SENSITIVE', 'EMAIL'
    COMMENT = 'This is PII policy tag object';


/* MASKING POLICY  https://data-engineering-simplified.medium.com/column-level-security-in-snowflake-5b2f8b199654  */
CREATE OR REPLACE MASKING POLICY 
    DEMO_DB.ALF_COMMON.PII_MASKING_POLICY
    AS (PII_TEXT STRING)
    RETURN STRING 
CREATE OR REPLACE MASKING POLICY 
    DEMO_DB.ALF_COMMON.PII_MASKING_POLICY as (PII_TEXT string) -- parameter
    returns string ->
    case
        when current_role() in ('SYSADMIN')
            then PII_TEXT
        when current_role() in ('USERADMIN') then regexp_replace(PII_TEXT,substring(PII_TEXT,1,15),'xxxx-xxxx-xxxx-')
        else '***PII***'
end;




