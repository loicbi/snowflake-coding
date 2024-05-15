USE DATABASE DEMO_DB;

USE SCHEMA ALF_CLEAN;

USE WAREHOUSE LOAD_WH;

-- RAW TABLE 

-- meta 
SELECT
META['created']::date as created,
META['data_version']::text as data_version,
META['revision']::number as revision
FROM DEMO_DB.ALF_RAW.MATCH_RAW_TBL;

-- info 
SELECT 
INFO:match_type::text as match_type,
INFO:match_type_number::number as match_type_number,
INFO:season::text as season,
INFO:overs::text as overs,
INFO:venue::text as venue

FROM DEMO_DB.ALF_RAW.MATCH_RAW_TBL;

-- CREATE TABLE match_detail_clean TO GET UNIQUE KEY FOR EXTRACTING DATA
create or replace transient table DEMO_DB.ALF_CLEAN.match_detail_clean as
select
    info:match_type_number::int as match_type_number, 
    info:event.name::text as event_name,
    case
    when 
        info:event.match_number::text is not null then info:event.match_number::text
    when 
        info:event.stage::text is not null then info:event.stage::text
    else
        'NA'
    end as match_stage,   
    info:dates[0]::date as event_date,
    date_part('year',info:dates[0]::date) as event_year,
    date_part('month',info:dates[0]::date) as event_month,
    date_part('day',info:dates[0]::date) as event_day,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue, 
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,
    case 
        when info:outcome.winner is not null then 'Result Declared'
        when info:outcome.result = 'tie' then 'Tie'
        when info:outcome.result = 'no result' then 'No Result'
        else info:outcome.result
    end as matach_result,
    case 
        when info:outcome.winner is not null then info:outcome.winner
        else 'NA'
    end as winner,   

    info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision,
    --
    STG_FILE_NAME ,
    STG_FILE_ROW_NUMBER,
    STG_FILE_HASHKEY,
    STG_FILE_MODIFIED_TS
    from 
    DEMO_DB.ALF_RAW.match_raw_tbl;
