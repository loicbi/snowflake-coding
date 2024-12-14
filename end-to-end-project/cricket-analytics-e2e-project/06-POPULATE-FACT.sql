USE ROLE DEV_ENT_DW_ENGINEER_FR;

USE WAREHOUSE LOAD_WH;

USE SCHEMA DEMO_DB.ALF_CONSUMPTION;


-- V1 
SELECT M.MATCH_TYPE_NUMBER AS MATCH_ID, DD.DATE_ID, 0 AS REFEREE_ID
FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN M 
JOIN DEMO_DB.ALF_CONSUMPTION.DATE_DIM DD ON DD.FULL_DT = M.EVENT_DATE
WHERE M.MATCH_TYPE_NUMBER = 4686;

-- V2 WITH TEAM 

SELECT M.MATCH_TYPE_NUMBER AS MATCH_ID, 
DD.DATE_ID, 
0 AS REFEREE_ID,
FTD.TEAM_NAME,
STD.TEAM_NAME
FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN M 
JOIN DEMO_DB.ALF_CONSUMPTION.DATE_DIM DD ON DD.FULL_DT = M.EVENT_DATE
JOIN DEMO_DB.ALF_CONSUMPTION.TEAM_DIM FTD ON M.FIRST_TEAM = FTD.TEAM_NAME
JOIN DEMO_DB.ALF_CONSUMPTION.TEAM_DIM STD ON M.SECOND_TEAM = STD.TEAM_NAME
WHERE M.MATCH_TYPE_NUMBER = 4686;



-- V2 WITH MATCH TYPE  

SELECT M.MATCH_TYPE_NUMBER AS MATCH_ID, 
DD.DATE_ID, 
0 AS REFEREE_ID,
FTD.TEAM_NAME,
STD.TEAM_NAME,
MTD.MATCH_TYPE_ID
-- ,VD.VENUE_ID
FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN M 
JOIN DEMO_DB.ALF_CONSUMPTION.DATE_DIM DD ON DD.FULL_DT = M.EVENT_DATE
JOIN DEMO_DB.ALF_CONSUMPTION.TEAM_DIM FTD ON M.FIRST_TEAM = FTD.TEAM_NAME
JOIN DEMO_DB.ALF_CONSUMPTION.TEAM_DIM STD ON M.SECOND_TEAM = STD.TEAM_NAME
JOIN DEMO_DB.ALF_CONSUMPTION.MATCH_TYPE_DIM MTD ON M.MATCH_TYPE = MTD.MATCH_TYPE
-- JOIN DEMO_DB.ALF_CONSUMPTION.VENUE_DIM VD ON M.VENUE = VD.VENUE_NAME
WHERE M.MATCH_TYPE_NUMBER = 4686;





insert into DEMO_DB.ALF_CONSUMPTION.MATCH_FACT 
select 
    m.match_type_number as match_id,
    dd.date_id as date_id,
    0 as referee_id,
    ftd.team_id as first_team_id,
    std.team_id as second_team_id,
    mtd.match_type_id as match_type_id,
    vd.venue_id as venue_id,
    50 as total_overs,
    6 as balls_per_overs,
    max(case when d.team_name = m.first_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_A,
    0 fours_by_team_a,
    0 sixes_by_team_a,
    (sum(case when d.team_name = m.first_team then  d.runs else 0 end ) + sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_A,
    sum(case when d.team_name = m.first_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_a,    
    
    max(case when d.team_name = m.second_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_B,
    0 fours_by_team_b,
    0 sixes_by_team_b,
    (sum(case when d.team_name = m.second_team then  d.runs else 0 end ) + sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_B,
    sum(case when d.team_name = m.second_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_b,
    tw.team_id as toss_winner_team_id,
    m.toss_decision as toss_decision,
    m.matach_result as matach_result,
    mw.team_id as winner_team_id
     
from 
    DEMO_DB.ALF_CLEAN.match_detail_clean m
    join date_dim dd on m.event_date = dd.full_dt
    join team_dim ftd on m.first_team = ftd.team_name 
    join team_dim std on m.second_team = std.team_name 
    join match_type_dim mtd on m.match_type = mtd.match_type
    join venue_dim vd on m.venue = vd.venue_name and m.city = vd.city
    join DEMO_DB.ALF_CLEAN.delivery_clean_tbl d  on d.match_type_number = m.match_type_number 
    join team_dim tw on m.toss_winner = tw.team_name 
    join team_dim mw on m.winner= mw.team_name 
    --where m.match_type_number = 4686
    group by
        m.match_type_number,
        date_id,
        referee_id,
        first_team_id,
        second_team_id,
        match_type_id,
        venue_id,
        total_overs,
        toss_winner_team_id,
        toss_decision,
        matach_result,
        winner_team_id
        ;