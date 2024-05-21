USE DATABASE DEMO_DB;

USE SCHEMA ALF_CONSUMPTION;

USE WAREHOUSE LOAD_WH;
-- DATE_DIM 
CREATE OR REPLACE TABLE DEMO_DB.ALF_CONSUMPTION.DATE_DIM(
    DATE_ID INT PRIMARY KEY AUTOINCREMENT,
    FULL_DT DATE,
    DAY INT,
    MONTH INT,
    YEAR INT,
    QUARTER INT,
    DAYOFWEEK INT,
    DAYOFMONTH INT,
    DAYOFYEAR INT,
    DAYOFWEEKNAME VARCHAR(3), /* TO STORE DAY e.g.: 'Mon - Tue' */
    ISWEEKEND BOOLEAN /* to indicate if it is a weekend (True/False Sat/Sun falls under weekends) */
);

-- match referees , reserve_umpires, tv_umpires, field_umpires 
CREATE OR REPLACE table DEMO_DB.ALF_CONSUMPTION.REFEREE_DIM (
    REFEREE_ID INT PRIMARY KEY AUTOINCREMENT,
    REFEREE_NAME TEXT NOT NULL,
    REFEREE_TYPE TEXT NOT NULL
);

-- India, Australia 
CREATE OR REPLACE TABLE DEMO_DB.ALF_CONSUMPTION.TEAM_DIM(
    TEAM_ID INT PRIMARY KEY AUTOINCREMENT,
    TEAM_NAME TEXT NOT NULL
);

-- PLAYER 
CREATE OR REPLACE TABLE  DEMO_DB.ALF_CONSUMPTION.PLAYER_DIM(
    PLAYER_ID INT PRIMARY KEY AUTOINCREMENT,
    TEAM_ID INT NOT NULL,
    PLAYER_NAME TEXT NOT NULL
);
    /* ALTER  PLAYER_DIM */
    ALTER TABLE DEMO_DB.ALF_CONSUMPTION.PLAYER_DIM ADD CONSTRAINT
    FK_TEAM_PLAYER_ID FOREIGN KEY (TEAM_ID) REFERENCES DEMO_DB.ALF_CONSUMPTION.TEAM_DIM (TEAM_ID);
    

-- venue_dim 
CREATE OR REPLACE TABLE DEMO_DB.ALF_CONSUMPTION.VENUE_DIM(
    VENUE_ID INT PRIMARY KEY,
    VENUE_NAME TEXT NOT NULL,
    CITY TEXT NOT NULL,
    STATE TEXT,
    COUNTRY TEXT,
    CONTINENT TEXT,
    END_NAMES TEXT,
    CAPACITY NUMBER,
    PITCH TEXT,
    FLOOD_LIGHT BOOLEAN,
    ESTABLISHED_DT DATE,
    PLAYING_AREA TEXT,
    OTHER_SPORTS TEXT,
    CURATOR TEXT,
    LATTITUDE NUMBER(10,6),
    LONGITUDE NUMBER(10,6)
);


-- MATCH_TYPE_DIM 
CREATE OR REPLACE TABLE DEMO_DB.ALF_CONSUMPTION.MATCH_TYPE_DIM(
    MATCH_TYPE_ID INT PRIMARY KEY AUTOINCREMENT,
    MATCH_TYPE TEXT NOT NULL
);


-- MATCH_FACT 

CREATE OR REPLACE TABLE DEMO_DB.ALF_CONSUMPTION.MATCH_FACT(
    match_id INT PRIMARY KEY,
    date_id INT NOT NULL,
    referee_id INT NOT NULL,
    team_a_id INT NOT NULL,
    team_b_id INT NOT NULL,
    match_type_id INT NOT NULL,
    venue_id INT NOT NULL,
    total_overs number(3),
    balls_per_over number(1),

    overs_played_by_team_a number(2),
    bowls_played_by_team_a number(3),
    extra_bowls_played_by_team_a number(3),
    extra_runs_scored_by_team_a number(3),
    fours_by_team_a number(3),
    sixes_by_team_a number(3),
    total_score_by_team_a number(3),
    wicket_lost_by_team_a number(2),

    overs_played_by_team_b number(2),
    bowls_played_by_team_b number(3),
    extra_bowls_played_by_team_b number(3),
    extra_runs_scored_by_team_b number(3),
    fours_by_team_b number(3),
    sixes_by_team_b number(3),
    total_score_by_team_b number(3),
    wicket_lost_by_team_b number(2),

    toss_winner_team_id int not null, 
    toss_decision text not null, 
    match_result text not null, 
    winner_team_id int not null,

    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES date_dim (date_id),
    CONSTRAINT fk_referee FOREIGN KEY (referee_id) REFERENCES referee_dim (referee_id),
    CONSTRAINT fk_team1 FOREIGN KEY (team_a_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_team2 FOREIGN KEY (team_b_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_match_type FOREIGN KEY (match_type_id) REFERENCES match_type_dim (match_type_id),
    CONSTRAINT fk_venue FOREIGN KEY (venue_id) REFERENCES venue_dim (venue_id),

    CONSTRAINT fk_toss_winner_team FOREIGN KEY (toss_winner_team_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_winner_team FOREIGN KEY (winner_team_id) REFERENCES team_dim (team_id)
);



/* LET'S POPULATE DATA 

We'll extract the dimension table data using our detail table from clean layer and it will be based on description field as we don't have any master data set. 
In real world, you may also get master data set as separate entities. 
 */

-- let's start with team dim, and fo simplicity, it is just team name 

-- V1
SELECT DISTINCT TEAM_NAME FROM (
    SELECT FIRST_TEAM AS TEAM_NAME FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN
    UNION ALL 
    SELECT SECOND_TEAM AS TEAM_NAME FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN
) ORDER BY 1 ASC;

-- V2 

INSERT INTO DEMO_DB.ALF_CONSUMPTION.TEAM_DIM (TEAM_NAME)  
    SELECT DISTINCT TEAM_NAME FROM (
        SELECT FIRST_TEAM AS TEAM_NAME FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN
        UNION ALL 
        SELECT SECOND_TEAM AS TEAM_NAME FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN
    ) ORDER BY 1 ASC
;
-- V3
SELECT * FROM DEMO_DB.ALF_CONSUMPTION.TEAM_DIM;



/* TEAM PLAYER  */

-- V1 
SELECT * FROM DEMO_DB.ALF_CLEAN.PLAYER_CLEAN_TBL;
-- V2 
SELECT COUNTRY, PLAYER_NAME FROM DEMO_DB.ALF_CLEAN.PLAYER_CLEAN_TBL
GROUP BY COUNTRY, PLAYER_NAME;

-- V3 : GET COUNTRY, TEAM_ID, PLAYER_NAME
SELECT PLA.COUNTRY, PLA.PLAYER_NAME, TEA.TEAM_ID FROM 
DEMO_DB.ALF_CLEAN.PLAYER_CLEAN_TBL PLA
JOIN DEMO_DB.ALF_CONSUMPTION.TEAM_DIM TEA ON TEA.TEAM_NAME = PLA.COUNTRY
GROUP BY PLA.COUNTRY, PLA.PLAYER_NAME, TEA.TEAM_ID;

-- V4 insert the data 

INSERT INTO DEMO_DB.ALF_CONSUMPTION.PLAYER_DIM (TEAM_ID, PLAYER_NAME)
SELECT B.TEAM_ID, A.PLAYER_NAME FROM 
DEMO_DB.ALF_CLEAN.PLAYER_CLEAN_TBL A JOIN DEMO_DB.ALF_CONSUMPTION.TEAM_DIM B
ON A.COUNTRY = B.TEAM_NAME
GROUP BY A.PLAYER_NAME, B.TEAM_ID
;

-- V5 CHECK DATA 
SELECT * FROM DEMO_DB.ALF_CONSUMPTION.PLAYER_DIM;


/*
    VENUE DIMENSION 
*/

-- V1 
SELECT * FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN LIMIT 10;

-- V2 
SELECT VENUE, CITY FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN LIMIT 10; 


-- V3
SELECT VENUE, CITY FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN
GROUP BY  VENUE, CITY;

ALTER TABLE DEMO_DB.ALF_CONSUMPTION.VENUE_DIM 
ALTER (COLUMN VENUE_NAME SET NOT NULL,
COLUMN CITY SET NOT NULL
);

-- V4 
INSERT INTO DEMO_DB.ALF_CONSUMPTION.VENUE_DIM (VENUE_NAME, CITY)
SELECT T.VENUE, T.CITY FROM (
    SELECT 
        VENUE AS VENUE,
        CASE WHEN CITY IS NULL THEN 'NA'
        ELSE CITY
        END AS CITY
    FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN
) AS T
GROUP BY VENUE, CITY;

-- 

SELECT * FROM DEMO_DB.ALF_CONSUMPTION.VENUE_DIM;

/*
    Match Type Dimension
*/
-- V1 
SELECT * FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN;

-- V2 
SELECT MATCH_TYPE FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN GROUP BY MATCH_TYPE;

-- V3
INSERT INTO DEMO_DB.ALF_CONSUMPTION.MATCH_TYPE_DIM (MATCH_TYPE)
SELECT MATCH_TYPE FROM DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN GROUP BY MATCH_TYPE;

SELECT * FROM DEMO_DB.ALF_CONSUMPTION.MATCH_TYPE_DIM;


/*
DATE DIMENSION  
*/

SELECT MIN(EVENT_DATE), MAX(EVENT_DATE) FROM 
DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN;


select '2020-01-01'::date+x 
from (
  select row_number() over(order by 0) x 
  from table(generator(rowcount => 1824))
);
-- generate date  'https://stackoverflow.com/questions/66448449/snowflake-creating-a-list-of-dates
/* 
select '2020-01-01'::date+x 
from (
  select row_number() over(order by 0) x 
  from table(generator(rowcount => 1824))
)
Shorter version:

select '2020-01-01'::date + row_number() over(order by 0) x 
from table(generator(rowcount => 1824))
With an arbitrary start_date and end_date:

select -1 + row_number() over(order by 0) i, start_date + i generated_date 
from (select '2020-01-01'::date start_date, '2020-01-15'::date end_date)
join table(generator(rowcount => 10000 )) x
qualify i < 1 + end_date - start_date

*/
select -1 + row_number() over(order by 0) i, START_DATE + i generated_date 
from (SELECT MIN(EVENT_DATE)::DATE AS START_DATE, MAX(EVENT_DATE) AS END_DATE FROM 
DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN)
join table(generator(rowcount => 10000 )) x
qualify i < 1 + end_date - START_DATE;

CREATE OR REPLACE TRANSIENT TABLE DEMO_DB.ALF_CONSUMPTION.DATE_RANGE01(DATE DATE);
INSERT INTO DEMO_DB.ALF_CONSUMPTION.DATE_RANGE01 (DATE)
SELECT T.generated_date FROM (
select -1 + row_number() over(order by 0) i, START_DATE + i generated_date 
from (SELECT MIN(EVENT_DATE)::DATE AS START_DATE, MAX(EVENT_DATE) AS END_DATE FROM 
DEMO_DB.ALF_CLEAN.MATCH_DETAIL_CLEAN)
join table(generator(rowcount => 10000 )) x
qualify i < 1 + end_date - START_DATE
) AS T
;

INSERT INTO DEMO_DB.ALF_CONSUMPTION.DATE_DIM (
    DATE_ID, FULL_DT, DAY, MONTH, YEAR, QUARTER, DAYOFWEEK, DAYOFMONTH, DAYOFYEAR, DAYOFWEEKNAME, ISWEEKEND
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY DATE) AS DATE_ID,
    DATE AS FULL_DATE,
    EXTRACT(DAY FROM DATE) AS DAY,
    EXTRACT(MONTH FROM DATE) AS MONTH,
    EXTRACT(YEAR FROM DATE) AS YEAR,
    CASE WHEN EXTRACT(QUARTER FROM DATE) IN (1,2,3,4)  THEN EXTRACT(QUARTER FROM DATE) END AS QUARTER,
    DAYOFWEEKISO(DATE) AS DAY_OF_WEEK,
    EXTRACT(DAY FROM DATE) AS DAY_OF_MONTH,
    DAYOFYEAR(DATE) AS DAY_OF_YEAR,
    DAYNAME(DATE) AS DAY_OF_WEEK_NAME,
    CASE WHEN DAYNAME(DATE) IN ('Sat', 'Sun')  THEN 1 ELSE 0 END AS IS_WEEKEND
FROM DEMO_DB.ALF_CONSUMPTION.DATE_RANGE01;

SELECT * FROM DEMO_DB.ALF_CONSUMPTION.DATE_DIM;


