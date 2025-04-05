/*

CUSTOMER ADDRESS ENTITY :
    - CONSUMPTION LAYER  ->  DEMO_DB.ALF_CONSUMPTION_SCH.delivery_agent_dim
 */

-- change context
use role DEV_ENT_DW_ENGINEER_FR;
use database DEMO_DB;
use schema ALF_STAGE_SCH;
use warehouse DEMO_WH;

ALTER SESSION SET TIMEZONE  = 'America/Montreal';


CREATE OR REPLACE TABLE DEMO_DB.ALF_CONSUMPTION_SCH.DATE_DIM (
    DATE_DIM_HK NUMBER PRIMARY KEY comment 'Menu Dim HK (EDW)',   -- Surrogate key for date dimension
    CALENDAR_DATE DATE UNIQUE,                     -- The actual calendar date
    YEAR NUMBER,                                   -- Year
    QUARTER NUMBER,                                -- Quarter (1-4)
    MONTH NUMBER,                                  -- Month (1-12)
    WEEK NUMBER,                                   -- Week of the year
    DAY_OF_YEAR NUMBER,                            -- Day of the year (1-365/366)
    DAY_OF_WEEK NUMBER,                            -- Day of the week (1-7)
    DAY_OF_THE_MONTH NUMBER,                       -- Day of the month (1-31)
    DAY_NAME STRING                                -- Name of the day (e.g., Monday)
)
comment = 'Date dimension table created using min of order data.';

insert into DEMO_DB.ALF_CONSUMPTION_SCH.DATE_DIM  
with recursive my_date_dim_cte as 
(
    -- anchor clause
    select 
        current_date() as today,
        year(today) as year,
        quarter(today) as quarter,
        month(today) as month,
        week(today) as week,
        dayofyear(today) as day_of_year,
        dayofweek(today) as day_of_week,
        day(today) as day_of_the_month,
        dayname(today) as day_name

    union all

     -- recursive clause
    select 
        dateadd('day', -1, today) as today_r,
        year(today_r) as year,
        quarter(today_r) as quarter,
        month(today_r) as month,
        week(today_r) as week,
        dayofyear(today_r) as day_of_year,
        dayofweek(today_r) as day_of_week,
        day(today_r) as day_of_the_month,
        dayname(today_r) as day_name
    from 
        my_date_dim_cte
    where 
        today_r > (select date(min(order_date)) from DEMO_DB.ALF_CLEAN_SCH.orders)
)
select 
    hash(SHA1_hex(today)) as DATE_DIM_HK,
    today ,                     -- The actual calendar date
    YEAR,                                   -- Year
    QUARTER,                                -- Quarter (1-4)
    MONTH,                                  -- Month (1-12)
    WEEK,                                   -- Week of the year
    DAY_OF_YEAR,                            -- Day of the year (1-365/366)
    DAY_OF_WEEK,                            -- Day of the week (1-7)
    DAY_OF_THE_MONTH,                       -- Day of the month (1-31)
    DAY_NAME     
from my_date_dim_cte;


SELECT * FROM DEMO_DB.ALF_CONSUMPTION_SCH.DATE_DIM ORDER BY CALENDAR_DATE ASC ;

    select 
        dateadd('day', -1, CURRENT_DATE) as today_r;