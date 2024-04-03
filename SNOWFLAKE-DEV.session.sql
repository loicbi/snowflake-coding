

-- start transaction name demo2;


alter session set autocommit = false;


show transactions;

USE DATABASE DEMO_DB;

USE SCHEMA DEMO_DB.ALF_LIQUIBASE;


create or replace table employee_history_demo2(employee_id number,
                     empl_join_date date,
                     dept varchar(10),
                     salary number,
                     manager_id number);
                     
insert into employee_history_demo2 values(8,'2014-10-01','HR',40000,4),
                                 (12,'2014-09-01','Tech',50000,9),
                                 (3,'2018-09-01','Marketing',30000,5),
                                 (4,'2017-09-01','HR',10000,5),
                                 (25,'2019-09-01','HR',35000,9),
                                 (12,'2014-09-01','Tech',50000,9),
                                 (86,'2015-09-01','Tech',90000,4),
                                 (73,'2016-09-01','Marketing',20000,1);
                                 
                                 
select * from employee_history_demo2;

rollback;

select * from employee_history_demo2;
-----------------------------------------------------------------------------------------------------------------------

show transactions;
