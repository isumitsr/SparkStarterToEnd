-- Databricks notebook source
create database if not exists demo_db

-- COMMAND ----------

select * from global_temp.vw_fire_service_calls

-- COMMAND ----------

create table if not exists demo_db.tbl_fire_service_calls (
CallNumber	integer,
UnitID	string,
IncidentNumber	integer,
CallType	string,
CallDate	string,
WatchDate	string,
CallFinalDisposition	string,
AvailableDtTm	string,
Address	string,
City	string,
Zipcode	integer,
Battalion	string,
StationArea	string,
Box	string,
OriginalPriority	string,
Priority	string,
FinalPriority	integer,
ALSUnit	boolean,
CallTypeGroup	string,
NumAlarms	integer,
UnitType	string,
UnitSequenceInCallDispatch	integer,
FirePreventionDistrict	string,
SupervisorDistrict	string,
Neighborhood	string,
[Location] string,
RowID	string,
Delay	float	
) using parquet

-- COMMAND ----------

select * from demo_db.tbl_fire_service_calls

-- COMMAND ----------

insert into demo_db.tbl_fire_service_calls
select * from global_temp.vw_fire_service_calls

-- COMMAND ----------

select * from demo_db.tbl_fire_service_calls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### How many distinct types of calls were made to the Fire Department

-- COMMAND ----------

select count(distinct CallType) as Distinct_callType from demo_db.tbl_fire_service_calls where CallType IS NOT NULL

-- COMMAND ----------

select distinct CallType as Distinct_callType_list from demo_db.tbl_fire_service_calls where CallType IS NOT NULL

-- COMMAND ----------

select CallNumber, Delay from demo_db.tbl_fire_service_calls where delay > 5

-- COMMAND ----------

select CallType, count(CallType) from demo_db.tbl_fire_service_calls group by CallType order by count(CallType) DESC

-- COMMAND ----------

select Calltype, Zipcode, count(*) from demo_db.tbl_fire_service_calls group by Zipcode,calltype order by count(CallType) DESC

-- COMMAND ----------

select distinct zipcode, neighborhood from demo_db.tbl_fire_service_calls where Zipcode IN ('94102','94103')

-- COMMAND ----------

select sum(numAlarms), avg(Delay), min(Delay), max(Delay) from demo_db.tbl_fire_service_calls

-- COMMAND ----------

select distinct YEAR(cast(callDate as date)) from demo_db.tbl_fire_service_calls

--select distinct year(to_date(callDate, "MM/dd/yyyy")) as disYear from demo_db.tbl_fire_service_calls

-- COMMAND ----------

select weekofyear(to_date(callDate, "yyyy-MM-dd")) week_year, count(*) as count
from demo_db.tbl_fire_service_calls
where year(to_date(callDate, "yyyy-MM-dd")) == 2018
group by week_year
order by count desc

-- COMMAND ----------

select Neighborhood, delay from demo_db.tbl_fire_service_calls where year(to_date(callDate, "yyyy-MM-dd")) = 2018
order by delay desc

-- COMMAND ----------


