-- Databricks notebook source
CREATE or Replace table db1.people10m_raw
USING DELTA
LOCATION '/databricks-datasets/learning-spark-v2/people/people-10m.delta';

-- COMMAND ----------

Create or replace table db1.people10m_p
using delta
location "/mnt/deltalake/people10m_partition"
partitioned by (Year_of_Birth)
as select *,date_format(birthDate,'y') as Year_of_Birth from db1.people10m



-- COMMAND ----------

Create or replace table db1.people10m_np
using delta
location "/mnt/myDeltaLake/people10m_nopartition"
as Select *,date_format(birthDate,'y') as Year_of_Birth from db1.people10m

-- COMMAND ----------

--Describe db1.people10m
select date_format(Current_date,'y') - Year_of_Birth  as Age, min(salary),max(Salary), Count(1) as total_pax from db1.people10m_p
Group by Year_of_Birth


-- COMMAND ----------

select date_format(Current_date,'y') - Year_of_Birth  as Age, min(salary),max(Salary),Count(1) as total_pax from db1.people10m_np
Group by Year_of_Birth


-- COMMAND ----------

--Drop table 

-- COMMAND ----------

drop table db1.people10m_np;
drop table db1.people10m_p;
