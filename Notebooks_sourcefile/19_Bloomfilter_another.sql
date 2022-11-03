-- Databricks notebook source
Drop table db1.people10m_indexed;
Create table db1.people10m_indexed
using delta
location '/mnt/deltalake/people10_indexed'
as
select * from db1.people10m

-- COMMAND ----------

SET spark.databricks.io.skipping.bloomFilter.enabled = true;

-- COMMAND ----------

CREATE BLOOMFILTER INDEX
ON TABLE db1.people10m_indexed
FOR COLUMNS(firstName OPTIONS (fpp=0.1, numItems=50000))

-- COMMAND ----------

Optimize db1.people10m_indexed Zorder by (firstName)

-- COMMAND ----------

Select count(*) from  db1.people10m_indexed
where firstName = "Terrie"



-- COMMAND ----------

Select count(*),LastName from  db1.people10m where firstName = "Nagaraj" 
Group by LastName

-- COMMAND ----------

Select count(*) from  db1.people10m_indexed
where  LastName = "McGrirl"


-- COMMAND ----------

Select * from  db1.people10m_indexed
