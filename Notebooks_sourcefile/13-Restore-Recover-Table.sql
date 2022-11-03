-- Databricks notebook source
-- MAGIC %md
-- MAGIC Revert a table to a version

-- COMMAND ----------

Describe history db1.people10m

-- COMMAND ----------

RESTORE TABLE db1.people10m TO VERSION AS OF 7
--RESTORE TABLE delta.`/data/target/` TO TIMESTAMP AS OF <timestamp>

-- COMMAND ----------

Describe detail db1.people10m

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drop table but recover before hard delete from storage

-- COMMAND ----------

Drop table db1.people10m

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Go to Azure Data Lake, use soft delete - revert feature to bring the parquet and delta log files

-- COMMAND ----------

CREATE TABLE db1.people10m 
Location '/mnt/deltalake/people'

-- COMMAND ----------

Select count(*) from db1.people10m 

-- COMMAND ----------

Describe history db1.people10m 
