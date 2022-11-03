-- Databricks notebook source
-- MAGIC %md
-- MAGIC deletedFileRetentionDuration controls the minimum age before which parquet files cant be delete by vaccum. Has a default value of 7 days. Setting it to 1 hour for this demo 

-- COMMAND ----------

ALTER TABLE db1.people10m_optimized SET TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 1 hour')

-- COMMAND ----------

Describe History db1.people10m_optimized

-- COMMAND ----------

delete from db1.people10m_optimized where salary < 60000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute vaccum after an hour

-- COMMAND ----------

Vacuum  db1.people10m_optimized

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Time travel doesn't work as Vacuum has cleared older parquet files

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC Select * from db1.people10m_optimized Version as of 1
