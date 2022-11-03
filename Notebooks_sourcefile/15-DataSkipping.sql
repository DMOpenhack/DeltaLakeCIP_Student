-- Databricks notebook source
Create or replace table db1.people10m_dataskipping
location '/mnt/deltalake/db1.people10m_dataskipping'
as
Select * from db1.people10m_stg

-- COMMAND ----------

Describe  db1.people10m_dataskipping

-- COMMAND ----------

-- MAGIC %md
-- MAGIC increase data skipping to 35 columns

-- COMMAND ----------

ALTER TABLE db1.people10m_dataskipping SET TBLPROPERTIES (delta.dataSkippingNumIndexedCols = 35)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC changing column order to bring the column into first 32 which enables data skipping

-- COMMAND ----------

ALTER TABLE db1.people10m_dataskipping CHANGE COLUMN SSN AFTER id

-- COMMAND ----------

Describe db1.people10m_dataskipping
