-- Databricks notebook source
-- MAGIC %md
-- MAGIC Set target file size for optimize

-- COMMAND ----------

ALTER TABLE db1.people10m SET TBLPROPERTIES (targetFileSize = 1073741824 )


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Optimize for write operations. Performs additional shuffle but optimizes file sizes

-- COMMAND ----------

ALTER TABLE db1.people10m SET TBLPROPERTIES (optimizeWrite = true)
