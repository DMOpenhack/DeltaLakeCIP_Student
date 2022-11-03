-- Databricks notebook source
-- MAGIC %md
-- MAGIC Start the notebook and immediately start 14-concurrency-2 notebook 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS db1.people10m_stg
-- MAGIC USING DELTA
-- MAGIC LOCATION '/databricks-datasets/learning-spark-v2/people/people-10m.delta';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Fails with - java.util.concurrent.ExecutionException:

-- COMMAND ----------

Optimize db1.people10m_stg
