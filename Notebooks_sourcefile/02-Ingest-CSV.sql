-- Databricks notebook source
-- MAGIC %md
-- MAGIC use scala to read csv files

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val diamonds = spark.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
-- MAGIC 
-- MAGIC display(diamonds)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create delta table

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.sql("Create database if not exists db1");
-- MAGIC diamonds.write.format("delta").mode("overwrite").saveAsTable("db1.diamonds")

-- COMMAND ----------

Select * from db1.diamonds

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC Describe Detail db1.diamonds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Read CSV using sparksql

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
-- MAGIC CREATE TEMPORARY VIEW diamonds_vw
-- MAGIC USING CSV
-- MAGIC OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true", mode "FAILFAST")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create delta table from temp view

-- COMMAND ----------

DROP TABLE if exists db1.diamonds_sql;
CREATE OR REPLACE TABLE db1.diamonds_sql 
USING DELTA
LOCATION '/mnt/deltalake/diamonds'
AS
Select * from diamonds_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create csv based table

-- COMMAND ----------

CREATE  TABLE db1.diamonds_csv 
USING CSV
LOCATION '/mnt/deltalake/diamonds_csv'
AS
Select * from diamonds_vw

-- COMMAND ----------

Select * from db1.diamonds_csv 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC updates fail in csv table.
-- MAGIC Inserts work

-- COMMAND ----------

Update db1.diamonds_csv SET color = 'Y' where color = 'E'

-- COMMAND ----------

Insert into db1.diamonds_csv select * from db1.diamonds_csv

-- COMMAND ----------

Drop table db1.diamonds_csv
