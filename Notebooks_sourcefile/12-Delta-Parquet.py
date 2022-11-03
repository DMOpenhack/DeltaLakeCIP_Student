# Databricks notebook source
# MAGIC %md
# MAGIC Create Parquet Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  TABLE db1.diamonds_parquet 
# MAGIC USING Parquet
# MAGIC LOCATION '/mnt/deltalake/diamonds_parquet'
# MAGIC AS
# MAGIC Select * from db1.diamonds_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe db1.diamonds_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC update fails in parquet table

# COMMAND ----------

# MAGIC %sql 
# MAGIC Update db1.diamonds_parquet SET color = 'F'

# COMMAND ----------

# MAGIC %md
# MAGIC Convert parquet to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/deltalake/diamonds_parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from DELTA.`dbfs:/mnt/deltalake/diamonds_parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe detail DELTA.`dbfs:/mnt/deltalake/diamonds_parquet`

# COMMAND ----------

# MAGIC %sql 
# MAGIC Update  DELTA.`dbfs:/mnt/deltalake/diamonds_parquet` SET color = 'F'

# COMMAND ----------

# MAGIC %md
# MAGIC Access using orignal parquet table name doesn't work even though we have converted the path as delta

# COMMAND ----------

# MAGIC %sql
# MAGIC Update db1.diamonds_parquet SET color = 'F'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from db1.diamonds_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC for seamless access drop parquet table and recreate it as delta

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table db1.diamonds_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC IF you dont like managing the table as Delta.``, use create table to convert it

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table db1.diamonds_parquet using delta location '/mnt/deltalake/diamonds_parquet'

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe detail db1.diamonds_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC Update db1.diamonds_parquet SET color = 'F'
