-- Databricks notebook source
-- MAGIC %md 
-- MAGIC exploring optimize to compact delta table. 

-- COMMAND ----------

CREATE TABLE db1.people10m_optimized
LOCATION '/mnt/deltalake/people10m_optimized'
AS 
Select * from db1.people10m

-- COMMAND ----------

Describe detail db1.people10m_optimized

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Identifying the number of files in the table

-- COMMAND ----------

select distinct input_file_name() from db1.people10m_optimized

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Perform some DML to create more parquet files

-- COMMAND ----------

Delete from db1.people10m_optimized where salary between 20000 and 50000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check parquet files

-- COMMAND ----------

select distinct input_file_name() from db1.people10m_optimized

-- COMMAND ----------

Optimize db1.people10m_optimized

-- COMMAND ----------

select distinct input_file_name() from db1.people10m_optimized

-- COMMAND ----------

-- MAGIC %python
-- MAGIC database_names_filter = "db1"
-- MAGIC try:
-- MAGIC     dbs = spark.sql(f"SHOW DATABASES LIKE '{database_names_filter}'").select("databaseName").collect()
-- MAGIC     dbs = [(row.databaseName) for row in dbs]
-- MAGIC     for database_name in dbs:
-- MAGIC         print(f"Found database: {database_name}, performing actions on all its tables..")
-- MAGIC         tables = spark.sql(f"SHOW TABLES FROM `{database_name}`").select("tableName").collect()
-- MAGIC         tables = [(row.tableName) for row in tables]
-- MAGIC         for table_name in tables:
-- MAGIC        #spark.sql(f"ALTER TABLE `{database_name}`.`{table_name}` SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 30 days', 'delta.deletedFileRetentionDuration'='interval 7 days')")
-- MAGIC             spark.sql(f"OPTIMIZE `{database_name}`.`{table_name}`")   
-- MAGIC 	    #spark.sql(f"VACUUM `{database_name}`.`{table_name}`")
-- MAGIC             #spark.sql(f"ANALYZE TABLE `{database_name}`.`{table_name}` COMPUTE STATISTICS")
-- MAGIC except Exception as e:
-- MAGIC     raise Exception(f"Exception:`{str(e)}`")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC optimize db1.people10m where gender = 'F' 
-- MAGIC --works only for partitioned columns
