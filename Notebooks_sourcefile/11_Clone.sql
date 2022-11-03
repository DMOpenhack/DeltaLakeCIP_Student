-- Databricks notebook source
-- MAGIC %md
-- MAGIC Create a table to test cloning options in delta table

-- COMMAND ----------

Create or replace table db1.clone_test using Delta location '/mnt/deltalake/clone_test' as Select * from db1.people10m



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Shallow clone example - happens quickly

-- COMMAND ----------

create or replace table db1.clone_test_sc SHALLOW CLONE db1.clone_test location '/mnt/deltalake/ShallowClone'
 --Drop table db1.clone_test_sc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Files would reference old path

-- COMMAND ----------

select distinct input_file_name() from db1.clone_test_sc

-- COMMAND ----------

select count(*) from db1.clone_test_sc

-- COMMAND ----------

Insert into db1.clone_test_sc select * from db1.clone_test

-- COMMAND ----------

select count(*) from db1.clone_test_sc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Files would show both old and new path as Insert statement added new parquet files

-- COMMAND ----------

Select distinct  input_file_name() from db1.clone_test_sc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC making changes at original table would not affect clone table

-- COMMAND ----------

Insert into db1.clone_test select * from db1.clone_test

-- COMMAND ----------

select count(*) from db1.clone_test_sc

-- COMMAND ----------

Select distinct  input_file_name() from db1.clone_test_sc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC deep clone physically copies the files

-- COMMAND ----------

create or replace table db1.clone_test_c  CLONE db1.clone_test_sc location '/mnt/deltalake/deep_clone'

-- COMMAND ----------

Select distinct  input_file_name() from db1.clone_test_c

-- COMMAND ----------

Insert into db1.clone_test_c Select * from db1.clone_test

-- COMMAND ----------

Select count(*) from db1.clone_test_c

-- COMMAND ----------

Select distinct  input_file_name() from db1.clone_test_c
