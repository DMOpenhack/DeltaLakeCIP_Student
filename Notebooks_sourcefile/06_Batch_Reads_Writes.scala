// Databricks notebook source
// MAGIC %md
// MAGIC In this notebook, we will look at performing updates, deletes , upserts via merge statement on delta tables

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS db1.people10m_stg
// MAGIC USING DELTA
// MAGIC LOCATION '/databricks-datasets/learning-spark-v2/people/people-10m.delta';

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS db1.people10m;
// MAGIC CREATE or Replace TABLE db1.people10m (
// MAGIC   id INT,
// MAGIC   firstName STRING,
// MAGIC   middleName STRING,
// MAGIC   lastName STRING,
// MAGIC   gender STRING,
// MAGIC   birthDate TIMESTAMP,
// MAGIC   ssn STRING,
// MAGIC   salary INT
// MAGIC )
// MAGIC USING DELTA Location  "/mnt/deltalake/people"
// MAGIC PARTITIONED BY (gender);

// COMMAND ----------

// MAGIC %sql
// MAGIC Insert into db1.people10m Select * from db1.people10m_stg

// COMMAND ----------

// MAGIC %sql
// MAGIC Describe Detail db1.people10m

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC Select count(*) from db1.people10m where firstName like 'A%' and salary between 40000 and 50000

// COMMAND ----------

// MAGIC %sql 
// MAGIC Delete From db1.people10m  where firstName like 'A%' and salary between 40000 and 50000

// COMMAND ----------

// MAGIC %md
// MAGIC Checking the version history of the table

// COMMAND ----------

// MAGIC %sql
// MAGIC Describe History db1.people10m 

// COMMAND ----------

// MAGIC %md
// MAGIC Control versions maintained by delta table using logRetentionDuration property. Default is 30 days.

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE db1.people10m SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 15 days')

// COMMAND ----------

// MAGIC %md
// MAGIC Querying the older version

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM db1.people10m Version AS OF 1 where firstName like 'A%' and salary between 40000 and 50000

// COMMAND ----------

// MAGIC %sql
// MAGIC Insert into db1.people10m SELECT * FROM db1.people10m Version AS OF 1 where firstName like 'A%' and salary between 40000 and 50000

// COMMAND ----------

// MAGIC %md
// MAGIC Lets perform some delete and update for the rows between 1 to 200

// COMMAND ----------

// MAGIC %sql
// MAGIC Delete From db1.people10m where id between 1 and 100

// COMMAND ----------

// MAGIC %sql
// MAGIC Select * from db1.people10m where id between 101 and 200

// COMMAND ----------

// MAGIC %sql
// MAGIC Update db1.people10m SET salary = salary * 1.20 where id between 101 and 200

// COMMAND ----------

// MAGIC %sql
// MAGIC Describe history  db1.people10m

// COMMAND ----------

// MAGIC %md
// MAGIC Using merge and time travel to revert the changes done. Ensure to update the timestamp

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO db1.people10m source
// MAGIC   USING db1.people10m TIMESTAMP AS OF "2022-03-31 03:50:32" target
// MAGIC   ON source.id = target.id
// MAGIC   WHEN MATCHED THEN UPDATE SET *
// MAGIC   WHEN NOT MATCHED
// MAGIC   THEN INSERT * 

// COMMAND ----------

// MAGIC %sql
// MAGIC Select *  From db1.people10m where id between 101 and 200

// COMMAND ----------

// MAGIC %sql
// MAGIC Delete  From db1.people10m where id between 1 and 100

// COMMAND ----------

// MAGIC %md
// MAGIC Overwrite table ( instead of append ) using insert overwrite

// COMMAND ----------

// MAGIC %sql
// MAGIC Insert overwrite  db1.people10m Select * from db1.people10m TIMESTAMP AS OF "2022-03-31 03:50:54"

// COMMAND ----------

// MAGIC %sql
// MAGIC Select count(*) from db1.people10m
