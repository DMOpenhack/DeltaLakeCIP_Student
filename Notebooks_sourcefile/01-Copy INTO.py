# Databricks notebook source
# MAGIC %md
# MAGIC Create a data lake storage account<br>
# MAGIC Create a container deltalake inside the storage account<br>
# MAGIC Follow the steps in the below link to grant access to mount the storage account to databricks workspace<br>

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access

# COMMAND ----------

# MAGIC %md
# MAGIC Steps to mount the container provided below <br>
# MAGIC Update the key vault and scope name

# COMMAND ----------

# DBTITLE 1,Configure authentication for mounting - sample code
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "44aeae90-c3b9-431f-af5b-eeb5f5e896eb",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="adlskey",key="adlskey"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC Update the storage account name as per your environment

# COMMAND ----------

# DBTITLE 1,Mount filesystem
dbutils.fs.mount(
  source = "abfss://staging@dmadlsg2raw.dfs.core.windows.net/",
  mount_point = "/mnt/deltalake",
  extra_configs = configs)



# COMMAND ----------

# MAGIC %sql
# MAGIC --#dbutils.fs.unmount("/mnt/deltalake")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a sample database, a sample table, load some data 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE Database if not exists  db1 ;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS db1.loan_risks_upload;
# MAGIC 
# MAGIC CREATE TABLE db1.loan_risks_upload (
# MAGIC   loan_id BIGINT,
# MAGIC   funded_amnt INT,
# MAGIC   paid_amnt DOUBLE,
# MAGIC   addr_state STRING
# MAGIC ) USING DELTA LOCATION '/mnt/deltalake/loan_risks_upload';

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO db1.loan_risks_upload
# MAGIC FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
# MAGIC FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from db1.loan_risks_upload;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM db1.loan_risks_upload;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE Database if not exists  db1 ;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS db1.loan_risks_upload;
# MAGIC 
# MAGIC CREATE TABLE db1.loan_risks_upload (
# MAGIC   loan_id BIGINT,
# MAGIC   funded_amnt INT,
# MAGIC   paid_amnt DOUBLE,
# MAGIC   addr_state STRING
# MAGIC ) USING DELTA LOCATION '/mnt/deltalake/loan_risks_upload';
# MAGIC 
# MAGIC COPY INTO db1.loan_risks_upload
# MAGIC FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
# MAGIC FILEFORMAT = PARQUET;
# MAGIC 
# MAGIC SELECT * FROM db1.loan_risks_upload;
# MAGIC 
# MAGIC -- Result:
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- | loan_id | funded_amnt | paid_amnt | addr_state |
# MAGIC -- +=========+=============+===========+============+
# MAGIC -- | 0       | 1000        | 182.22    | CA         |
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- | 1       | 1000        | 361.19    | WA         |
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- | 2       | 1000        | 176.26    | TX         |
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- ...

# COMMAND ----------

# MAGIC %md
# MAGIC Navigate to the container and folder in datalake gen 2 account and show the directory structure, data file and log file format

# COMMAND ----------

# DBTITLE 1,Basic Insert
# MAGIC %sql
# MAGIC Insert into db1.loan_risks_upload Select * from db1.loan_risks_upload

# COMMAND ----------

# DBTITLE 1,Read Data
# MAGIC %sql
# MAGIC Select count(*) from db1.loan_risks_upload

# COMMAND ----------

# DBTITLE 1,Select with Join
# MAGIC %sql
# MAGIC select x.addr_state,y.addr_state from db1.loan_risks_upload x inner join db1.loan_risks_upload Y on x.loan_id = y.loan_id where x.paid_amnt between 400 and 500
