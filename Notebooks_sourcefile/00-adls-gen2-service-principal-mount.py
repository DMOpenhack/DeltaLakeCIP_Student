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
