// Databricks notebook source
// MAGIC %md
// MAGIC Create a container called autoloader<br>
// MAGIC Mount the container as explained in https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access <br>

// COMMAND ----------

// MAGIC %python
// MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
// MAGIC            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC            "fs.azure.account.oauth2.client.id": "70a9660e-6e43-4c70-8c58-21a7781b64d9",
// MAGIC            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="databricks",key="databricks"),
// MAGIC            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/cbece553-ab71-488a-8047-d07f6eef836d/oauth2/token"}

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.mount(
// MAGIC   source = "abfss://autoloader@deltalakedbs.dfs.core.windows.net/",
// MAGIC   mount_point = "/mnt/autoloader",
// MAGIC   extra_configs = configs)

// COMMAND ----------

// MAGIC %md
// MAGIC Create a folder "population" in autoloader container <br>
// MAGIC Load the contents (csv files) of "population" dolder available in dataset folder in IP artifacts into population folder in autoloader container <br>

// COMMAND ----------

val upload_path = "/mnt/autoloader/Population"

dbutils.fs.mkdirs(upload_path)

// COMMAND ----------

val checkpoint_path = "/mnt/autoloader/population_ckpt"
val write_path = "/mnt/autoloader/population_write"

// Set up the stream to begin reading incoming files from the
// upload_path location.
val df = spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true")
  .schema("city string, year int, population long")
  .load(upload_path)

// Start the stream.
// Use the checkpoint_path location to keep a record of all files that
// have already been uploaded to the upload_path location.
// For those that have been uploaded since the last check,
// write the newly-uploaded files' data to the write_path location.
df.writeStream.format("delta")
  .option("checkpointLocation", checkpoint_path)
  .start(write_path)

// COMMAND ----------

val df_population = spark.read.format("delta").load(write_path)

display(df_population)

/* Result:
+----------------+------+------------+
| city           | year | population |
+================+======+============+
| Seattle metro  | 2019 | 3406000    |
+----------------+------+------------+
| Seattle metro  | 2020 | 3433000    |
+----------------+------+------------+
| Portland metro | 2019 | 2127000    |
+----------------+------+------------+
| Portland metro | 2020 | 2151000    |
+----------------+------+------------+
*/

// COMMAND ----------

dbutils.fs.rm(write_path, true)
dbutils.fs.rm(upload_path, true)
