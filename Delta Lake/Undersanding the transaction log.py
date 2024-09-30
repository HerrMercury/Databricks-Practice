# Databricks notebook source
service_credential = dbutils.secrets.get(scope="adls-access",key="SP-Secret")

spark.conf.set("fs.azure.account.auth.type.sivadbadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sivadbadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sivadbadls.dfs.core.windows.net", 'b2633bb8-0599-4842-bc8b-b478671a8f1b')
spark.conf.set("fs.azure.account.oauth2.client.secret.sivadbadls.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sivadbadls.dfs.core.windows.net", "https://login.microsoftonline.com/268a66dd-648a-4fa2-b072-f15dc98cdc46/oauth2/token")

# COMMAND ----------

source = 'abfs://test@sivadbadls.dfs.core.windows.net/Files/DeltaFolder'

# COMMAND ----------

dbutils.fs.ls(f'{source}')

# COMMAND ----------

dbutils.fs.ls(f'{source}/_delta_log/00000000000000000000.json')

# COMMAND ----------

display(spark.read.format('text').load(f'{source}/_delta_log/00000000000000000000.json'))


# COMMAND ----------

# MAGIC %md
# MAGIC {"commitInfo":{"timestamp":1727443012202,"userId":"3761106770248329","userName":"admin@m365x82319650.onmicrosoft.com","operation":"WRITE","operationParameters":{"mode":"Overwrite","statsOnLoad":false,"partitionBy":"[]"},"notebook":{"notebookId":"3819167227200290"},"clusterId":"0904-064914-l7a2ti0a","isolationLevel":"WriteSerializable","isBlindAppend":false,"operationMetrics":{"numFiles":"1","numOutputRows":"1524","numOutputBytes":"26333"},"tags":{"restoresDeletedRows":"false"},"engineInfo":"Databricks-Runtime/14.3.x-scala2.12","txnId":"b5dc3415-a0f6-4614-a652-7b274881dfb5"}}
# MAGIC
# MAGIC {"metaData":{"id":"afbefc2e-5b81-41a9-b944-14431157d7f0","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"Education_Level\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Line_Number\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Employed\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Unemployed\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Industry\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Gender\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Date_Inserted\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dense_rank\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true"},"createdTime":1727443010061}}
# MAGIC
# MAGIC {"add":{"path":"part-00000-ccee080c-355b-401c-b6cf-2cee6f0d46d3-c000.snappy.parquet","partitionValues":{},"size":26333,"modificationTime":1727443010000,"dataChange":true,"stats":"{\"numRecords\":1524,\"minValues\":{\"Education_Level\":\"Associate's Degree\",\"Line_Number\":0,\"Employed\":4700,\"Unemployed\":257,\"Industry\":\"Agriculture\",\"Gender\":\"Female\",\"Date_Inserted\":\"1/1/1995\",\"dense_rank\":1},\"maxValues\":{\"Education_Level\":\"Master's Degree\",\"Line_Number\":1523,\"Employed\":18300000,\"Unemployed\":7481500,\"Industry\":\"finance\",\"Gender\":\"Male\",\"Date_Inserted\":\"9/1/2022\",\"dense_rank\":609},\"nullCount\":{\"Education_Level\":0,\"Line_Number\":0,\"Employed\":0,\"Unemployed\":0,\"Industry\":0,\"Gender\":0,\"Date_Inserted\":0,\"dense_rank\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1727443010000000","MIN_INSERTION_TIME":"1727443010000000","MAX_INSERTION_TIME":"1727443010000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading the dela lake file

# COMMAND ----------

df = spark.read.format('delta').load(source)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Filtering the data in the df

# COMMAND ----------

df_delta = df.filter("Education_Level = 'High School'")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Overwritingthe data in the delta file

# COMMAND ----------

df_delta.write.format("delta").mode("overwrite").save(f'{source}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Reading the overwritten file

# COMMAND ----------

spark.read.format("delta").load(f'{source}').display()

# COMMAND ----------

dbutils.fs.ls(source)

# COMMAND ----------

display(spark.read.format('text').load(f'{source}/_delta_log/00000000000000000001.json'))


# COMMAND ----------

# MAGIC %md
# MAGIC {"commitInfo":{"timestamp":1727443529061,"userId":"3761106770248329","userName":"admin@m365x82319650.onmicrosoft.com","operation":"WRITE","operationParameters":{"mode":"Overwrite","statsOnLoad":false,"partitionBy":"[]"},"notebook":{"notebookId":"3927928914734243"},"clusterId":"0904-064914-l7a2ti0a","readVersion":0,"isolationLevel":"WriteSerializable","isBlindAppend":false,"operationMetrics":{"numFiles":"1","numOutputRows":"112","numOutputBytes":"5169"},"tags":{"restoresDeletedRows":"false"},"engineInfo":"Databricks-Runtime/14.3.x-scala2.12","txnId":"a6c07993-5fb8-4a53-bf52-43f9ef1bb7cb"}}
# MAGIC
# MAGIC {"add":{"path":"part-00000-5fe56b90-497c-46b8-ab43-145ee38b7b85-c000.snappy.parquet","partitionValues":{},"size":5169,"modificationTime":1727443528000,"dataChange":true,"stats":"{\"numRecords\":112,\"minValues\":{\"Education_Level\":\"High School\",\"Line_Number\":141,\"Employed\":4700,\"Unemployed\":258,\"Industry\":\"Construction\",\"Gender\":\"Female\",\"Date_Inserted\":\"1/1/1995\",\"dense_rank\":5},\"maxValues\":{\"Education_Level\":\"High School\",\"Line_Number\":1508,\"Employed\":17630700,\"Unemployed\":2382900,\"Industry\":\"finance\",\"Gender\":\"Male\",\"Date_Inserted\":\"9/1/2014\",\"dense_rank\":556},\"nullCount\":{\"Education_Level\":0,\"Line_Number\":0,\"Employed\":0,\"Unemployed\":0,\"Industry\":0,\"Gender\":0,\"Date_Inserted\":0,\"dense_rank\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1727443528000000","MIN_INSERTION_TIME":"1727443528000000","MAX_INSERTION_TIME":"1727443528000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}
# MAGIC
# MAGIC {"remove":{"path":"part-00000-ccee080c-355b-401c-b6cf-2cee6f0d46d3-c000.snappy.parquet","deletionTimestamp":1727443529056,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":26333,"tags":{"INSERTION_TIME":"1727443010000000","MIN_INSERTION_TIME":"1727443010000000","MAX_INSERTION_TIME":"1727443010000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}

# COMMAND ----------

df_delta.printSchema()
