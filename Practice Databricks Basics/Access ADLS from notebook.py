# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="adls-access",key="SP-Secret")

print(service_credential)

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="adls-access",key="SP-Secret")

spark.conf.set("fs.azure.account.auth.type.sivadbadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sivadbadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sivadbadls.dfs.core.windows.net", 'b2633bb8-0599-4842-bc8b-b478671a8f1b')
spark.conf.set("fs.azure.account.oauth2.client.secret.sivadbadls.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sivadbadls.dfs.core.windows.net", "https://login.microsoftonline.com/268a66dd-648a-4fa2-b072-f15dc98cdc46/oauth2/token")

# COMMAND ----------

df = spark.read.option("header", "true").csv('abfs://test@sivadbadls.dfs.core.windows.net/sample/*.csv')

# COMMAND ----------

df.display()
