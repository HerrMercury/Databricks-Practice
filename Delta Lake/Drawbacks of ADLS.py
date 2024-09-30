# Databricks notebook source
service_credential = dbutils.secrets.get(scope="adls-access",key="SP-Secret")

spark.conf.set("fs.azure.account.auth.type.sivadbadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sivadbadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sivadbadls.dfs.core.windows.net", 'b2633bb8-0599-4842-bc8b-b478671a8f1b')
spark.conf.set("fs.azure.account.oauth2.client.secret.sivadbadls.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sivadbadls.dfs.core.windows.net", "https://login.microsoftonline.com/268a66dd-648a-4fa2-b072-f15dc98cdc46/oauth2/token")

# COMMAND ----------

source = 'abfs://test@sivadbadls.dfs.core.windows.net/Files/'

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,FloatType,DoubleType

schema1 = StructType([
    StructField('Education_Level',StringType()),
    StructField('Line_Number',IntegerType()),
    StructField('Employed',IntegerType()),
    StructField('Unemployed',IntegerType()),
    StructField('Industry',StringType()),
    StructField('Gender',StringType()),
    StructField('Date_Inserted',StringType()),
    StructField('dense_rank',IntegerType())
])

# COMMAND ----------

df = (spark.read.format('csv')
            .option('header','true')
            .schema(schema1)
            .load(f'{source}*.csv'))

# COMMAND ----------

display(df.head())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing into Parquet format

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
        .save(f'{source}/ParquetFolder')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading from Parquet file

# COMMAND ----------

par_df = spark.read.format('parquet').load(f'{source}/ParquetFolder')

# COMMAND ----------

display(par_df)

# COMMAND ----------

par_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a temp view
# MAGIC

# COMMAND ----------

par_df.createOrReplaceTempView("ParquetView")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE ParquetView
# MAGIC SET Education_Level = 'School'
# MAGIC where Education_Level = 'High School'

# COMMAND ----------

spark.sql("""UPDATE ParquetView
SET Education_Level = 'School'
where Education_Level = 'High School' """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Filter and Overwrite

# COMMAND ----------

df_filtered = par_df.filter("Education_Level = 'High School'")

# COMMAND ----------

df_filtered.display()

# COMMAND ----------

df_filtered.write.format("parquet").mode("overwrite").save(f'{source}/ParquetFolder') 

# COMMAND ----------

filtered_df = spark.read.format('parquet').load(f'{source}/ParquetFolder')

# COMMAND ----------

filtered_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Writing the df in Delta format

# COMMAND ----------

dbutils.fs.rm(f'{source}/DeltaFolder', True)

# COMMAND ----------

par_df.write.format('delta').mode('overwrite').save(f'{source}/DeltaFolder')

# COMMAND ----------

dbutils.fs.ls(f'{source}/DeltaFolder') 

# COMMAND ----------

spark.read.format('parquet').load('abfs://test@sivadbadls.dfs.core.windows.net/Files/DeltaFolder/part-00000-8d7c3f14-2520-4090-949a-ad8f9a5b14c2-c000.snappy.parquet').display()
