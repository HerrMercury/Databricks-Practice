# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ##DBUtils FileStore

# COMMAND ----------

# MAGIC %md
# MAGIC ###List the Directory

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Directory
# MAGIC
# MAGIC #### dbutils.fs.mkdir(`<path>`) command helps you to create directory in the specified path 

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/FileTest')

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/FileTest3/file1')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Directory 
# MAGIC
# MAGIC #### dbutils.fs.rm(`<path>`,Reccursive Boolean = False (Default)) 
# MAGIC ##### Above rm command removes the directory in the given path and the second parameter has to Set <span style = "Background-color : Yellow">True</span> when there is subfolders or contents in that directory

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/FileTest3')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/FileTest3',True)

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write into a file 
# MAGIC #### put(`<path>`,`<text>`,overwrite boolean = False (Default))

# COMMAND ----------

path = 'dbfs:/FileStore/FileTest'

dbutils.fs.put(path+'/test.txt','My Name is Siva')

# COMMAND ----------

df = spark.read.text(path+'/test.txt')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Read File
# MAGIC
# MAGIC #### Head(`<path>`,`maxBytes`) path is the path of file and maxBytes is the number of bytes to be returned by the function
# MAGIC
# MAGIC #### Reads first defined bytes of data from the given file
# MAGIC

# COMMAND ----------

dbutils.fs.head(path+'/test.txt',200)

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/')

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/FileTest2')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Copy file
# MAGIC
# MAGIC #### cp(`<srcpath>`,`<targetpath>`,recursive boolean = False (default))

# COMMAND ----------

srcpath = 'dbfs:/FileStore/FileTest/test.txt'
targetpath = 'dbfs:/FileStore/FileTest2/'

dbutils.fs.cp(srcpath,targetpath)

# COMMAND ----------

dbutils.fs.ls(targetpath)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Move the file or directory
# MAGIC
# MAGIC #### mv(`<srcpath>`,`<targetpath>`,recursive boolean = False (default))
# MAGIC

# COMMAND ----------

srcpath1 = 'dbfs:/FileStore/FileTest/test.txt'
targetpath1 = 'dbfs:/FileStore/MovedFolder/test.txt'

dbutils.fs.mv(srcpath1,targetpath1)
