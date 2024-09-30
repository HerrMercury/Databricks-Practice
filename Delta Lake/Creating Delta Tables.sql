-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS `hive_metastore`.Delta

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `hive_metastore`.`Delta`.DeltaFile(
  Education_Level VARCHAR(50) ,
  Line_Number INT ,
  Employed INT ,
  Unemployed INT ,
  Industry VARCHAR(50)  ,
  Gender VARCHAR(20) ,
  Date_Inserted DATE ,
  dense_rank INT 
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(f'dbfs:/user/hive/warehouse/delta.db/deltafile')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(f'dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log')

-- COMMAND ----------

SELECT * FROM TEXT.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000000.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Insert data into table

-- COMMAND ----------

insert into `hive_metastore`.`Delta`.DeltaFile
values('Bachelors',1,450,100,'IT','Male','2022-01-01',1),
      ('Masters',2,250,50,'Research','Male','2022-01-02',2),
      ('High School',3,600,150,'Sales','Female','2022-01-03',3),
      ('Diploma',4,300,75,'Finance','Male','2022-01-04',4)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ####Reading metadata json file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(f'dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log')

-- COMMAND ----------

Select * from Text.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Update Records in the Delta table

-- COMMAND ----------

UPDATE `hive_metastore`.`Delta`.DeltaFile
SET Gender = 'Female'
where Education_Level = 'Bachelors'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Reading Metadata files after the update command

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(f'dbfs:/user/hive/warehouse/delta.db/deltafile')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(f'dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log')

-- COMMAND ----------

Select * from Text.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000002.json`

-- COMMAND ----------

Select * from Text.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000003.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Delete operation on Delta Table

-- COMMAND ----------

DELETE from `hive_metastore`.`Delta`.DeltaFile
where dense_rank=2

-- COMMAND ----------

SELECT * FROM `hive_metastore`.`Delta`.DeltaFile

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(f'dbfs:/user/hive/warehouse/delta.db/deltafile') 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(f'dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log') 

-- COMMAND ----------

select * from TEXT.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000004.json`
