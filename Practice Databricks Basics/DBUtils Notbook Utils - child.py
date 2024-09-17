# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

dbutils.widgets.text('a','10','Enter value for a')
dbutils.widgets.text('b','20','Enter value for b')

# COMMAND ----------

a = int(dbutils.widgets.get('a'))
b = int(dbutils.widgets.get('b'))

# COMMAND ----------

c = a+b

# COMMAND ----------

dbutils.notebook.exit(f'Notebook Executed and returnes {c}')

# COMMAND ----------

print('Notebook Exited')
