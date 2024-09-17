# Databricks notebook source
# MAGIC %md
# MAGIC ####Widgets Practice Notebook

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #####ComboBox

# COMMAND ----------

dbutils.widgets.combobox('combobox_name','Emp',['Emp','Dev','Prod'],'Designation')

# COMMAND ----------

dbutils.widgets.dropdown('dropdown_name','Emp',['Emp','Dev','Prod'],'Designation')

# COMMAND ----------

dbutils.widgets.get('dropdown_name')


# COMMAND ----------

dbutils.widgets.get('combobox_name')

# COMMAND ----------

dbutils.widgets.multiselect('multiselect_name','Emp',['Emp','Dev','Test','Prod'],'Multiselect ')

# COMMAND ----------

dbutils.widgets.get('multiselect_name')

# COMMAND ----------

dbutils.widgets.getAll()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Text

# COMMAND ----------

dbutils.widgets.text('Year','','Year Label')

# COMMAND ----------

result = dbutils.widgets.get('Year')

print(f'SELECT * FROM EMP WHERE YEAR(Joining_Date)= {result}')
