# Databricks notebook source
# MAGIC %md 
# MAGIC ####Access data lake storage using access key
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 2. list the file from demo container
# MAGIC 3. read the data from circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get("formula1scope", "formula1dl12-account-key")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl12.dfs.core.windows.net")

# COMMAND ----------



# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dl12.dfs.core.windows.net",
formula1dl_account_key)



# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl12.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl12.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl12.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


