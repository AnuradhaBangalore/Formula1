# Databricks notebook source
# MAGIC %md 
# MAGIC #### Access data lake storage using SAS tokens
# MAGIC 1. set the spark for sas token
# MAGIC 2. list the file from demo container
# MAGIC 3. read the data from circuits.csv file

# COMMAND ----------

formula1dl12_demo_sas_token = dbutils.secrets.get("formula1scope", "formula1dl12-demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl12.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl12.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl12.dfs.core.windows.net", formula1dl12_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl12.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl12.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


