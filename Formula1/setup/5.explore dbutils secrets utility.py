# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC %%%% explore the capabilites of dbutils.secrect utility 

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list(scope="formula1scope")

# COMMAND ----------

dbutils.secrets.get("formula1scope", "formula1dl12-account-key")

# COMMAND ----------


