# Databricks notebook source
# MAGIC %md 
# MAGIC ####Access data lake storage using service principals
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role Storage Blob Data Contributor to the Data Lake

# COMMAND ----------

client_id= dbutils.secrets.get("formula1scope", "formula1-client-app-id")
tenent_id=dbutils.secrets.get("formula1scope", "formula1-app-tenent-id")
client_secret=dbutils.secrets.get("formula1scope", "formula1-app-client-secret-key")


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl12.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl12.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl12.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl12.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl12.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl12.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl12.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


