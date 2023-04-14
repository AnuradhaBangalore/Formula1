# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id= dbutils.secrets.get("formula1scope", "formula1-client-app-id")
    tenent_id=dbutils.secrets.get("formula1scope", "formula1-app-tenent-id")
    client_secret=dbutils.secrets.get("formula1scope", "formula1-app-client-secret-key")
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("formula1dl12", "presentation")

# COMMAND ----------

mount_adls("formula1dl12", "processed")

# COMMAND ----------

mount_adls("formula1dl12", "raw")

# COMMAND ----------

# MAGIC %fs ls /mnt/formula1dl12/raw

# COMMAND ----------


