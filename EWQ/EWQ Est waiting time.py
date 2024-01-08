# Databricks notebook source
# MAGIC %md #### 1. Mount Azure Data Lake storage using Service Principal
# MAGIC
# MAGIC

# COMMAND ----------

### Setup client_id,tenant_id and client_secret 
client_id = dbutils.secrets.get(scope = 'EWQ-scope', key = 'ewq-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'EWQ-scope', key = 'ewq-app-tenent-id')
client_secret = dbutils.secrets.get(scope = 'EWQ-scope', key = 'ewq-app-client-secret')

# COMMAND ----------

### Mount Azure Data Lake using Service Principal
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'EWQ-scope', key = 'ewq-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'EWQ-scope', key = 'ewq-app-tenent-id')
    client_secret = dbutils.secrets.get(scope = 'EWQ-scope', key = 'ewq-app-client-secret')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())


##### Mount Raw Container

mount_adls('ewqstorageaccount', 'csv')

# COMMAND ----------

### List all files from storage account container 

display(dbutils.fs.ls("/mnt/ewqstorageaccount/csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Reading CSV files into dataframe

# COMMAND ----------

### Read the unnamed_store file 
df1 = spark.read.option("header", "true").option("delimiter",";").csv("/mnt/ewqstorageaccount/csv/unnamed_store.csv") 

# COMMAND ----------

### Read the unnamed_store_2 file 
df2 = spark.read.option("header", "true").option("delimiter",";").csv("/mnt/ewqstorageaccount/csv/unnamed_store_2.csv") 

# COMMAND ----------

### combining 2 csv files from storage account 
df = df1.union(df2)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Estimated waiting time in minutes

# COMMAND ----------

from pyspark.sql.functions import expr, col, unix_timestamp , count

df_estimated_time_in_secs = df \
    .withColumn("estimated_waiting_time_secs", expr("unix_timestamp(time_called) - unix_timestamp(time_generated)")) 

df_estimated_time_in_mins = df_estimated_time_in_secs \
    .withColumn("estimated_waiting_time_minutes", (col("estimated_waiting_time_secs") / 60))



# COMMAND ----------

display(df_estimated_time_in_mins)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4) Estimated service time in minutes

# COMMAND ----------

##### calculating estiamted service in minutes

display(df.withColumn("ServiceDuration_Mins", col("dur_service") / 60))

# COMMAND ----------

# Calculate the total service duration in minutes and group by 'area_code' 'group name' and 'desk number'

from pyspark.sql.functions import col, sum, round, count

result_df = (
    df.withColumn("ServiceDuration_Mins", round(col("dur_service") / 60, 1)) 
      .groupBy("area_code", "group_name", "desk_number")
      .agg(sum("ServiceDuration_Mins").alias("Total_Service_Duration_Mins"),count("id").alias("Total_Number_of_Tokens"))
)

# Display the result
display(result_df)


# COMMAND ----------

# Calculate the total waiting duration in minutes and group by 'area_code' 'group name' and 'desk number'

from pyspark.sql.functions import col, avg, round, count

result_df = (
    df_estimated_time_in_mins
      .groupBy("area_code", "group_name", "desk_number")
      .agg(
          avg("estimated_waiting_time_minutes").alias("Total_waiting_Duration_Mins"),
          count("id").alias("Total_Number_of_Tokens")
      )
)

# Display the result
display(result_df)



# COMMAND ----------

from pyspark.sql.functions import expr,col
# code for calculating waiting time
df_selected = df.select(col("id"), col("area_code"), col("group_name"), col("desk_number"),
                        col("ticket_number"), col("queue_length"), col("time_generated"),
                        col("time_called"), col("count_desks"), col("status"))
df_estimated_time_in_secs = df_selected \
    .withColumn("estimated_waiting_time_secs", col("time_called").cast("long") - col("time_generated").cast("long"))
df_estimated_time_in_mins = df_estimated_time_in_secs\
    .withColumn("estimated_waiting_time_minutes", (col("estimated_waiting_time_secs") / 60))\
    .withColumn("estimated_waiting_time_minutes", round("estimated_waiting_time_minutes", 2))\
    .drop(col("estimated_waiting_time_secs"))\
    .orderBy(col("area_code"))
# Calculating average waiting time per area code
avg_waiting_time_per_area = df_estimated_time_in_mins\
                                        .groupBy("area_code").avg("estimated_waiting_time_minutes")
# Registering the resulting DataFrame as a temporary SQL table
avg_waiting_time_per_area.createOrReplaceTempView("avg_waiting_time_table")
# Use Databricks display function to visualize the results
display(spark.sql("SELECT * FROM avg_waiting_time_table"))
