# Databricks notebook source
# MAGIC %md #### 1.  Extracted the historical weather data

# COMMAND ----------

#Extracted the historical weather data from https://www.visualcrossing.com/weather/weather-data-services, fmi does not have historical data
import urllib.request
import sys
import json
                
try: 
  ResultBytes = urllib.request.urlopen("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Finland/2022-12-10/2023-12-13?unitGroup=metric&include=days&key=DAN9AESNZH5WS835QFJADVHCX&contentType=json")
  
  # Parse the results as JSON
  jsonData = json.load(ResultBytes)
        
except urllib.error.HTTPError  as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code, ErrorInfo)
  sys.exit()
except  urllib.error.URLError as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code,ErrorInfo)
  sys.exit()

# COMMAND ----------

import json
json_string = json.dumps(jsonData)

# COMMAND ----------

display(json_string)

# COMMAND ----------

# This is already executed, no need to run again.
# dbutils.fs.put("/dbfs/fmi/historical_weather.json", json_string)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import explode

json_df = spark.read.json("/dbfs/fmi/historical_weather.json")

# Explode the 'days' array
exploded_df = json_df.select("latitude", "longitude", "resolvedAddress", "timezone", "tzoffset", explode("days").alias("day"))

# Extract datetime, feelslike, and pressure from the exploded DataFrame
weather_df = exploded_df.select(
                                col("latitude"),\
                                col("longitude"),\
                                col("resolvedAddress"),\
                                col("timezone"),\
                                col("tzoffset"),\
                                col("day.datetime").alias("datetime"),\
                                col("day.feelslike").alias("feelslike"),\
                                col("day.pressure").alias("pressure"),\
                                col("day.temp").alias("temperature"),\
                                col("day.snow").alias("snow"),\
                                col("day.windspeed").alias("windspeed"),\
                                col("day.visibility").alias("visibility")
                            )

# COMMAND ----------

# Weather data
display(weather_df)

# COMMAND ----------

# MAGIC %md #### 2. Mount Azure Data Lake storage using Service Principal
# MAGIC
# MAGIC

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
##### Mount csv Container
mount_adls('ewqstorageaccount', 'csv')

# COMMAND ----------

### List all files from storage account container 
display(dbutils.fs.ls("/mnt/ewqstorageaccount/csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Reading EWQ CSV files into dataframe

# COMMAND ----------

### Read the unnamed_store file 
df1 = spark.read.option("header", "true").option("delimiter",";").csv("/mnt/ewqstorageaccount/csv/unnamed_store.csv") 
df2 = spark.read.option("header", "true").option("delimiter",";").csv("/mnt/ewqstorageaccount/csv/unnamed_store_2.csv") 

### combining 2 csv files from storage account 
df = df1.union(df2)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Joining EWQ Dataframe and weather dataframe

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
df = df.withColumn("date", date_format(from_utc_timestamp("time_generated", "UTC"), "yyyy-MM-dd"))
joined_df = df.join(weather_df, df["date"] == weather_df["datetime"], "inner")
display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Estimated waiting time in minutes

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,expr,round
estimated_waiting_time = joined_df.withColumn(
    "estimated_waiting_time",
   round(expr("unix_timestamp(time_called) - unix_timestamp(time_generated)")/60,2)
)
columns_to_drop = ["master_code", "group_number", "ticket_uuid", "time_processed", "dur_approach", "status", "resolution", "lang_code", "external_id", "mobile_tracked", "priority", "metadata", "resolvedAddress"]
estimated_waiting_time = estimated_waiting_time.drop(*columns_to_drop)
display(estimated_waiting_time)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. calculating average waiting time per area code

# COMMAND ----------

from pyspark.sql.functions import col, avg, round
avg_waiting_time_per_area = (estimated_waiting_time.orderBy(col("area_code")).groupBy("area_code").agg(round(avg("estimated_waiting_time"), 2).alias("avg_estimated_waiting_time")))

# Display the result
display(avg_waiting_time_per_area)

# COMMAND ----------

# Registering the resulting DataFrame as a temporary SQL table
avg_waiting_time_per_area.createOrReplaceTempView("avg_waiting_time_table")

# Using Databricks display function to visualize the results
display(spark.sql("SELECT * FROM avg_waiting_time_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Estimated Service time in minutes

# COMMAND ----------

from pyspark.sql.functions import col, round
estimated_service_time = joined_df.withColumn(
    "estimated_service_time", round(col("dur_service") / 60, 2)
)
columns_to_drop = ["master_code", "group_number", "ticket_uuid", "time_processed", "dur_approach", "status", "resolution", "lang_code", "external_id", "mobile_tracked", "priority", "metadata", "resolvedAddress"]

estimated_service_time = estimated_service_time.drop(*columns_to_drop)
# Display the DataFrame with the new column
display(estimated_service_time)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. calculating average service time per area code

# COMMAND ----------

from pyspark.sql.functions import col, avg, round
avg_service_time_per_area = (estimated_service_time.orderBy(col("area_code")).groupBy("area_code").agg(round(avg("estimated_service_time"), 2).alias("avg_estimated_service_time")))
# Display the result
display(avg_service_time_per_area)
