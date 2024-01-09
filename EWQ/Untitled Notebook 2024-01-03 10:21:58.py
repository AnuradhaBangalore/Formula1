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
# MAGIC #### 2) Reading CSV files into dataframe

# COMMAND ----------

### Read the unnamed_store file 
df1 = spark.read.option("header", "true").option("delimiter",";").csv("/mnt/ewqstorageaccount/csv/unnamed_store.csv") 
df2 = spark.read.option("header", "true").option("delimiter",";").csv("/mnt/ewqstorageaccount/csv/unnamed_store_2.csv") 

### combining 2 csv files from storage account 
df = df1.union(df2)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.types import TimestampType

# set the legacy time parser policy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Weather data
weather_df = weather_df.withColumn("formatted_datetime", unix_timestamp(col("datetime"),"yyyy-MM-dd").cast(TimestampType()))

df = df.withColumn("formatted_datetime", unix_timestamp(col("time_generated"),"yyyy-MM-dd").cast(TimestampType()))
df = df.withColumn("datetime", unix_timestamp(col("time_generated"), "yyyy-MM-dd'T'HH:mm:ss'Z'").cast(TimestampType()))

display(df)
display(weather_df)

# COMMAND ----------


# Perform an inner join on the "datetime" and "time_generated" columns
joined_df = weather_df.join(df, weather_df["formatted_datetime"] == df["time_generated1"], "inner")
display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Estimated waiting time in minutes

# COMMAND ----------

from pyspark.sql.functions import expr, col, unix_timestamp , count

df_estimated_time_in_secs = joined_df \
    .withColumn("estimated_waiting_time_secs", expr("unix_timestamp(time_called) - unix_timestamp(time_generated)")) 

#display(df_estimated_time_in_secs)

df_estimated_time_in_mins = df_estimated_time_in_secs \
    .withColumn("estimated_waiting_time_minutes", (col("estimated_waiting_time_secs") / 60))
display(df_estimated_time_in_mins)


# COMMAND ----------

display(df_estimated_time_in_mins)

# COMMAND ----------

##### calculating estiamted service in minutes

display(df.withColumn("ServiceDuration_Mins", col("dur_service") / 60))

# COMMAND ----------

# Calculate the total service duration in minutes and group by 'area_code' 'group_name' and 'desk_number'

from pyspark.sql.functions import col, sum, round, count

result_df = (
    df.withColumn("ServiceDuration_Mins", round(col("dur_service") / 60, 1)) 
      .groupBy("area_code", "group_name", "desk_number")
      .agg(sum("ServiceDuration_Mins").alias("Total_Service_Duration_Mins"),count("id").alias("Total_Number_of_Tokens"))
)

# Display the result
display(result_df)
