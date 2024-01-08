# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.ewqstorageaccount.dfs.core.windows.net",
    "/tByYkwT3ev+fJ5wsybOj2rVT6ma8cNh/gCw+ZV4vxLsCW+j98rYcjRfyvCsv5Fpjzo7ehp3tzYb+AStDPMAgw==")



# COMMAND ----------

# COMMAND ----------

display(dbutils.fs.ls("abfss://csv@ewqstorageaccount.dfs.core.windows.net"))

# COMMAND ----------


# COMMAND ----------

display(spark.read.csv("abfss://csv@ewqstorageaccount.dfs.core.windows.net/unnamed_store.csv"))

# COMMAND ----------

DataFrame1 = spark.read \
    .option("header", "true").option("delimiter",";").csv("abfss://csv@ewqstorageaccount.dfs.core.windows.net/unnamed_store.csv") 

# COMMAND ----------

DataFrame2 = spark.read \
    .option("header", "true").option("delimiter",";").csv("abfss://csv@ewqstorageaccount.dfs.core.windows.net/unnamed_store_2.csv") 

# COMMAND ----------

DataFrame = DataFrame1.union(DataFrame2)

# COMMAND ----------

display(DataFrame)

# COMMAND ----------

from pyspark.sql.functions import expr,col
df_estimated_time_in_secs = DataFrame \
    .withColumn("estimated_waiting_time_secs", expr("(time_called)- (time_generated)"))
df_estimated_time_in_mins = df_estimated_time_in_secs\
    .withColumn("estimated_waiting_time_minutes", (col("estimated_waiting_time_secs") / 60))

# COMMAND ----------

display(df_estimated_time_in_secs)

# COMMAND ----------

display(df_estimated_time_in_mins)



# COMMAND ----------

df_estimated_time_in_secs = DataFrame.withColumn("estimated_waiting_time_secs", expr("unix_timestamp(time_called)- unix_timestamp(time_generated)"))

# COMMAND ----------

display(DataFrame1.count())

# COMMAND ----------

display(DataFrame2.count())

# COMMAND ----------

display(DataFrame.count())
