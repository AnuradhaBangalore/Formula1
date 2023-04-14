# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest races.csv file 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl12/raw"))

# COMMAND ----------

races_df = (spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/formula1dl12/raw/races.csv"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DoubleType, TimestampType

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                     StructField("year", StringType(), True),
                                     StructField("round", StringType(), True),
                                     StructField("circuitId", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DoubleType(), True),
                                     StructField("time", DoubleType(), True),
                                     StructField("url", StringType(), True)
                                     ]
                            )

# COMMAND ----------

display(races_df = spark.read.option("header", True).schema(races_schema).csv("dbfs:/mnt/formula1dl12/raw/races.csv"))

# COMMAND ----------

# MAGIC %fs ls /mnt/formula1dl12/raw

# COMMAND ----------

dbfs:/mnt/formula1dl12/raw/races.csv

# COMMAND ----------

type(races_df)

# COMMAND ----------

races_df.show()

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####select only required fields from the data frame

# COMMAND ----------

races_selected_df = races_df.select("raceId", "year", "round","circuitId","name","date","time")

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df = races_df.select(races_df["raceId"], 
                                          races_df["year"],
                                          races_df["round"],
                                          races_df["circuitId"],
                                          races_df["name"],
                                          races_df["date"],
                                          races_df["time"])


# COMMAND ----------

from pyspark.sql.functions import  current_timestamp, to_timestamp, col, lit, concat

# COMMAND ----------

races_with_timestamp_df = races_selected_df.withColumn("ingestion_date",current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_final_select_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), 
                                                       col("year").alias("race_year"), 
                                                       col("round"),
                                                       col("circuitId").alias("circuit_id"),
                                                       col("name"),
                                                       col("ingestion_date"),
                                                       col("race_timestamp"))

# COMMAND ----------

display(races_final_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####write dataframe to datalake in parque format

# COMMAND ----------

races_final_select_df.write.parquet("/mnt/formula1dl12/processed/races")

# COMMAND ----------

races_final_select_df.write.mode('overwrite').partitionBy('race_year').parquet("/mnt/formula1dl12/processed/races")

# COMMAND ----------

df=spark.read.parquet("/mnt/formula1dl12/processed/races")

# COMMAND ----------

display(df)

# COMMAND ----------


