# Databricks notebook source
# MAGIC %fs 
# MAGIC ls /mnt/ewqstorageaccount/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
races_schema =  StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url",StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header",True) \
.schema(races_schema) \
.csv("/mnt/ewqstorageaccount/raw/races.csv")

# COMMAND ----------

from pyspark.sql.functions import col
race_select= races_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("date"),col("time"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, col

races_timestamp_df=race_select.withColumn("ingestion_date", current_timestamp()) \
            .withColumn("race_timestamp", to_timestamp(concat('date', lit(' '), 'time'), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_timestamp_df)

# COMMAND ----------

df=races_timestamp_df.drop(col("date"),col("time"))

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/ewqstorageaccount/processed/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ewqstorageaccount/processed/races

# COMMAND ----------

display(spark.read.parquet("/mnt/ewqstorageaccount/processed/races"))
