# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId",IntegerType(), True),
                                     StructField("lap",IntegerType(), True),
                                     StructField("position",IntegerType(), True),
                                     StructField("time",StringType(), True),
                                     StructField("milliseconds",IntegerType(), True)
                                    ])
           




# COMMAND ----------

display(lap_times_schema)

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv("/mnt/formula1dl12/raw/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp

# COMMAND ----------

rename_lap_times_df=lap_times_df.withColumnRenamed("raceId","race_id") \
                                .withColumnRenamed("driverId","driver_id") \
                            .withColumn("ingestion_time",current_timestamp()) 

# COMMAND ----------

display(rename_lap_times_df)

# COMMAND ----------

rename_lap_times_df.write.mode('overwrite').parquet("/mnt/formula1dl12/processed/lap_times")

# COMMAND ----------

display("/mnt/formula1dl12/processed/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl12/processed/lap_times"))

# COMMAND ----------


