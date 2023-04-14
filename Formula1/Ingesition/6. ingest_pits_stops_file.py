# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pits_stop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId",IntegerType(), True),
                                     StructField("stop",StringType(), True),
                                     StructField("lap",IntegerType(), True),
                                     StructField("time",IntegerType(), True),
                                     StructField("duration",IntegerType(), True),
                                     StructField("milliseconds",IntegerType(), True)
                                    ])
           




# COMMAND ----------

display(pits_stop_schema)

# COMMAND ----------

pits_stop_df = spark.read.schema(pits_stop_schema).option("multiline",True).json("/mnt/formula1dl12/raw/pit_stops.json")

# COMMAND ----------

display(pits_stop_df)

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp

# COMMAND ----------

rename_pit_stops_df=pits_stop_df.withColumnRenamed("raceId","race_id") \
                                .withColumnRenamed("driverId","driver_id") \
                            .withColumn("ingestion_time",current_timestamp()) 

# COMMAND ----------

display(rename_pit_stops_df)

# COMMAND ----------

rename_pit_stops_df.write.mode('overwrite').parquet("/mnt/formula1dl12/processed/pit_stops")

# COMMAND ----------

display("/mnt/formula1dl12/processed/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl12/processed/pit_stops"))

# COMMAND ----------


