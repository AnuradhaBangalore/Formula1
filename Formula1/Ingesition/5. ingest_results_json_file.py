# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest results json file
# MAGIC ##Read json using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                   StructField("raceId",IntegerType(), True),
                                   StructField("driverId",IntegerType(), True),
                                   StructField("constructorId",IntegerType(), True),
                                   StructField("number",IntegerType(), True),
                                   StructField("grid",IntegerType(), True),
                                   StructField("position",IntegerType(), True),
                                   StructField("positionText",StringType(), True),
                                   StructField("positionOrder",IntegerType(), True),
                                   StructField("points", FloatType(), True),
                                   StructField("laps",IntegerType(),True),
                                   StructField("time",StringType(), True),
                                   StructField("milliseconds",IntegerType(), True),
                                   StructField("fastestLap",IntegerType(), True),
                                   StructField("rank",IntegerType(), True),
                                   StructField("fastestLapTime",StringType(), True),
                                   StructField("fastestLapSpeed", FloatType(), True),
                                   StructField("statusId",StringType(),True)
                                   ])

# COMMAND ----------

display(results_schema)

# COMMAND ----------

results_df=spark.read.schema(results_schema).json("/mnt/formula1dl12/raw/results.json")

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp

# COMMAND ----------

rename_results_df=results_df.withColumnRenamed("resultId","result_id") \
                            .withColumnRenamed("raceId","race_id") \
                            .withColumnRenamed("driverId","driver_id") \
                            .withColumnRenamed("constructorId","constructor_id") \
                            .withColumnRenamed("positionText","position_text") \
                            .withColumnRenamed("positionOrder","position_order") \
                            .withColumnRenamed("fastestLap","fastest_lap") \
                            .withColumnRenamed("fastestLapTime","fastest_lap_Time") \
                            .withColumnRenamed("fastestLapSpeed","fastest_lap_Speed") \
                            .drop("statusId","status_id") \
                            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(rename_results_df)

# COMMAND ----------

rename_results_df.write.mode('overwrite').parquet("/mnt/formula1dl12/processed/results")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl12/processed/results"))

# COMMAND ----------


