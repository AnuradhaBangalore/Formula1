# Databricks notebook source
# MAGIC %run "../Includes/configurations"  https://adb-5020662142272334.14.azuredatabricks.net/?o=5020662142272334#folder/1755924839135936

# COMMAND ----------

# MAGIC %md
# MAGIC ####Ingest qualifying json file
# MAGIC ##Read json using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                   StructField("raceId",IntegerType(), True),
                                   StructField("driverId",IntegerType(), True),
                                   StructField("constructorId",IntegerType(), True),
                                   StructField("number",IntegerType(), True),
                                   StructField("position",IntegerType(), True),
                                   StructField("q1",StringType(), True),
                                   StructField("q2",StringType(), True),
                                   StructField("q3",StringType(),True)
                                   ])



                            

# COMMAND ----------

display(qualifying_schema)

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema).option("multiline", True).json("/mnt/formula1dl12/raw/qualifying")

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp

# COMMAND ----------

rename_qualifying_df=qualifying_df.withColumnRenamed("resultId","result_id") \
                            .withColumnRenamed("raceId","race_id") \
                            .withColumnRenamed("driverId","driver_id") \
                            .withColumnRenamed("constructorId","constructor_id") \
                            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(rename_qualifying_df)

# COMMAND ----------

rename_qualifying_df.write.mode('overwrite').parquet("/mnt/formula1dl12/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl12/processed/qualifying"))

# COMMAND ----------


