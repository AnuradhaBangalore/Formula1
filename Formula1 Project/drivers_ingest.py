# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True)
])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
.option("header",True) \
.schema(drivers_schema) \
.json("dbfs:/mnt/ewqstorageaccount/raw/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat,lit,col
df=drivers_df.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
             .withColumnRenamed("driverId","driver_id")\
             .withColumnRenamed("driverRef","driver_ref")\
             .drop("url")\
             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/ewqstorageaccount/processed/drivers")

# COMMAND ----------

# MAGIC %fs ls /mnt/ewqstorageaccount/processed/drivers

# COMMAND ----------

display(spark.read.parquet("/mnt/ewqstorageaccount/processed/drivers"))
