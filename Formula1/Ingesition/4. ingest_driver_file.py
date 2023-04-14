# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest drivers json file nested json
# MAGIC ##Read json using dataframe reader api
# MAGIC  

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                ]
                        )

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                   StructField("driverRef",StringType(), True),
                                   StructField("number",IntegerType(), True),
                                   StructField("code",StringType(), True),
                                   StructField("name", name_schema),
                                   StructField("dob",DateType(),True),
                                   StructField("nationality",StringType(), True),
                                   StructField("url",StringType(), True)
                                   ])

# COMMAND ----------


display(driver_schema)


# COMMAND ----------

driver_df=spark.read.schema(driver_schema).json("/mnt/formula1dl12/raw/drivers.json")

# COMMAND ----------

display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp, col, lit, concat

# COMMAND ----------

driver_rename = driver_df.withColumnRenamed("driverId","driver_id") \
                         .withColumnRenamed("driverRef","driver_ref") \
                         .withColumn("ingestion_date", current_timestamp()) \
                         .withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname")))

# COMMAND ----------

display(driver_rename)

# COMMAND ----------

driver_final_selected = driver_rename.drop(col("url"))

# COMMAND ----------

display(driver_final_selected)

# COMMAND ----------

driver_final_selected.write.mode('overwrite').parquet("/mnt/formula1dl12/processed/drivers")

# COMMAND ----------

df=spark.read.parquet("/mnt/formula1dl12/processed/drivers")

# COMMAND ----------

display(df)

# COMMAND ----------


