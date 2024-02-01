# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
constructors_schema= StructType(fields=[StructField("constructorId", IntegerType(), True),
                                    StructField("constructorRef", StringType(), False),
                                    StructField("name", StringType(), False),
                                    StructField("nationality", StringType(), False),
                                    StructField("url", StringType(), False)
])

# COMMAND ----------


constructors_df = spark.read \
.option("header",True) \
.schema(constructors_schema) \
.json("dbfs:/mnt/ewqstorageaccount/raw/constructors.json")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_drop=constructors_df.drop("url")

# COMMAND ----------

display(constructors_drop)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
constructors_final = constructors_drop.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(constructors_final)

# COMMAND ----------

constructors_final.write.mode("overwrite").parquet("/mnt/ewqstorageaccount/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ewqstorageaccount/processed/constructors
