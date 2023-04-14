# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest circuit.csv file 

# COMMAND ----------

# MAGIC %run "../Includes/configurations"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(spark.read.csv(f"{raw_folder_path}"))

# COMMAND ----------

circuits_df = (spark.read.option("header",True).option("inferSchema",True).csv(f"{raw_folder_path}/circuits.csv"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("url", StringType(), True)
                                     ]
                            )

# COMMAND ----------

display(circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_folder_path}/circuits.csv"))

# COMMAND ----------

# MAGIC %fs ls /mnt/formula1dl12/raw

# COMMAND ----------

dbfs:/mnt/formula1dl12/raw/circuits.csv

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####select only required fields from the data frame

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name","location","country","lat","lng")

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"], 
                                          circuits_df["circuitRef"],
                                          circuits_df["name"],
                                          circuits_df["location"],
                                          circuits_df["country"],
                                          circuits_df["lat"],
                                          circuits_df["lng"])
                                        

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"),col("location"),col("country"),col("lat"),col("lng"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_Id") \
                                          .withColumnRenamed("circuitRef","circuit_Ref") \
                                          .withColumnRenamed("lat","latitude") \
                                          .withColumnRenamed("lng","longituide") 

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####write dataframe to datalake in parque format

# COMMAND ----------

circuits_final_df.write.parquet("/mnt/formula1dl12/processed/circuits")

# COMMAND ----------

df=spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------


