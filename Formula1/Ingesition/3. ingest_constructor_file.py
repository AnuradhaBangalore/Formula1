# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest constructor json file 

# COMMAND ----------

# MAGIC %md
# MAGIC ##read json file using spark datafram reader

# COMMAND ----------

constructor_schema= "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructor_df= spark.read.schema(constructor_schema).json("/mnt/formula1dl12/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop column from  spark datafram reader

# COMMAND ----------

constructor_drop_df = constructor_df.drop('url')

# COMMAND ----------

display(constructor_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rename column from  spark datafram and add ingestion timestamp column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df=constructor_drop_df.withColumnRenamed("constructorId","constructor_Id") \
                                        .withColumnRenamed("constructorRef","constructor_Ref") \
                                        .withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####write dataframe to datalake in parque format

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet("/mnt/formula1dl12/processed/constructor")

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------


