# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'EWQ-scope', key = 'ewq-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'EWQ-scope', key = 'ewq-app-tenent-id')
    client_secret = dbutils.secrets.get(scope = 'EWQ-scope', key = 'ewq-app-client-secret')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('ewqstorageaccount', 'raw')

# COMMAND ----------

mount_adls('ewqstorageaccount', 'processed')
mount_adls('ewqstorageaccount', 'presentation')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/ewqstorageaccount/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC  |-- circuitId: string (nullable = true)
# MAGIC  |-- circuitRef: string (nullable = true)
# MAGIC  |-- name: string (nullable = true)
# MAGIC  |-- location: string (nullable = true)
# MAGIC  |-- country: string (nullable = true)
# MAGIC  |-- lat: string (nullable = true)
# MAGIC  |-- lng: string (nullable = true)
# MAGIC  |-- alt: string (nullable = true)
# MAGIC  |-- url: string (nullable = true)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), False),
                                    StructField("name", StringType(), False),
                                    StructField("location", StringType(), False),
                                    StructField("country", StringType(), False),
                                    StructField("lat", DoubleType(), False),
                                    StructField("lng", DoubleType(), False),
                                    StructField("alt", IntegerType(), False),
                                    StructField("url", StringType(), False)
])

# COMMAND ----------

circuits_df = spark.read \
.option("header",True) \
.schema(circuits_schema) \
.csv("dbfs:/mnt/ewqstorageaccount/raw/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

display(circuits_df)


# COMMAND ----------

circuits_df.drop("url")

# COMMAND ----------

circuits_df_drop= display(circuits_df.drop("url"))

# COMMAND ----------

circuits_df_drop.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df=circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), 
                                        col("location"), col("country"), col("lat"),
                                        col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()).withColumn("env",lit("production"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/ewqstorageaccount/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ewqstorageaccount/processed/circuits

# COMMAND ----------

df = spark.read.parquet("/mnt/ewqstorageaccount/processed/circuits")

# COMMAND ----------

display(df)
