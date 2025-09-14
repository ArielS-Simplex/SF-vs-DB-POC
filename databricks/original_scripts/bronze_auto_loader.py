# Databricks notebook source
# DBTITLE 1,Widget Variables Initialization
SOURCE_PATH = dbutils.widgets.get("SOURCE_PATH")
OPERATIONAL_VOLUME = dbutils.widgets.get("OPERATIONAL_VOLUME")
TARGET_TABLE = dbutils.widgets.get("TARGET_TABLE")
SP_NAME = SOURCE_PATH.split("/")[-1]

# COMMAND ----------

# DBTITLE 1,Load Utility Module
# MAGIC %run ./data_utility_modules

# COMMAND ----------

# DBTITLE 1,Print Schema Fields Data Types
# Initialize SchemaManager
schema_manager = SchemaManager(spark)

schema = schema_manager.get_schema(TARGET_TABLE)

# Print Schema
df_empty = spark.createDataFrame([], schema)
print(f"Schema of table: {TARGET_TABLE}")
df_empty.printSchema()


# COMMAND ----------

import os

cloud_provider = get_cloud_provider() 

if cloud_provider == "Azure":
    blob_storage = 'mlanalyticsstore01'
    print(f"Connecting to Azure Blob Storage {blob_storage}")
    key = dbutils.secrets.get(scope="azure-secret-scope", key='azure_analytics_blob_storage_key')
    spark.conf.set(f"fs.azure.account.key.{blob_storage}.dfs.core.windows.net", key)
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Reading and Processing Streaming Data with Timestamp
import os
import logging
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, col
from pyspark.sql.types import FloatType

# Set Python logging level to ERROR
logging.getLogger("py4j").setLevel(logging.ERROR)

# Set up paths dynamically
CHECKPOINT_PATH = os.path.join(OPERATIONAL_VOLUME, SP_NAME, "checkpoint")
BAD_RECORDS_PATH = os.path.join(OPERATIONAL_VOLUME, SP_NAME, "badRecordsPath")
SCHEMA_LOCATION = os.path.join(OPERATIONAL_VOLUME, SP_NAME, "schema")

print(f"Checkpoints Sink: {CHECKPOINT_PATH}")
print(f"Bad Records Sink: {BAD_RECORDS_PATH}")
print(f"Schema Evolution Sink: {SCHEMA_LOCATION}")
print(f"Reading data from: {SOURCE_PATH}")

def read_data_from_sink(spark, source_path):
    """ Reads data from the given source path using Spark streaming. """
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
            .option("cloudFiles.allowOverwrites", "true")
            .option("delimiter", "\t")
            .option("header", False)
            .option("escape", '"')
            # .option("badRecordsPath", BAD_RECORDS_PATH)
            .option("multiLine", "false")
            .option("encoding", "ISO-8859-1")
            .option("quote", '"')
            .schema(schema)
            .load(source_path + "/*")
            .withColumn("source_file_name", col("_metadata.file_name"))
            .withColumn("source_file_path", col("_metadata.file_path"))
            )


# Read data from source path
df = read_data_from_sink(spark, SOURCE_PATH)
df_final = df.withColumn("inserted_at", from_utc_timestamp(current_timestamp(), "GMT"))


# COMMAND ----------

# DBTITLE 1,Check and Create Delta Table if Not Exists
df_final.printSchema()

# Check if Delta table exists, if not create it with the schema provided
if not spark.catalog.tableExists(TARGET_TABLE):
    df_empty = spark.createDataFrame([], schema)
    df_empty.write.format("delta").saveAsTable(TARGET_TABLE)

# COMMAND ----------

# DBTITLE 1,Streaming Data to Delta Table in Unity Catalog
# Writing the streaming data to a Delta table in Unity Catalog
query = (
    df_final.writeStream
      .format("delta")
      .option("checkpointLocation", CHECKPOINT_PATH).trigger(once=True)
      .outputMode("append")
      .option("mergeSchema", "true")
      .table(TARGET_TABLE))

# COMMAND ----------

# DBTITLE 1,Await Streaming Query Termination
# Wait for termination
query.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Handle Streaming Query Status and Input Rows
from datetime import datetime

# Check if the stream failed
if query.exception():
    print("Query Status:", query.status)
    print("Stream failed with error:", query.exception())
else:
    # Extract numInputRows if no failure
    if query.lastProgress:
        num_input_rows = query.lastProgress["numInputRows"]
        print("Total number of input rows processed:", num_input_rows)
        if num_input_rows > 0:
            schema_manager.update_metadata(TARGET_TABLE, "checkpoint", str(datetime.now()))
    else:
        print("No progress recorded.")


# COMMAND ----------

# DBTITLE 1,- Optimize Delta Table and Display Result
# Optimize the target table
result = spark.sql(f"OPTIMIZE {TARGET_TABLE}")

display(result)
