# Databricks notebook source
# ============================================================
# FINLAKE - BRONZE INGESTION
# ============================================================

SOURCE_BASE = "/Volumes/workspace/finlake_bronze/source_data"

METADATA_BASE = "/Volumes/workspace/finlake_monitoring/pipeline_metadata"

BRONZE_CATALOG = "workspace"
BRONZE_SCHEMA = "finlake_bronze"

print("Source:", SOURCE_BASE)
print("Metadata:", METADATA_BASE)
print("Bronze:", f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

customers_source = f"{SOURCE_BASE}/customers/"

display(
    dbutils.fs.ls(customers_source)
)

# COMMAND ----------

customers_test = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load(customers_source)
)

display(customers_test.limit(10))

# COMMAND ----------

print("Customer records:", customers_test.count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Now using Auto Loader

# COMMAND ----------

customers_checkpoint = (
    f"{METADATA_BASE}/customers/checkpoint"
)

customers_schema = (
    f"{METADATA_BASE}/customers/schema"
)

customers_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", customers_schema)
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.includeExistingFiles", "true")
    .load(customers_source)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion metadata

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    regexp_extract,
)


customers_bronze = (
    customers_stream
    .withColumn(
        "_ingest_timestamp",
        current_timestamp()
    )
    .withColumn(
        "_source_file",
        input_file_name()
    )
    .withColumn(
        "_batch_date",
        regexp_extract(
            input_file_name(),
            r"customers_(\d{4}-\d{2}-\d{2})\.csv",
            1
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing to Bronze Delta

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    current_timestamp,
    regexp_extract,
)

customers_bronze_fixed = (
    customers_stream
    .withColumn(
        "_ingest_timestamp",
        current_timestamp()
    )
    .withColumn(
        "_source_file",
        col("_metadata.file_path")
    )
    .withColumn(
        "_batch_date",
        regexp_extract(
            col("_metadata.file_path"),
            r"customers_(\d{4}-\d{2}-\d{2})\.csv",
            1
        )
    )
)

customer_query = (
    customers_bronze_fixed.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        customers_checkpoint
    )
    .trigger(availableNow=True)
    .toTable(
        "workspace.finlake_bronze.customers"
    )
)

customer_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verifying the customer table in bronze layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.finlake_bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_bronze.customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _batch_date,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM workspace.finlake_bronze.customers
# MAGIC GROUP BY _batch_date
# MAGIC ORDER BY _batch_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     _batch_date,
# MAGIC     _source_file,
# MAGIC     _ingest_timestamp
# MAGIC FROM workspace.finlake_bronze.customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the ingestion configuration table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.finlake_monitoring.ingestion_config (
# MAGIC     domain STRING,
# MAGIC     source_path STRING,
# MAGIC     target_table STRING,
# MAGIC     checkpoint_path STRING,
# MAGIC     schema_path STRING,
# MAGIC     file_format STRING,
# MAGIC     active BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.finlake_monitoring.ingestion_config;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO workspace.finlake_monitoring.ingestion_config
# MAGIC VALUES
# MAGIC (
# MAGIC     'customers',
# MAGIC     '/Volumes/workspace/finlake_bronze/source_data/customers/',
# MAGIC     'workspace.finlake_bronze.customers',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/customers/checkpoint/',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/customers/schema/',
# MAGIC     'csv',
# MAGIC     true
# MAGIC ),
# MAGIC (
# MAGIC     'accounts',
# MAGIC     '/Volumes/workspace/finlake_bronze/source_data/accounts/',
# MAGIC     'workspace.finlake_bronze.accounts',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/accounts/checkpoint/',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/accounts/schema/',
# MAGIC     'csv',
# MAGIC     true
# MAGIC ),
# MAGIC (
# MAGIC     'transactions',
# MAGIC     '/Volumes/workspace/finlake_bronze/source_data/transactions/',
# MAGIC     'workspace.finlake_bronze.transactions',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/transactions/checkpoint/',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/transactions/schema/',
# MAGIC     'csv',
# MAGIC     true
# MAGIC ),
# MAGIC (
# MAGIC     'cards',
# MAGIC     '/Volumes/workspace/finlake_bronze/source_data/cards/',
# MAGIC     'workspace.finlake_bronze.cards',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/cards/checkpoint/',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/cards/schema/',
# MAGIC     'csv',
# MAGIC     true
# MAGIC ),
# MAGIC (
# MAGIC     'loans',
# MAGIC     '/Volumes/workspace/finlake_bronze/source_data/loans/',
# MAGIC     'workspace.finlake_bronze.loans',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/loans/checkpoint/',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/loans/schema/',
# MAGIC     'csv',
# MAGIC     true
# MAGIC ),
# MAGIC (
# MAGIC     'payments',
# MAGIC     '/Volumes/workspace/finlake_bronze/source_data/payments/',
# MAGIC     'workspace.finlake_bronze.payments',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/payments/checkpoint/',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/payments/schema/',
# MAGIC     'csv',
# MAGIC     true
# MAGIC ),
# MAGIC (
# MAGIC     'events',
# MAGIC     '/Volumes/workspace/finlake_bronze/source_data/events/',
# MAGIC     'workspace.finlake_bronze.events',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/events/checkpoint/',
# MAGIC     '/Volumes/workspace/finlake_monitoring/pipeline_metadata/events/schema/',
# MAGIC     'csv',
# MAGIC     true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.ingestion_config
# MAGIC ORDER BY domain;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE workspace.finlake_monitoring.ingestion_config
# MAGIC SET active = false
# MAGIC WHERE domain = 'customers';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     domain,
# MAGIC     active
# MAGIC FROM workspace.finlake_monitoring.ingestion_config
# MAGIC ORDER BY domain;

# COMMAND ----------

