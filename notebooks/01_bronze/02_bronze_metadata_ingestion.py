# Databricks notebook source
# ============================================================
# FINLAKE - METADATA-DRIVEN BRONZE INGESTION
# ============================================================

CONFIG_TABLE = "workspace.finlake_monitoring.ingestion_config"

config_df = (
    spark.table(CONFIG_TABLE)
    .filter("active = true")
)

display(config_df)

# COMMAND ----------

configs = (
    config_df
    .select(
        "domain",
        "source_path",
        "target_table",
        "checkpoint_path",
        "schema_path",
        "file_format"
    )
    .collect()
)

print(f"Active ingestion pipelines: {len(configs)}")

for config in configs:
    print(
        f"{config['domain']:15} "
        f"→ {config['target_table']}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the generic ingestion function

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    current_timestamp,
    regexp_extract,
)


def ingest_to_bronze(config):

    domain = config["domain"]
    source_path = config["source_path"]
    target_table = config["target_table"]
    checkpoint_path = config["checkpoint_path"]
    schema_path = config["schema_path"]
    file_format = config["file_format"]

    print("=" * 70)
    print(f"Starting ingestion: {domain}")
    print(f"Source      : {source_path}")
    print(f"Target      : {target_table}")
    print("=" * 70)

    # --------------------------------------------------------
    # Auto Loader
    # --------------------------------------------------------

    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            file_format
        )
        .option(
            "header",
            "true"
        )
        .option(
            "cloudFiles.schemaLocation",
            schema_path
        )
        .option(
            "cloudFiles.inferColumnTypes",
            "false"
        )
        .option(
            "cloudFiles.includeExistingFiles",
            "true"
        )
        .load(source_path)
    )

    # --------------------------------------------------------
    # Ingestion metadata
    # --------------------------------------------------------

    bronze_df = (
        stream_df
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
                rf"{domain}_(\d{{4}}-\d{{2}}-\d{{2}})\.csv",
                1
            )
        )
    )

    # --------------------------------------------------------
    # Write Bronze Delta table
    # --------------------------------------------------------

    query = (
        bronze_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .trigger(
            availableNow=True
        )
        .toTable(
            target_table
        )
    )

    query.awaitTermination()

    print(f"Completed ingestion: {domain}")

# COMMAND ----------

# DBTITLE 1,Run ingestion loop
for config in configs:

    try:
        ingest_to_bronze(config)
    except Exception as e:
        print(f"Schema evolved for {config['domain']}, retrying: {e}")
        for q in spark.streams.active:
            q.stop()
        ingest_to_bronze(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verifying the Ingestion into bronze layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.finlake_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers' AS table_name,
# MAGIC        COUNT(*) AS record_count
# MAGIC FROM workspace.finlake_bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'accounts',
# MAGIC        COUNT(*)
# MAGIC FROM workspace.finlake_bronze.accounts
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'transactions',
# MAGIC        COUNT(*)
# MAGIC FROM workspace.finlake_bronze.transactions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'cards',
# MAGIC        COUNT(*)
# MAGIC FROM workspace.finlake_bronze.cards
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'loans',
# MAGIC        COUNT(*)
# MAGIC FROM workspace.finlake_bronze.loans
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'payments',
# MAGIC        COUNT(*)
# MAGIC FROM workspace.finlake_bronze.payments
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'events',
# MAGIC        COUNT(*)
# MAGIC FROM workspace.finlake_bronze.events;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verifying batch level ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _batch_date,
# MAGIC     COUNT(*) AS records
# MAGIC FROM workspace.finlake_bronze.transactions
# MAGIC GROUP BY _batch_date
# MAGIC ORDER BY _batch_date;

# COMMAND ----------

