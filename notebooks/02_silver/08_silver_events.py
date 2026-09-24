# Databricks notebook source
# MAGIC %md
# MAGIC # FinLake — Silver Layer
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook transforms the raw Bronze Delta table into a trusted Silver table.
# MAGIC
# MAGIC The Silver layer performs:
# MAGIC - Type conversion
# MAGIC - String standardization
# MAGIC - Data normalization
# MAGIC - Current-state deduplication
# MAGIC - Referential-integrity validation
# MAGIC - Silver processing metadata
# MAGIC
# MAGIC > **Design principle:** Bronze preserves the ingested source data. Silver contains the latest trusted representation of each business entity.
# MAGIC
# MAGIC
# MAGIC ## Domain: Customer Events
# MAGIC
# MAGIC **Bronze:** `workspace.finlake_bronze.events`
# MAGIC
# MAGIC **Silver:** `workspace.finlake_silver.events`
# MAGIC
# MAGIC Events are semi-structured because the Bronze `metadata` column contains a string representation of an event-specific dictionary.
# MAGIC
# MAGIC Silver will:
# MAGIC - Standardize event fields
# MAGIC - Convert the event timestamp
# MAGIC - Parse the metadata string into JSON
# MAGIC - Extract common metadata attributes
# MAGIC - Deduplicate events
# MAGIC - Validate the customer relationship

# COMMAND ----------

BRONZE_TABLE = "workspace.finlake_bronze.events"
SILVER_TABLE = "workspace.finlake_silver.events"

bronze_events = spark.table(BRONZE_TABLE)

print(f"Bronze event records: {bronze_events.count():,}")
display(bronze_events.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect the Bronze schema and sample metadata

# COMMAND ----------

bronze_events.printSchema()

display(
    bronze_events.select(
        "event_id",
        "event_type",
        "metadata"
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import transformation functions

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_timestamp,
    to_date,
    current_timestamp,
    row_number,
    regexp_replace,
    from_json,
    get_json_object,
    when,
    lit,
)
from pyspark.sql.types import StringType, MapType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Standardize event fields
# MAGIC
# MAGIC The generator writes Python-dict-like strings such as:
# MAGIC
# MAGIC `{'ip': '...', 'browser': 'Chrome'}`
# MAGIC
# MAGIC JSON requires double quotes, so we normalize the string before parsing it.

# COMMAND ----------

silver_events_base = (
    bronze_events
    .withColumn("event_id", trim(col("event_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("event_type", upper(trim(col("event_type"))))
    .withColumn("device", upper(trim(col("device"))))
    .withColumn("channel", upper(trim(col("channel"))))
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    .withColumn("_batch_date", to_date(col("_batch_date")))
    .withColumn(
        "_metadata_json",
        regexp_replace(
            col("metadata"),
            "'",
            '"'
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Parse semi-structured metadata
# MAGIC
# MAGIC We use a generic `MAP<STRING,STRING>` because different event types contain different keys.

# COMMAND ----------

metadata_schema = MapType(
    StringType(),
    StringType()
)

silver_events_parsed = (
    silver_events_base
    .withColumn(
        "metadata_map",
        from_json(
            col("_metadata_json"),
            metadata_schema
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extract commonly useful metadata
# MAGIC
# MAGIC The event-specific map remains available, while common fields are exposed as queryable columns.
# MAGIC
# MAGIC Because event metadata has different structures, fields that do not apply to an event type remain null.

# COMMAND ----------

silver_events_enriched = (
    silver_events_parsed

    .withColumn(
        "ip_address",
        col("metadata_map")["ip"]
    )

    .withColumn(
        "browser",
        col("metadata_map")["browser"]
    )

    .withColumn(
        "transfer_type",
        col("metadata_map")["transfer_type"]
    )

    .withColumn(
        "payment_type",
        col("metadata_map")["payment_type"]
    )

    .withColumn(
        "loan_type",
        col("metadata_map")["loan_type"]
    )

    .withColumn(
        "card_type",
        col("metadata_map")["card_type"]
    )

    .withColumn(
        "field_updated",
        col("metadata_map")["field_updated"]
    )

    .withColumn(
        "event_metadata_amount",
        col("metadata_map")["amount"].cast("decimal(18,2)")
    )

    .withColumn(
        "requested_amount",
        col("metadata_map")["requested_amount"].cast("decimal(18,2)")
    )

    .withColumn(
        "password_change_method",
        col("metadata_map")["method"]
    )

    .withColumn(
        "card_block_reason",
        col("metadata_map")["reason"]
    )

    .withColumn(
        "beneficiary_type",
        col("metadata_map")["beneficiary_type"]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Deduplicate events

# COMMAND ----------

event_window = (
    Window
    .partitionBy("event_id")
    .orderBy(
        col("_batch_date").desc(),
        col("_ingest_timestamp").desc()
    )
)

silver_events_current = (
    silver_events_enriched
    .withColumn("_row_number", row_number().over(event_window))
    .filter(col("_row_number") == 1)
    .drop("_row_number")
)

print(f"Silver current-state events: {silver_events_current.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Build the final Silver schema

# COMMAND ----------

silver_events_final = (
    silver_events_current
    .select(
        "event_id",
        "customer_id",
        "event_type",
        "event_timestamp",
        "device",
        "channel",
        "metadata_map",
        "ip_address",
        "browser",
        "transfer_type",
        "payment_type",
        "loan_type",
        "card_type",
        "field_updated",
        "event_metadata_amount",
        "requested_amount",
        "password_change_method",
        "card_block_reason",
        "beneficiary_type",
        "_batch_date",
        "_ingest_timestamp",
        "_source_file"
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

display(silver_events_final.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write the Silver Delta table

# COMMAND ----------

(
    silver_events_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Created/updated: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validate record count and uniqueness

# COMMAND ----------

silver_count = spark.table(SILVER_TABLE).count()
print(f"Silver event records: {silver_count:,}")
assert silver_count == 60000, f"Expected 50,000 events, found {silver_count:,}"

spark.sql(f'''
SELECT event_id, COUNT(*) AS record_count
FROM {SILVER_TABLE}
GROUP BY event_id
HAVING COUNT(*) > 1
''').show()

print("Duplicate event IDs should return no rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validate Customer → Event relationship

# COMMAND ----------

spark.sql(f'''
SELECT COUNT(*) AS orphan_events
FROM {SILVER_TABLE} e
LEFT JOIN workspace.finlake_silver.customers c
    ON e.customer_id = c.customer_id
WHERE c.customer_id IS NULL
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validate event types

# COMMAND ----------

spark.sql(f'''
SELECT event_type, COUNT(*) AS events
FROM {SILVER_TABLE}
GROUP BY event_type
ORDER BY events DESC
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validate event metadata parsing

# COMMAND ----------

spark.sql(f'''
SELECT
    event_type,
    COUNT(*) AS events,
    COUNT(metadata_map) AS parsed_metadata
FROM {SILVER_TABLE}
GROUP BY event_type
ORDER BY events DESC
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Inspect semi-structured fields
# MAGIC
# MAGIC For example, transfer events should expose `transfer_type` and an amount when those fields exist in the source metadata.

# COMMAND ----------

spark.sql(f'''
SELECT
    event_id,
    customer_id,
    event_type,
    transfer_type,
    payment_type,
    event_metadata_amount,
    requested_amount,
    _batch_date
FROM {SILVER_TABLE}
WHERE event_type IN ('TRANSFER', 'PAYMENT', 'LOAN_APPLICATION')
LIMIT 30
''').show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Final schema

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE {SILVER_TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion criteria
# MAGIC
# MAGIC - [ ] 50,000 current-state event records
# MAGIC - [ ] No duplicate `event_id`
# MAGIC - [ ] No orphan `customer_id`
# MAGIC - [ ] `event_timestamp` is `TIMESTAMP`
# MAGIC - [ ] Event metadata is parsed into a Map
# MAGIC - [ ] Common metadata attributes are queryable
# MAGIC - [ ] Event types are standardized
# MAGIC
# MAGIC **Silver domain transformations are now complete.**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'customers' AS table_name,
# MAGIC     COUNT(*) AS records
# MAGIC FROM workspace.finlake_silver.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'accounts',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.finlake_silver.accounts
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'transactions',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.finlake_silver.transactions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'cards',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.finlake_silver.cards
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'loans',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.finlake_silver.loans
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'payments',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.finlake_silver.payments
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'events',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.finlake_silver.events;

# COMMAND ----------

