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
# MAGIC ## Domain: Cards
# MAGIC
# MAGIC **Bronze:** `workspace.finlake_bronze.cards`
# MAGIC
# MAGIC **Silver:** `workspace.finlake_silver.cards`
# MAGIC
# MAGIC Cards belong to accounts, so the pipeline also validates the `account_id` relationship.

# COMMAND ----------

# ============================================================
# FINLAKE - SILVER CARDS
# ============================================================

BRONZE_TABLE = "workspace.finlake_bronze.cards"
SILVER_TABLE = "workspace.finlake_silver.cards"

bronze_cards = spark.table(BRONZE_TABLE)

print(f"Bronze card records: {bronze_cards.count():,}")
display(bronze_cards.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect the Bronze schema
# MAGIC
# MAGIC Bronze columns are intentionally raw. We will explicitly cast and standardize them in Silver.

# COMMAND ----------

bronze_cards.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import transformation functions

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_date,
    to_timestamp,
    current_timestamp,
    row_number,
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean and standardize
# MAGIC
# MAGIC Card numbers are deliberately represented only by their last four digits in the source model. We retain that masked representation.

# COMMAND ----------

silver_cards = (
    bronze_cards
    .withColumn("card_id", trim(col("card_id")))
    .withColumn("account_id", trim(col("account_id")))
    .withColumn("card_type", upper(trim(col("card_type"))))
    .withColumn("card_network", upper(trim(col("card_network"))))
    .withColumn("card_last_four", trim(col("card_last_four")))
    .withColumn("card_status", upper(trim(col("card_status"))))
    .withColumn("issue_date", to_date(col("issue_date")))
    .withColumn("expiry_date", to_date(col("expiry_date")))
    .withColumn("credit_limit", col("credit_limit").cast("decimal(18,2)"))
    .withColumn("created_at", to_timestamp(col("created_at")))
    .withColumn("_batch_date", to_date(col("_batch_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicate to the latest card version
# MAGIC
# MAGIC Three source batches currently contain repeated business keys. We retain the latest record for each `card_id`.

# COMMAND ----------

card_window = (
    Window
    .partitionBy("card_id")
    .orderBy(
        col("_batch_date").desc(),
        col("_ingest_timestamp").desc()
    )
)

silver_cards_current = (
    silver_cards
    .withColumn("_row_number", row_number().over(card_window))
    .filter(col("_row_number") == 1)
    .drop("_row_number")
)

print(f"Silver current-state cards: {silver_cards_current.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build the final Silver schema

# COMMAND ----------

silver_cards_final = (
    silver_cards_current
    .select(
        "card_id",
        "account_id",
        "card_type",
        "card_network",
        "card_last_four",
        "card_status",
        "issue_date",
        "expiry_date",
        "credit_limit",
        "_batch_date",
        "_ingest_timestamp",
        "_source_file"
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

display(silver_cards_final.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write the Silver Delta table

# COMMAND ----------

(
    silver_cards_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Created/updated: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate the Silver table

# COMMAND ----------

silver_count = spark.table(SILVER_TABLE).count()

print(f"Silver card records: {silver_count:,}")

assert silver_count == 8300

# COMMAND ----------

spark.sql(f'''
SELECT card_id, COUNT(*) AS record_count
FROM {SILVER_TABLE}
GROUP BY card_id
HAVING COUNT(*) > 1
''').show()

print("Duplicate card IDs should return no rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Account → Card relationship

# COMMAND ----------

spark.sql(f'''
SELECT COUNT(*) AS orphan_cards
FROM {SILVER_TABLE} c
LEFT JOIN workspace.finlake_silver.accounts a
    ON c.account_id = a.account_id
WHERE a.account_id IS NULL
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validate business fields

# COMMAND ----------

spark.sql(f'''
SELECT card_type, COUNT(*) AS cards
FROM {SILVER_TABLE}
GROUP BY card_type
ORDER BY cards DESC
''').show()

spark.sql(f'''
SELECT card_status, COUNT(*) AS cards
FROM {SILVER_TABLE}
GROUP BY card_status
ORDER BY cards DESC
''').show()

spark.sql(f'''
SELECT COUNT(*) AS negative_credit_limits
FROM {SILVER_TABLE}
WHERE credit_limit < 0
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Final schema check

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE {SILVER_TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion criteria
# MAGIC
# MAGIC - [ ] 8,000 current-state card records
# MAGIC - [ ] No duplicate `card_id`
# MAGIC - [ ] No orphan `account_id`
# MAGIC - [ ] `credit_limit` is `DECIMAL(18,2)`
# MAGIC - [ ] `issue_date` and `expiry_date` are `DATE`
# MAGIC - [ ] `created_at` is `TIMESTAMP`
# MAGIC - [ ] Categorical values are standardized
# MAGIC
# MAGIC **Next domain:** Loans.