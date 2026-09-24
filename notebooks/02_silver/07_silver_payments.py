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
# MAGIC ## Domain: Payments
# MAGIC
# MAGIC **Bronze:** `workspace.finlake_bronze.payments`
# MAGIC
# MAGIC **Silver:** `workspace.finlake_silver.payments`
# MAGIC
# MAGIC Payments reference both a loan and a customer. Silver validates both relationships and also checks that the payment's customer matches the loan owner.

# COMMAND ----------

BRONZE_TABLE = "workspace.finlake_bronze.payments"
SILVER_TABLE = "workspace.finlake_silver.payments"

bronze_payments = spark.table(BRONZE_TABLE)

print(f"Bronze payment records: {bronze_payments.count():,}")
display(bronze_payments.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect the Bronze schema

# COMMAND ----------

bronze_payments.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import transformation functions

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_date,
    current_timestamp,
    row_number,
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean and type the payment data

# COMMAND ----------

silver_payments = (
    bronze_payments
    .withColumn("payment_id", trim(col("payment_id")))
    .withColumn("loan_id", trim(col("loan_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("payment_amount", col("payment_amount").cast("decimal(18,2)"))
    .withColumn("payment_date", to_date(col("payment_date")))
    .withColumn("payment_status", upper(trim(col("payment_status"))))
    .withColumn("payment_method", upper(trim(col("payment_method"))))
    .withColumn("_batch_date", to_date(col("_batch_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicate payments

# COMMAND ----------

payment_window = (
    Window
    .partitionBy("payment_id")
    .orderBy(
        col("_batch_date").desc(),
        col("_ingest_timestamp").desc()
    )
)

silver_payments_current = (
    silver_payments
    .withColumn("_row_number", row_number().over(payment_window))
    .filter(col("_row_number") == 1)
    .drop("_row_number")
)

print(f"Silver current-state payments: {silver_payments_current.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build the final Silver schema

# COMMAND ----------

silver_payments_final = (
    silver_payments_current
    .select(
        "payment_id",
        "loan_id",
        "customer_id",
        "payment_amount",
        "payment_date",
        "payment_status",
        "payment_method",
        "_batch_date",
        "_ingest_timestamp",
        "_source_file"
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

display(silver_payments_final.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write the Silver Delta table

# COMMAND ----------

(
    silver_payments_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Created/updated: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate record count and uniqueness

# COMMAND ----------

silver_count = spark.table(SILVER_TABLE).count()
print(f"Silver payment records: {silver_count:,}")
assert silver_count == 25000, f"Expected 20,000 payments, found {silver_count:,}"

spark.sql(f'''
SELECT payment_id, COUNT(*) AS record_count
FROM {SILVER_TABLE}
GROUP BY payment_id
HAVING COUNT(*) > 1
''').show()

print("Duplicate payment IDs should return no rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Loan → Payment relationship

# COMMAND ----------

spark.sql(f'''
SELECT COUNT(*) AS orphan_payments
FROM {SILVER_TABLE} p
LEFT JOIN workspace.finlake_silver.loans l
    ON p.loan_id = l.loan_id
WHERE l.loan_id IS NULL
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validate Customer → Payment relationship

# COMMAND ----------

spark.sql(f'''
SELECT COUNT(*) AS orphan_payments
FROM {SILVER_TABLE} p
LEFT JOIN workspace.finlake_silver.customers c
    ON p.customer_id = c.customer_id
WHERE c.customer_id IS NULL
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validate Loan/Customer consistency
# MAGIC
# MAGIC A payment's customer should be the customer who owns the referenced loan.

# COMMAND ----------

spark.sql(f'''
SELECT COUNT(*) AS mismatched_payments
FROM {SILVER_TABLE} p
JOIN workspace.finlake_silver.loans l
    ON p.loan_id = l.loan_id
WHERE p.customer_id <> l.customer_id
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validate payment amounts and statuses

# COMMAND ----------

spark.sql(f'''
SELECT COUNT(*) AS invalid_amounts
FROM {SILVER_TABLE}
WHERE payment_amount <= 0
''').show()

spark.sql(f'''
SELECT payment_status, COUNT(*) AS payments
FROM {SILVER_TABLE}
GROUP BY payment_status
ORDER BY payments DESC
''').show()

spark.sql(f'''
SELECT payment_method, COUNT(*) AS payments
FROM {SILVER_TABLE}
GROUP BY payment_method
ORDER BY payments DESC
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Final schema

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE {SILVER_TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion criteria
# MAGIC
# MAGIC - [ ] 20,000 current-state payment records
# MAGIC - [ ] No duplicate `payment_id`
# MAGIC - [ ] No orphan `loan_id`
# MAGIC - [ ] No orphan `customer_id`
# MAGIC - [ ] No loan/customer mismatches
# MAGIC - [ ] `payment_amount` is `DECIMAL(18,2)`
# MAGIC - [ ] Payment status and method are standardized
# MAGIC
# MAGIC **Next domain:** Customer Events.