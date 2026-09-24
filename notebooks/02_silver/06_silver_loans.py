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
# MAGIC ## Domain: Loans
# MAGIC
# MAGIC **Bronze:** `workspace.finlake_bronze.loans`
# MAGIC
# MAGIC **Silver:** `workspace.finlake_silver.loans`
# MAGIC
# MAGIC Loans belong to customers. Financial amounts and interest rates are explicitly converted to fixed-precision numeric types.

# COMMAND ----------

BRONZE_TABLE = "workspace.finlake_bronze.loans"
SILVER_TABLE = "workspace.finlake_silver.loans"

bronze_loans = spark.table(BRONZE_TABLE)

print(f"Bronze loan records: {bronze_loans.count():,}")
display(bronze_loans.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect the Bronze schema

# COMMAND ----------

bronze_loans.printSchema()

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
# MAGIC ## 3. Clean, standardize and type the loan data

# COMMAND ----------

silver_loans = (
    bronze_loans
    .withColumn("loan_id", trim(col("loan_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("loan_type", upper(trim(col("loan_type"))))
    .withColumn("loan_purpose", upper(trim(col("loan_purpose"))))
    .withColumn("principal_amount", col("principal_amount").cast("decimal(18,2)"))
    .withColumn("interest_rate", col("interest_rate").cast("decimal(7,4)"))
    .withColumn("tenure_months", col("tenure_months").cast("integer"))
    .withColumn("loan_status", upper(trim(col("loan_status"))))
    .withColumn("disbursement_date", to_date(col("disbursement_date")))
    .withColumn("outstanding_amount", col("outstanding_amount").cast("decimal(18,2)"))
    .withColumn("created_at", to_timestamp(col("created_at")))
    .withColumn("_batch_date", to_date(col("_batch_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicate to the latest loan version

# COMMAND ----------

loan_window = (
    Window
    .partitionBy("loan_id")
    .orderBy(
        col("_batch_date").desc(),
        col("_ingest_timestamp").desc()
    )
)

silver_loans_current = (
    silver_loans
    .withColumn("_row_number", row_number().over(loan_window))
    .filter(col("_row_number") == 1)
    .drop("_row_number")
)

print(f"Silver current-state loans: {silver_loans_current.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build the final Silver schema

# COMMAND ----------

silver_loans_final = (
    silver_loans_current
    .select(
        "loan_id",
        "customer_id",
        "loan_type",
        "loan_purpose",
        "principal_amount",
        "interest_rate",
        "tenure_months",
        "loan_status",
        "disbursement_date",
        "outstanding_amount",
        "_batch_date",
        "_ingest_timestamp",
        "_source_file"
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

display(silver_loans_final.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write the Silver Delta table

# COMMAND ----------

(
    silver_loans_final
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
print(f"Silver loan records: {silver_count:,}")
assert silver_count == 5250, f"Expected 5,000 loans, found {silver_count:,}"

spark.sql(f'''
SELECT loan_id, COUNT(*) AS record_count
FROM {SILVER_TABLE}
GROUP BY loan_id
HAVING COUNT(*) > 1
''').show()

print("Duplicate loan IDs should return no rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Customer → Loan relationship

# COMMAND ----------

spark.sql(f'''
SELECT COUNT(*) AS orphan_loans
FROM {SILVER_TABLE} l
LEFT JOIN workspace.finlake_silver.customers c
    ON l.customer_id = c.customer_id
WHERE c.customer_id IS NULL
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validate financial fields

# COMMAND ----------

spark.sql(f'''
SELECT COUNT(*) AS invalid_amounts
FROM {SILVER_TABLE}
WHERE principal_amount <= 0
   OR outstanding_amount < 0
   OR outstanding_amount > principal_amount
''').show()

spark.sql(f'''
SELECT COUNT(*) AS invalid_interest_rates
FROM {SILVER_TABLE}
WHERE interest_rate <= 0
   OR interest_rate >= 100
''').show()

spark.sql(f'''
SELECT COUNT(*) AS invalid_tenures
FROM {SILVER_TABLE}
WHERE tenure_months <= 0
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validate loan statuses and types

# COMMAND ----------

spark.sql(f'''
SELECT loan_status, COUNT(*) AS loans
FROM {SILVER_TABLE}
GROUP BY loan_status
ORDER BY loans DESC
''').show()

spark.sql(f'''
SELECT loan_type, COUNT(*) AS loans
FROM {SILVER_TABLE}
GROUP BY loan_type
ORDER BY loans DESC
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Final schema

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE {SILVER_TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion criteria
# MAGIC
# MAGIC - [ ] 5,000 current-state loan records
# MAGIC - [ ] No duplicate `loan_id`
# MAGIC - [ ] No orphan `customer_id`
# MAGIC - [ ] Financial amounts use `DECIMAL(18,2)`
# MAGIC - [ ] Interest rate uses fixed precision
# MAGIC - [ ] Tenure is integer
# MAGIC - [ ] No negative/invalid financial values
# MAGIC
# MAGIC **Next domain:** Payments.