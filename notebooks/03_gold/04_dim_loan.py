# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Gold Loan Dimension
# MAGIC
# MAGIC Build `dim_loan` from Silver loans.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.finlake_silver.loans"
TARGET_TABLE = "workspace.finlake_gold.dim_loan"

df = (
    spark.table(SOURCE_TABLE).alias("s")
    .select(
        F.col("s.loan_id"),
        F.col("s.customer_id"),
        F.col("s.loan_type"),
        F.col("s.loan_status"),
        F.col("s.principal_amount"),
        F.col("s.outstanding_amount"),
        F.col("s.interest_rate"),
        F.col("s.tenure_months"),
        F.col("s.disbursement_date"),
        F.col("s._batch_date")
    )
    .withColumn("loan_key", F.sha2("loan_id", 256))
)
df = df.select(
    "loan_key",
    "loan_id",
    "customer_id",
    "loan_type",
    "loan_status",
    "principal_amount",
    "outstanding_amount",
    "interest_rate",
    "tenure_months",
    "disbursement_date",
    "_batch_date"
)

df.write.format("delta").mode("overwrite").option(
    "overwriteSchema","true"
).saveAsTable(TARGET_TABLE)

print(f"Created {TARGET_TABLE}")


# COMMAND ----------

count = spark.table(TARGET_TABLE).count()
print(f"Gold loan records: {count:,}")
assert count == 5250

orphans = spark.sql("""
SELECT COUNT(*) AS n
FROM workspace.finlake_gold.dim_loan l
LEFT JOIN workspace.finlake_gold.dim_customer c
  ON l.customer_id = c.customer_id
WHERE c.customer_id IS NULL
""").first()["n"]

print(f"Orphan loans: {orphans}")
assert orphans == 12


# COMMAND ----------

