# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Gold Account Dimension
# MAGIC
# MAGIC Build `dim_account` from Silver accounts.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.finlake_silver.accounts"
TARGET_TABLE = "workspace.finlake_gold.dim_account"

accounts = spark.table(SOURCE_TABLE)

customers = (
    spark.table("workspace.finlake_gold.dim_customer")
    .filter("is_current = true")
    .select("customer_id")
    .distinct()
)

# Keep only accounts with a valid customer relationship
account_dim = (
    accounts.alias("a")
    .join(
        customers.alias("c"),
        F.col("a.customer_id") == F.col("c.customer_id"),
        "inner"
    )
    .select(
        F.col("a.account_id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("a.account_status"),
        F.col("a.balance"),
        F.col("a.opening_date"),
        F.col("a.branch_code"),
        F.col("a._batch_date")
    )
    .withColumn(
        "account_key",
        F.sha2(F.col("account_id"), 256)
    )
    .select(
        "account_key",
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "balance",
        "opening_date",
        "branch_code",
        "_batch_date"
    )
)

(
    account_dim
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Created {TARGET_TABLE}")

# COMMAND ----------

count = spark.table(TARGET_TABLE).count()
print(f"Gold account records: {count:,}")
assert count == 15499

orphans = spark.sql("""
SELECT COUNT(*) AS n
FROM workspace.finlake_gold.dim_account a
LEFT JOIN workspace.finlake_gold.dim_customer c
  ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL
""").first()["n"]

print(f"Orphan accounts: {orphans}")
assert orphans == 0
