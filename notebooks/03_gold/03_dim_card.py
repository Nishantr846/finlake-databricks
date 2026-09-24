# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Gold Card Dimension
# MAGIC
# MAGIC Build `dim_card` from Silver cards.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.finlake_silver.cards"
TARGET_TABLE = "workspace.finlake_gold.dim_card"

df = (
    spark.table(SOURCE_TABLE).alias("s")
    .join(
        spark.table("workspace.finlake_gold.dim_account").alias("a"),
        F.col("s.account_id") == F.col("a.account_id"),
        "inner"
    )
    .select(
        F.col("s.card_id"),
        F.col("s.account_id"),
        F.col("a.customer_id"),
        F.col("s.card_type"),
        F.col("s.card_status"),
        F.col("s.credit_limit"),
        F.col("s.issue_date"),
        F.col("s._batch_date")
    )
    .withColumn("card_key", F.sha2("card_id", 256))
)

df = df.select(
    "card_key","card_id","account_id","customer_id","card_type","card_status",
    "credit_limit","issue_date","_batch_date"
)

df.write.format("delta").mode("overwrite").option(
    "overwriteSchema","true"
).saveAsTable(TARGET_TABLE)

print(f"Created {TARGET_TABLE}")


# COMMAND ----------

count = spark.table(TARGET_TABLE).count()
print(f"Gold card records: {count:,}")
assert count == 8299

checks = spark.sql("""
SELECT
  SUM(CASE WHEN a.account_id IS NULL THEN 1 ELSE 0 END) AS orphan_accounts,
  SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS orphan_customers
FROM workspace.finlake_gold.dim_card d
LEFT JOIN workspace.finlake_gold.dim_account a ON d.account_id = a.account_id
LEFT JOIN workspace.finlake_gold.dim_customer c ON d.customer_id = c.customer_id
""").first()

print(checks)
assert checks["orphan_accounts"] == 0
assert checks["orphan_customers"] == 0


# COMMAND ----------

