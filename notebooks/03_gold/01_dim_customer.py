# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Gold Customer Dimension
# MAGIC
# MAGIC Build the Gold customer dimension from the Silver SCD Type 2 customer table.
# MAGIC
# MAGIC The dimension preserves historical customer versions and supports
# MAGIC point-in-time analytical queries.

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.finlake_silver.customer_scd2"
TARGET_TABLE = "workspace.finlake_gold.dim_customer"

customer_dim = (
    spark.table(SOURCE_TABLE)
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "customer_segment",
        "customer_status",
        "effective_from",
        "effective_to",
        "is_current",
        "scd_created_at"
    )
)

# COMMAND ----------

customer_dim = customer_dim.withColumn(
    "customer_key",
    F.sha2(
        F.concat_ws(
            "||",
            F.col("customer_id"),
            F.col("effective_from").cast("string")
        ),
        256
    )
)

# COMMAND ----------

customer_dim = customer_dim.select(
    "customer_key",
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "customer_segment",
    "customer_status",
    "effective_from",
    "effective_to",
    "is_current",
    "scd_created_at"
)

# COMMAND ----------

(
    customer_dim
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

gold_count = spark.table(TARGET_TABLE).count()

print(f"Gold customer records: {gold_count:,}")

assert gold_count == 11000

# COMMAND ----------

display(
    spark.table(TARGET_TABLE)
    .orderBy("customer_id", "effective_from")
    .limit(20)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_versions,
# MAGIC     COUNT(DISTINCT customer_id) AS customers,
# MAGIC     SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_customers
# MAGIC FROM workspace.finlake_gold.dim_customer;

# COMMAND ----------

