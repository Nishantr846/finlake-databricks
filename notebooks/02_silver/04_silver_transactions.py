# Databricks notebook source
# ============================================================
# FINLAKE - SILVER TRANSACTIONS
# ============================================================

BRONZE_TABLE = "workspace.finlake_bronze.transactions"
SILVER_TABLE = "workspace.finlake_silver.transactions"

bronze_transactions = spark.table(BRONZE_TABLE)

print(
    f"Bronze transaction records: "
    f"{bronze_transactions.count():,}"
)

display(
    bronze_transactions.limit(10)
)

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    initcap,
    to_date,
    to_timestamp,
    current_timestamp,
    row_number,
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Standardize Data

# COMMAND ----------

silver_transactions = (
    bronze_transactions

    # --------------------------------------------------------
    # IDs
    # --------------------------------------------------------

    .withColumn(
        "transaction_id",
        trim(col("transaction_id"))
    )

    .withColumn(
        "account_id",
        trim(col("account_id"))
    )

    .withColumn(
        "customer_id",
        trim(col("customer_id"))
    )

    # --------------------------------------------------------
    # Transaction type
    # --------------------------------------------------------

    .withColumn(
        "transaction_type",
        upper(trim(col("transaction_type")))
    )

    .withColumn(
        "transaction_category",
        upper(trim(col("transaction_category")))
    )

    .withColumn(
        "transaction_status",
        upper(trim(col("transaction_status")))
    )

    # --------------------------------------------------------
    # Merchant
    # --------------------------------------------------------

    .withColumn(
        "merchant",
        initcap(trim(col("merchant")))
    )

    # --------------------------------------------------------
    # Monetary amount
    # --------------------------------------------------------

    .withColumn(
        "amount",
        col("amount").cast("decimal(18,2)")
    )

    # --------------------------------------------------------
    # Dates
    # --------------------------------------------------------

    .withColumn(
        "transaction_date",
        to_date(col("transaction_date"))
    )

    .withColumn(
        "transaction_timestamp",
        to_timestamp(col("transaction_timestamp"))
    )

    .withColumn(
        "_batch_date",
        to_date(col("_batch_date"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DeDuplicate Transactions

# COMMAND ----------

transaction_window = (
    Window
    .partitionBy("transaction_id")
    .orderBy(
        col("_batch_date").desc(),
        col("_ingest_timestamp").desc()
    )
)

# COMMAND ----------

silver_transactions_current = (
    silver_transactions
    .withColumn(
        "_row_number",
        row_number().over(transaction_window)
    )
    .filter(
        col("_row_number") == 1
    )
    .drop("_row_number")
)

# COMMAND ----------

print(
    f"Silver current-state transactions: "
    f"{silver_transactions_current.count():,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Final Schema

# COMMAND ----------

silver_transactions_final = (
    silver_transactions_current
    .select(
        "transaction_id",
        "account_id",
        "customer_id",
        "transaction_type",
        "transaction_category",
        "amount",
        "transaction_date",
        "transaction_timestamp",
        "transaction_status",
        "merchant",
        "reference_number",
        "_batch_date",
        "_ingest_timestamp",
        "_source_file"
    )
    .withColumn(
        "_silver_processed_at",
        current_timestamp()
    )
)

# COMMAND ----------

display(
    silver_transactions_final.limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Silver Transactions

# COMMAND ----------

(
    silver_transactions_final
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        SILVER_TABLE
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Transaction Categories

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_category,
# MAGIC     COUNT(*) AS transactions
# MAGIC FROM workspace.finlake_silver.transactions
# MAGIC GROUP BY transaction_category
# MAGIC ORDER BY transactions DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     transaction_date,
# MAGIC     transaction_timestamp
# MAGIC FROM workspace.finlake_silver.transactions
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.finlake_silver.transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _batch_date,
# MAGIC     COUNT(*) AS records
# MAGIC FROM workspace.finlake_silver.transactions
# MAGIC GROUP BY _batch_date
# MAGIC ORDER BY _batch_date;

# COMMAND ----------

