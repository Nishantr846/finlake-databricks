# Databricks notebook source
# ============================================================
# FINLAKE - SILVER ACCOUNTS
# ============================================================

BRONZE_TABLE = "workspace.finlake_bronze.accounts"
SILVER_TABLE = "workspace.finlake_silver.accounts"

bronze_accounts = spark.table(BRONZE_TABLE)

print(
    f"Bronze account records: "
    f"{bronze_accounts.count():,}"
)

display(bronze_accounts.limit(10))

# COMMAND ----------

bronze_accounts.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Transformation functions

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    initcap,
    regexp_replace,
    to_date,
    to_timestamp,
    current_timestamp,
    row_number,
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Standardize the data

# COMMAND ----------

silver_accounts = (
    bronze_accounts

    # --------------------------------------------------------
    # IDs
    # --------------------------------------------------------

    .withColumn(
        "account_id",
        trim(col("account_id"))
    )

    .withColumn(
        "customer_id",
        trim(col("customer_id"))
    )

    # --------------------------------------------------------
    # Account number
    # --------------------------------------------------------

    .withColumn(
        "account_number",
        trim(col("account_number"))
    )

    # --------------------------------------------------------
    # Categorical fields
    # --------------------------------------------------------

    .withColumn(
        "account_type",
        upper(trim(col("account_type")))
    )

    .withColumn(
        "branch_code",
        upper(trim(col("branch_code")))
    )

    .withColumn(
        "account_status",
        upper(trim(col("account_status")))
    )

    .withColumn(
        "currency",
        upper(trim(col("currency")))
    )

    # --------------------------------------------------------
    # Numeric field
    # --------------------------------------------------------

    .withColumn(
        "balance",
        col("balance").cast("decimal(18,2)")
    )

    # --------------------------------------------------------
    # Dates
    # --------------------------------------------------------

    .withColumn(
        "opening_date",
        to_date(col("opening_date"))
    )

    .withColumn(
        "created_at",
        to_timestamp(col("created_at"))
    )

    .withColumn(
        "_batch_date",
        to_date(col("_batch_date"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### De duplication

# COMMAND ----------

account_window = (
    Window
    .partitionBy("account_id")
    .orderBy(
        col("_batch_date").desc(),
        col("_ingest_timestamp").desc()
    )
)

# COMMAND ----------

silver_accounts_current = (
    silver_accounts
    .withColumn(
        "_row_number",
        row_number().over(account_window)
    )
    .filter(
        col("_row_number") == 1
    )
    .drop("_row_number")
)

# COMMAND ----------

print(
    f"Silver current-state accounts: "
    f"{silver_accounts_current.count():,}"
)

# COMMAND ----------

silver_accounts_final = (
    silver_accounts_current
    .select(
        "account_id",
        "customer_id",
        "account_number",
        "account_type",
        "branch_code",
        "account_status",
        "opening_date",
        "balance",
        "currency",
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
    silver_accounts_final.limit(20)
)

# COMMAND ----------

(
    silver_accounts_final
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

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS record_count
# MAGIC FROM workspace.finlake_silver.accounts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     account_id,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM workspace.finlake_silver.accounts
# MAGIC GROUP BY account_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS orphan_accounts
# MAGIC FROM workspace.finlake_silver.accounts a
# MAGIC LEFT JOIN workspace.finlake_silver.customers c
# MAGIC     ON a.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### validate Account Types

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     account_type,
# MAGIC     COUNT(*) AS accounts
# MAGIC FROM workspace.finlake_silver.accounts
# MAGIC GROUP BY account_type
# MAGIC ORDER BY accounts DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Account Statuses

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     account_status,
# MAGIC     COUNT(*) AS accounts
# MAGIC FROM workspace.finlake_silver.accounts
# MAGIC GROUP BY account_status
# MAGIC ORDER BY accounts DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate numeric conversion

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE workspace.finlake_silver.accounts;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Batch Information

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _batch_date,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM workspace.finlake_silver.accounts
# MAGIC GROUP BY _batch_date
# MAGIC ORDER BY _batch_date;

# COMMAND ----------

