# Databricks notebook source
# ============================================================
# FINLAKE - SILVER CUSTOMERS
# ============================================================

BRONZE_TABLE = "workspace.finlake_bronze.customers"
SILVER_TABLE = "workspace.finlake_silver.customers"

bronze_customers = spark.table(BRONZE_TABLE)

print(f"Bronze records: {bronze_customers.count():,}")

display(bronze_customers.limit(10))

# COMMAND ----------

bronze_customers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### importing functions to standardize the data

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    initcap,
    lower,
    regexp_replace,
    to_date,
    to_timestamp,
    current_timestamp,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardize string coloumns

# COMMAND ----------

silver_customers = (
    bronze_customers

    # --------------------------------------------------------
    # IDs
    # --------------------------------------------------------

    .withColumn(
        "customer_id",
        trim(col("customer_id"))
    )

    # --------------------------------------------------------
    # Names
    # --------------------------------------------------------

    .withColumn(
        "first_name",
        initcap(trim(col("first_name")))
    )

    .withColumn(
        "last_name",
        initcap(trim(col("last_name")))
    )

    # --------------------------------------------------------
    # Email
    # --------------------------------------------------------

    .withColumn(
        "email",
        lower(trim(col("email")))
    )

    # --------------------------------------------------------
    # Phone
    # --------------------------------------------------------

    .withColumn(
        "phone",
        regexp_replace(
            trim(col("phone")),
            r"[^0-9+]",
            ""
        )
    )

    # --------------------------------------------------------
    # Categorical fields
    # --------------------------------------------------------

    .withColumn(
        "gender",
        upper(trim(col("gender")))
    )

    .withColumn(
        "city",
        initcap(trim(col("city")))
    )

    .withColumn(
        "occupation",
        upper(trim(col("occupation")))
    )

    .withColumn(
        "customer_segment",
        upper(trim(col("customer_segment")))
    )

    .withColumn(
        "customer_status",
        upper(trim(col("customer_status")))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert dates and timestamps

# COMMAND ----------

silver_customers = (
    silver_customers

    .withColumn(
        "date_of_birth",
        to_date(
            col("date_of_birth")
        )
    )

    .withColumn(
        "registration_date",
        to_date(
            col("registration_date")
        )
    )

    .withColumn(
        "created_at",
        to_timestamp(
            col("created_at")
        )
    )

    .withColumn(
        "_batch_date",
        to_date(
            col("_batch_date")
        )
    )
)

# COMMAND ----------

silver_customers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dropping Duplicate records

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

customer_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        col("_batch_date").desc(),
        col("_ingest_timestamp").desc()
    )
)

# COMMAND ----------

from pyspark.sql.functions import row_number

silver_customers_current = (
    silver_customers
    .withColumn(
        "_row_number",
        row_number().over(customer_window)
    )
    .filter(
        col("_row_number") == 1
    )
    .drop("_row_number")
)

# COMMAND ----------

print(
    f"Silver current-state records: "
    f"{silver_customers_current.count():,}"
)

# COMMAND ----------

silver_customers_final = (
    silver_customers_current
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "date_of_birth",
        "gender",
        "city",
        "occupation",
        "customer_segment",
        "customer_status",
        "registration_date",
        "created_at",
        "_batch_date",
        "_ingest_timestamp",
        "_source_file"
    )
)

# COMMAND ----------

display(
    silver_customers_final.limit(20)
)

# COMMAND ----------

silver_customers_final = (
    silver_customers_final
    .withColumn(
        "_silver_processed_at",
        current_timestamp()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the Silver Delta table

# COMMAND ----------

(
    silver_customers_final
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
# MAGIC ### Validate silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.finlake_silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_silver.customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM workspace.finlake_silver.customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.finlake_silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS null_customer_ids
# MAGIC FROM workspace.finlake_silver.customers
# MAGIC WHERE customer_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS null_emails
# MAGIC FROM workspace.finlake_silver.customers
# MAGIC WHERE email IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT customer_status
# MAGIC FROM workspace.finlake_silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT customer_segment
# MAGIC FROM workspace.finlake_silver.customers;

# COMMAND ----------

