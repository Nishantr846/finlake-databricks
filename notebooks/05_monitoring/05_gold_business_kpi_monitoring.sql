-- Databricks notebook source
-- MAGIC %md
-- MAGIC # FinLake — Phase 9.5: Gold Business KPI Monitoring
-- MAGIC
-- MAGIC Creates dashboard-ready KPI views from the Gold layer. This notebook only creates monitoring views; it does not modify Gold tables.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC GOLD = "workspace.finlake_gold"
-- MAGIC MON = "workspace.finlake_monitoring"
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Customer KPIs

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_customer_kpis AS
SELECT
    COUNT(*) AS total_customers,
    SUM(CASE WHEN customer_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_customers,
    SUM(CASE WHEN customer_status <> 'ACTIVE' THEN 1 ELSE 0 END) AS inactive_customers
FROM workspace.finlake_gold.dim_customer;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Account KPIs

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_account_kpis AS
SELECT
    COUNT(*) AS total_accounts,
    ROUND(SUM(CAST(balance AS DECIMAL(18,2))), 2) AS total_balance,
    ROUND(AVG(CAST(balance AS DECIMAL(18,2))), 2) AS average_balance,
    COUNT(DISTINCT customer_id) AS customers_with_accounts
FROM workspace.finlake_gold.dim_account;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Transaction KPIs

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_transaction_kpis AS
SELECT
    COUNT(*) AS total_transactions,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_transaction_value,
    ROUND(AVG(CAST(amount AS DECIMAL(18,2))), 2) AS average_transaction_value,
    COUNT(DISTINCT customer_id) AS transacting_customers,
    COUNT(DISTINCT account_id) AS transacting_accounts
FROM workspace.finlake_gold.fact_transaction;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Monthly transaction trend

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_monthly_transaction_trend AS
SELECT
    DATE_TRUNC('month', transaction_date) AS transaction_month,
    COUNT(*) AS transaction_count,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS transaction_value,
    ROUND(AVG(CAST(amount AS DECIMAL(18,2))), 2) AS average_transaction_value
FROM workspace.finlake_gold.fact_transaction
GROUP BY DATE_TRUNC('month', transaction_date);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Payment KPIs

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_payment_kpis AS
SELECT
    COUNT(*) AS total_payments,
    ROUND(SUM(CAST(payment_amount AS DECIMAL(18,2))), 2) AS total_payment_value,
    ROUND(AVG(CAST(payment_amount AS DECIMAL(18,2))), 2) AS average_payment_value,
    COUNT(DISTINCT customer_id) AS paying_customers,
    COUNT(DISTINCT loan_id) AS loans_with_payments
FROM workspace.finlake_gold.fact_payment;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Loan portfolio KPIs

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_loan_kpis AS
SELECT
    COUNT(*) AS total_loans,
    ROUND(SUM(CAST(principal_amount AS DECIMAL(18,2))), 2) AS total_principal,
    ROUND(SUM(CAST(outstanding_amount AS DECIMAL(18,2))), 2) AS total_outstanding,
    ROUND(AVG(CAST(interest_rate AS DECIMAL(10,4))), 2) AS average_interest_rate,
    ROUND(
        100.0 * SUM(CAST(outstanding_amount AS DECIMAL(18,2))) /
        NULLIF(SUM(CAST(principal_amount AS DECIMAL(18,2))), 0), 2
    ) AS outstanding_ratio_pct
FROM workspace.finlake_gold.dim_loan;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Fraud / suspicious activity KPIs

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_fraud_kpis AS
SELECT
    COUNT(*) AS total_fraud_records,
    COUNT(DISTINCT customer_id) AS affected_customers,
    COUNT(DISTINCT transaction_id) AS affected_transactions,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS suspicious_transaction_value
FROM workspace.finlake_gold.fraud_analytics;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Branch / account KPI table

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_branch_kpis AS
SELECT *
FROM workspace.finlake_gold.account_kpis;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Executive KPI summary

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_executive_kpis AS
SELECT
    c.total_customers,
    c.active_customers,
    a.total_accounts,
    a.total_balance,
    t.total_transactions,
    t.total_transaction_value,
    p.total_payments,
    p.total_payment_value,
    l.total_loans,
    l.total_principal,
    l.total_outstanding,
    f.total_fraud_records,
    f.suspicious_transaction_value
FROM workspace.finlake_monitoring.v_customer_kpis c
CROSS JOIN workspace.finlake_monitoring.v_account_kpis a
CROSS JOIN workspace.finlake_monitoring.v_transaction_kpis t
CROSS JOIN workspace.finlake_monitoring.v_payment_kpis p
CROSS JOIN workspace.finlake_monitoring.v_loan_kpis l
CROSS JOIN workspace.finlake_monitoring.v_fraud_kpis f;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Validation

-- COMMAND ----------

SELECT * FROM workspace.finlake_monitoring.v_executive_kpis;
SELECT * FROM workspace.finlake_monitoring.v_customer_kpis;
SELECT * FROM workspace.finlake_monitoring.v_account_kpis;
SELECT * FROM workspace.finlake_monitoring.v_transaction_kpis;
SELECT * FROM workspace.finlake_monitoring.v_payment_kpis;
SELECT * FROM workspace.finlake_monitoring.v_loan_kpis;
SELECT * FROM workspace.finlake_monitoring.v_fraud_kpis;
SELECT * FROM workspace.finlake_monitoring.v_monthly_transaction_trend
ORDER BY transaction_month;
