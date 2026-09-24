-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Phase 5 — Data Quality Metadata
-- MAGIC
-- MAGIC This notebook creates the metadata tables used by the FinLake
-- MAGIC data quality framework.
-- MAGIC
-- MAGIC The framework is metadata-driven so that validation rules can be
-- MAGIC added or modified without rewriting the core DQ engine.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.finlake_monitoring.dq_rules (
    rule_id STRING,
    table_name STRING,
    column_name STRING,
    rule_type STRING,
    rule_description STRING,
    severity STRING,
    active BOOLEAN
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.finlake_monitoring.dq_results (
    run_id STRING,
    table_name STRING,
    total_records BIGINT,
    valid_records BIGINT,
    invalid_records BIGINT,
    dq_pass_rate DECIMAL(7,4),
    rules_executed INT,
    execution_timestamp TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.finlake_monitoring.quarantine_records (
    run_id STRING,
    table_name STRING,
    record_id STRING,
    rule_id STRING,
    failed_column STRING,
    failure_reason STRING,
    quarantine_timestamp TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

INSERT INTO workspace.finlake_monitoring.dq_rules VALUES

-- Customers
('CUST_001', 'customers', 'customer_id',
 'NOT_NULL', 'Customer ID must not be null', 'ERROR', true),

('CUST_002', 'customers', 'email',
 'EMAIL_FORMAT', 'Customer email must have a valid format', 'ERROR', true),

('CUST_003', 'customers', 'customer_status',
 'ALLOWED_VALUES', 'Customer status must be ACTIVE, INACTIVE or BLOCKED', 'ERROR', true),

-- Accounts
('ACC_001', 'accounts', 'account_id',
 'NOT_NULL', 'Account ID must not be null', 'ERROR', true),

('ACC_002', 'accounts', 'customer_id',
 'REFERENTIAL', 'Account customer must exist in customers', 'ERROR', true),

('ACC_003', 'accounts', 'balance',
 'NON_NEGATIVE', 'Account balance must not be negative', 'ERROR', true),

-- Transactions
('TXN_001', 'transactions', 'transaction_id',
 'NOT_NULL', 'Transaction ID must not be null', 'ERROR', true),

('TXN_002', 'transactions', 'amount',
 'POSITIVE', 'Transaction amount must be greater than zero', 'ERROR', true),

('TXN_003', 'transactions', 'account_id',
 'REFERENTIAL', 'Transaction account must exist', 'ERROR', true),

('TXN_004', 'transactions', 'customer_id',
 'REFERENTIAL', 'Transaction customer must exist', 'ERROR', true),

-- Cards
('CARD_001', 'cards', 'card_id',
 'NOT_NULL', 'Card ID must not be null', 'ERROR', true),

('CARD_002', 'cards', 'account_id',
 'REFERENTIAL', 'Card account must exist', 'ERROR', true),

('CARD_003', 'cards', 'expiry_date',
 'DATE_AFTER', 'Card expiry date must be after issue date', 'ERROR', true),

-- Loans
('LOAN_001', 'loans', 'loan_id',
 'NOT_NULL', 'Loan ID must not be null', 'ERROR', true),

('LOAN_002', 'loans', 'principal_amount',
 'POSITIVE', 'Loan principal must be greater than zero', 'ERROR', true),

('LOAN_003', 'loans', 'outstanding_amount',
 'RANGE', 'Outstanding amount must be between zero and principal', 'ERROR', true),

('LOAN_004', 'loans', 'customer_id',
 'REFERENTIAL', 'Loan customer must exist', 'ERROR', true),

-- Payments
('PAY_001', 'payments', 'payment_id',
 'NOT_NULL', 'Payment ID must not be null', 'ERROR', true),

('PAY_002', 'payments', 'payment_amount',
 'POSITIVE', 'Payment amount must be greater than zero', 'ERROR', true),

('PAY_003', 'payments', 'loan_id',
 'REFERENTIAL', 'Payment loan must exist', 'ERROR', true),

-- Events
('EVENT_001', 'events', 'event_id',
 'NOT_NULL', 'Event ID must not be null', 'ERROR', true),

('EVENT_002', 'events', 'customer_id',
 'REFERENTIAL', 'Event customer must exist', 'ERROR', true);

-- COMMAND ----------

SELECT
    table_name,
    COUNT(*) AS rule_count
FROM workspace.finlake_monitoring.dq_rules
GROUP BY table_name
ORDER BY table_name;

-- COMMAND ----------

