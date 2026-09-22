import pandas as pd
import numpy as np
from pathlib import Path

from config import (
    CUSTOMERS_PATH,
    ACCOUNTS_PATH,
    TRANSACTIONS_PATH,
    CARDS_PATH,
    LOANS_PATH,
    PAYMENTS_PATH,
    EVENTS_PATH
)

# ============================================================
# Configuration
# ============================================================

PREVIOUS_BATCH = "2026-08-03"
CURRENT_BATCH = "2026-08-04"

RANDOM_SEED = 42
rng = np.random.default_rng(RANDOM_SEED)


# ============================================================
# Helper
# ============================================================

def load_csv(path):
    return pd.read_csv(path)


def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"Saved: {path}")


def add_new_ids(existing_ids, prefix, count):
    """
    Generate IDs that do not already exist.
    """
    existing_ids = set(existing_ids)

    new_ids = []
    counter = 100000

    while len(new_ids) < count:
        candidate = f"{prefix}_{counter:06d}"

        if candidate not in existing_ids:
            new_ids.append(candidate)

        counter += 1

    return new_ids


# ============================================================
# 1. CUSTOMERS
# ============================================================

print("\n========== CUSTOMERS ==========")

previous_file = CUSTOMERS_PATH / f"customers_{PREVIOUS_BATCH}.csv"
current_file = CUSTOMERS_PATH / f"customers_{CURRENT_BATCH}.csv"

previous = load_csv(previous_file)
current = load_csv(current_file)

# Keep 95% existing customers
# Update approximately 5%
update_count = int(len(current) * 0.05)

update_indices = rng.choice(
    current.index,
    size=update_count,
    replace=False
)

# Change city for updated customers
cities = [
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Pune",
    "Chennai"
]

current.loc[update_indices, "city"] = rng.choice(
    cities,
    size=update_count
)

# Add new customers
new_customer_count = 500

new_customer_ids = add_new_ids(
    current["customer_id"].dropna(),
    "CUST",
    new_customer_count
)

new_customers = current.iloc[:new_customer_count].copy()

new_customers["customer_id"] = new_customer_ids

# Give new customers different names/emails
new_customers["first_name"] = [
    f"NewCustomer{i}"
    for i in range(new_customer_count)
]

new_customers["last_name"] = [
    "FinLake"
    for _ in range(new_customer_count)
]

new_customers["email"] = [
    f"newcustomer{i}@finlake.com"
    for i in range(new_customer_count)
]

new_customers["_batch_date"] = CURRENT_BATCH

current = pd.concat(
    [current, new_customers],
    ignore_index=True
)

save_csv(current, current_file)

print(f"Customers: {len(current)}")
print(f"Updated customers: {update_count}")
print(f"New customers: {new_customer_count}")


# ============================================================
# 2. ACCOUNTS
# ============================================================

print("\n========== ACCOUNTS ==========")

current_file = ACCOUNTS_PATH / f"accounts_{CURRENT_BATCH}.csv"

accounts = load_csv(current_file)

# Update approximately 5% of accounts
update_count = int(len(accounts) * 0.05)

update_indices = rng.choice(
    accounts.index,
    size=update_count,
    replace=False
)

accounts.loc[
    update_indices,
    "balance"
] = (
    accounts.loc[update_indices, "balance"].astype(float)
    + rng.uniform(100, 5000, update_count)
).round(2)

# Add new accounts
new_account_count = 500

new_account_ids = add_new_ids(
    accounts["account_id"].dropna(),
    "ACC",
    new_account_count
)

new_accounts = accounts.iloc[:new_account_count].copy()

new_accounts["account_id"] = new_account_ids

# Use existing customers as valid foreign keys
customer_ids = accounts["customer_id"].dropna().unique()

new_accounts["customer_id"] = rng.choice(
    customer_ids,
    size=new_account_count
)

new_accounts["balance"] = rng.uniform(
    1000,
    100000,
    new_account_count
).round(2)

new_accounts["_batch_date"] = CURRENT_BATCH

accounts = pd.concat(
    [accounts, new_accounts],
    ignore_index=True
)

save_csv(accounts, current_file)

print(f"Accounts: {len(accounts)}")
print(f"Updated accounts: {update_count}")
print(f"New accounts: {new_account_count}")


# ============================================================
# 3. CARDS
# ============================================================

print("\n========== CARDS ==========")

current_file = CARDS_PATH / f"cards_{CURRENT_BATCH}.csv"

cards = load_csv(current_file)

update_count = int(len(cards) * 0.05)

update_indices = rng.choice(
    cards.index,
    size=update_count,
    replace=False
)

# Update card status
cards.loc[
    update_indices,
    "card_status"
] = "ACTIVE"

# Add new cards
new_card_count = 300

new_card_ids = add_new_ids(
    cards["card_id"].dropna(),
    "CARD",
    new_card_count
)

new_cards = cards.iloc[:new_card_count].copy()

new_cards["card_id"] = new_card_ids

account_ids = accounts["account_id"].dropna().unique()

new_cards["account_id"] = rng.choice(
    account_ids,
    size=new_card_count
)

new_cards["_batch_date"] = CURRENT_BATCH

cards = pd.concat(
    [cards, new_cards],
    ignore_index=True
)

save_csv(cards, current_file)

print(f"Cards: {len(cards)}")
print(f"Updated cards: {update_count}")
print(f"New cards: {new_card_count}")


# ============================================================
# 4. LOANS
# ============================================================

print("\n========== LOANS ==========")

current_file = LOANS_PATH / f"loans_{CURRENT_BATCH}.csv"

loans = load_csv(current_file)

update_count = int(len(loans) * 0.05)

update_indices = rng.choice(
    loans.index,
    size=update_count,
    replace=False
)

# Reduce outstanding balance for updated loans
principal = loans.loc[
    update_indices,
    "principal_amount"
].astype(float)

new_outstanding = (
    principal * rng.uniform(
        0.30,
        0.80,
        update_count
    )
).round(2)

loans.loc[
    update_indices,
    "outstanding_amount"
] = new_outstanding

# Add new loans
new_loan_count = 250

new_loan_ids = add_new_ids(
    loans["loan_id"].dropna(),
    "LOAN",
    new_loan_count
)

new_loans = loans.iloc[:new_loan_count].copy()

new_loans["loan_id"] = new_loan_ids

customer_ids = customers_ids = current["customer_id"].dropna().unique()

new_loans["customer_id"] = rng.choice(
    customer_ids,
    size=new_loan_count
)

new_loans["principal_amount"] = rng.uniform(
    50000,
    500000,
    new_loan_count
).round(2)

new_loans["outstanding_amount"] = (
    new_loans["principal_amount"] * 0.9
).round(2)

new_loans["_batch_date"] = CURRENT_BATCH

loans = pd.concat(
    [loans, new_loans],
    ignore_index=True
)

save_csv(loans, current_file)

print(f"Loans: {len(loans)}")
print(f"Updated loans: {update_count}")
print(f"New loans: {new_loan_count}")


# ============================================================
# 5. TRANSACTIONS
# ============================================================

print("\n========== TRANSACTIONS ==========")

current_file = TRANSACTIONS_PATH / f"transactions_{CURRENT_BATCH}.csv"

transactions = load_csv(current_file)

# Transactions are primarily new events.
# We don't update historical transactions.

new_transaction_count = 10000

new_transaction_ids = add_new_ids(
    transactions["transaction_id"].dropna(),
    "TXN",
    new_transaction_count
)

new_transactions = transactions.iloc[
    :new_transaction_count
].copy()

new_transactions["transaction_id"] = new_transaction_ids

account_ids = accounts["account_id"].dropna().unique()

new_transactions["account_id"] = rng.choice(
    account_ids,
    size=new_transaction_count
)

# Make customer IDs consistent with selected accounts
account_customer_map = (
    accounts[
        ["account_id", "customer_id"]
    ]
    .drop_duplicates("account_id")
    .set_index("account_id")["customer_id"]
    .to_dict()
)

new_transactions["customer_id"] = (
    new_transactions["account_id"]
    .map(account_customer_map)
)

new_transactions["amount"] = rng.uniform(
    50,
    50000,
    new_transaction_count
).round(2)

new_transactions["_batch_date"] = CURRENT_BATCH

transactions = pd.concat(
    [transactions, new_transactions],
    ignore_index=True
)

save_csv(transactions, current_file)

print(f"Transactions: {len(transactions)}")
print(f"New transactions: {new_transaction_count}")


# ============================================================
# 6. PAYMENTS
# ============================================================

print("\n========== PAYMENTS ==========")

current_file = PAYMENTS_PATH / f"payments_{CURRENT_BATCH}.csv"

payments = load_csv(current_file)

new_payment_count = 5000

new_payment_ids = add_new_ids(
    payments["payment_id"].dropna(),
    "PAY",
    new_payment_count
)

new_payments = payments.iloc[
    :new_payment_count
].copy()

new_payments["payment_id"] = new_payment_ids

loan_ids = loans["loan_id"].dropna().unique()

new_payments["loan_id"] = rng.choice(
    loan_ids,
    size=new_payment_count
)

loan_customer_map = (
    loans[
        ["loan_id", "customer_id"]
    ]
    .drop_duplicates("loan_id")
    .set_index("loan_id")["customer_id"]
    .to_dict()
)

new_payments["customer_id"] = (
    new_payments["loan_id"]
    .map(loan_customer_map)
)

new_payments["payment_amount"] = rng.uniform(
    500,
    20000,
    new_payment_count
).round(2)

new_payments["_batch_date"] = CURRENT_BATCH

payments = pd.concat(
    [payments, new_payments],
    ignore_index=True
)

save_csv(payments, current_file)

print(f"Payments: {len(payments)}")
print(f"New payments: {new_payment_count}")


# ============================================================
# 7. EVENTS
# ============================================================

print("\n========== EVENTS ==========")

current_file = EVENTS_PATH / f"events_{CURRENT_BATCH}.csv"

events = load_csv(current_file)

new_event_count = 10000

new_event_ids = add_new_ids(
    events["event_id"].dropna(),
    "EVENT",
    new_event_count
)

new_events = events.iloc[
    :new_event_count
].copy()

new_events["event_id"] = new_event_ids

customer_ids = current["customer_id"].dropna().unique()

new_events["customer_id"] = rng.choice(
    customer_ids,
    size=new_event_count
)

new_events["_batch_date"] = CURRENT_BATCH

events = pd.concat(
    [events, new_events],
    ignore_index=True
)

save_csv(events, current_file)

print(f"Events: {len(events)}")
print(f"New events: {new_event_count}")


# ============================================================
# COMPLETE
# ============================================================

print("\n========================================")
print("Incremental batch preparation complete")
print(f"Previous batch: {PREVIOUS_BATCH}")
print(f"Current batch:  {CURRENT_BATCH}")
print("========================================")