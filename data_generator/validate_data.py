import pandas as pd

from config import (
    CUSTOMER_FILE,
    ACCOUNT_FILE,
    TRANSACTION_FILE,
    CARD_FILE,
    LOAN_FILE,
    PAYMENT_FILE,
    EVENT_FILE,
)


# ============================================================
# LOAD DATA
# ============================================================

print("Loading generated data...\n")

customers = pd.read_csv(
    CUSTOMER_FILE
)

accounts = pd.read_csv(
    ACCOUNT_FILE
)

transactions = pd.read_csv(
    TRANSACTION_FILE
)

cards = pd.read_csv(
    CARD_FILE
)

loans = pd.read_csv(
    LOAN_FILE
)

payments = pd.read_csv(
    PAYMENT_FILE
)

events = pd.read_csv(
    EVENT_FILE
)


# ============================================================
# RECORD COUNTS
# ============================================================

print("=" * 60)
print("RECORD COUNTS")
print("=" * 60)

print(f"Customers     : {len(customers):,}")
print(f"Accounts      : {len(accounts):,}")
print(f"Transactions  : {len(transactions):,}")
print(f"Cards         : {len(cards):,}")
print(f"Loans         : {len(loans):,}")
print(f"Payments      : {len(payments):,}")
print(f"Events        : {len(events):,}")


# ============================================================
# PRIMARY KEY CHECKS
# ============================================================

print("\n" + "=" * 60)
print("PRIMARY KEY CHECKS")
print("=" * 60)

checks = {
    "Customer IDs": customers["customer_id"],
    "Account IDs": accounts["account_id"],
    "Transaction IDs": transactions["transaction_id"],
    "Card IDs": cards["card_id"],
    "Loan IDs": loans["loan_id"],
    "Payment IDs": payments["payment_id"],
    "Event IDs": events["event_id"],
}

for name, series in checks.items():

    duplicates = series.duplicated().sum()

    print(
        f"{name:<20}: "
        f"{'PASS' if duplicates == 0 else 'FAIL'} "
        f"(duplicates={duplicates})"
    )


# ============================================================
# REFERENTIAL INTEGRITY
# ============================================================

print("\n" + "=" * 60)
print("REFERENTIAL INTEGRITY")
print("=" * 60)


# Account → Customer

invalid_accounts = (
    ~accounts["customer_id"].isin(
        customers["customer_id"]
    )
).sum()

print(
    f"Account → Customer       : "
    f"{'PASS' if invalid_accounts == 0 else 'FAIL'} "
    f"(invalid={invalid_accounts})"
)


# Transaction → Account

invalid_transaction_accounts = (
    ~transactions["account_id"].isin(
        accounts["account_id"]
    )
).sum()

print(
    f"Transaction → Account    : "
    f"{'PASS' if invalid_transaction_accounts == 0 else 'FAIL'} "
    f"(invalid={invalid_transaction_accounts})"
)


# Transaction → Customer

invalid_transaction_customers = (
    ~transactions["customer_id"].isin(
        customers["customer_id"]
    )
).sum()

print(
    f"Transaction → Customer   : "
    f"{'PASS' if invalid_transaction_customers == 0 else 'FAIL'} "
    f"(invalid={invalid_transaction_customers})"
)


# Card → Account

invalid_cards = (
    ~cards["account_id"].isin(
        accounts["account_id"]
    )
).sum()

print(
    f"Card → Account           : "
    f"{'PASS' if invalid_cards == 0 else 'FAIL'} "
    f"(invalid={invalid_cards})"
)


# Loan → Customer

invalid_loans = (
    ~loans["customer_id"].isin(
        customers["customer_id"]
    )
).sum()

print(
    f"Loan → Customer          : "
    f"{'PASS' if invalid_loans == 0 else 'FAIL'} "
    f"(invalid={invalid_loans})"
)


# Payment → Loan

invalid_payment_loans = (
    ~payments["loan_id"].isin(
        loans["loan_id"]
    )
).sum()

print(
    f"Payment → Loan           : "
    f"{'PASS' if invalid_payment_loans == 0 else 'FAIL'} "
    f"(invalid={invalid_payment_loans})"
)


# Payment → Customer

invalid_payment_customers = (
    ~payments["customer_id"].isin(
        customers["customer_id"]
    )
).sum()

print(
    f"Payment → Customer       : "
    f"{'PASS' if invalid_payment_customers == 0 else 'FAIL'} "
    f"(invalid={invalid_payment_customers})"
)


# Event → Customer

invalid_events = (
    ~events["customer_id"].isin(
        customers["customer_id"]
    )
).sum()

print(
    f"Event → Customer         : "
    f"{'PASS' if invalid_events == 0 else 'FAIL'} "
    f"(invalid={invalid_events})"
)


# ============================================================
# FINAL RESULT
# ============================================================

all_checks = [
    invalid_accounts,
    invalid_transaction_accounts,
    invalid_transaction_customers,
    invalid_cards,
    invalid_loans,
    invalid_payment_loans,
    invalid_payment_customers,
    invalid_events,
]

print("\n" + "=" * 60)

if all(value == 0 for value in all_checks):
    print("VALIDATION RESULT: PASS")
    print("All referential integrity checks passed.")
else:
    print("VALIDATION RESULT: FAIL")
    print("One or more referential integrity checks failed.")

print("=" * 60)