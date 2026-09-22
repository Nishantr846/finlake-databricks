import pandas as pd
from pathlib import Path
import random

from config import (
    CUSTOMERS_PATH,
    ACCOUNTS_PATH,
    TRANSACTIONS_PATH,
    CARDS_PATH,
    LOANS_PATH,
    PAYMENTS_PATH,
    EVENTS_PATH
)

BATCH_DATE = "2026-08-04"


customers_file = CUSTOMERS_PATH / f"customers_{BATCH_DATE}.csv"

customers = pd.read_csv(customers_file)

# Invalid email
customers.loc[0, "email"] = "invalid-email"

# Null customer ID
customers.loc[1, "customer_id"] = None

customers.to_csv(customers_file, index=False)

accounts_file = ACCOUNTS_PATH / f"accounts_{BATCH_DATE}.csv"

accounts = pd.read_csv(accounts_file)

# Unknown customer
accounts.loc[0, "customer_id"] = "CUST_UNKNOWN_999"

accounts.to_csv(accounts_file, index=False)

transactions_file = TRANSACTIONS_PATH / f"transactions_{BATCH_DATE}.csv"

transactions = pd.read_csv(transactions_file)

# Negative transaction amount
transactions.loc[0, "amount"] = -500

# Unknown account
transactions.loc[1, "account_id"] = "ACC_UNKNOWN_999"

# Unknown customer
transactions.loc[2, "customer_id"] = "CUST_UNKNOWN_999"

transactions.to_csv(transactions_file, index=False)

cards_file = CARDS_PATH / f"cards_{BATCH_DATE}.csv"

cards = pd.read_csv(cards_file)

cards.loc[0, "expiry_date"] = "2020-01-01"

cards.to_csv(cards_file, index=False)

loans_file = LOANS_PATH / f"loans_{BATCH_DATE}.csv"

loans = pd.read_csv(loans_file)

loans.loc[0, "outstanding_amount"] = (loans.loc[0, "principal_amount"] + 10000)

loans.to_csv(loans_file, index=False)

payments_file = PAYMENTS_PATH / f"payments_{BATCH_DATE}.csv"

payments = pd.read_csv(payments_file)

payments.loc[0, "loan_id"] = "LOAN_UNKNOWN_999"

payments.to_csv(payments_file, index=False)

events_file = EVENTS_PATH / f"events_{BATCH_DATE}.csv"

events = pd.read_csv(events_file)

events.loc[0, "customer_id"] = "CUST_UNKNOWN_999"

events.to_csv(events_file, index=False)