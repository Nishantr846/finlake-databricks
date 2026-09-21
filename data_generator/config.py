import sys
from pathlib import Path


# ============================================================
# BATCH DATE
# ============================================================

if len(sys.argv) > 1:
    BATCH_DATE = sys.argv[1]
else:
    BATCH_DATE = "2026-08-01"


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data"


CUSTOMERS_PATH = DATA_ROOT / "customers"
ACCOUNTS_PATH = DATA_ROOT / "accounts"
TRANSACTIONS_PATH = DATA_ROOT / "transactions"
CARDS_PATH = DATA_ROOT / "cards"
LOANS_PATH = DATA_ROOT / "loans"
PAYMENTS_PATH = DATA_ROOT / "payments"
EVENTS_PATH = DATA_ROOT / "events"


# ============================================================
# RECORD COUNTS
# ============================================================

NUM_CUSTOMERS = 10_000
NUM_ACCOUNTS = 15_000
NUM_CARDS = 8_000
NUM_LOANS = 5_000
NUM_PAYMENTS = 20_000
NUM_TRANSACTIONS = 100_000
NUM_EVENTS = 50_000


# ============================================================
# RANDOM SEED
# ============================================================

RANDOM_SEED = 42


# ============================================================
# BATCH-SPECIFIC FILES
# ============================================================

CUSTOMER_FILE = (
    CUSTOMERS_PATH / f"customers_{BATCH_DATE}.csv"
)

ACCOUNT_FILE = (
    ACCOUNTS_PATH / f"accounts_{BATCH_DATE}.csv"
)

TRANSACTION_FILE = (
    TRANSACTIONS_PATH / f"transactions_{BATCH_DATE}.csv"
)

CARD_FILE = (
    CARDS_PATH / f"cards_{BATCH_DATE}.csv"
)

LOAN_FILE = (
    LOANS_PATH / f"loans_{BATCH_DATE}.csv"
)

PAYMENT_FILE = (
    PAYMENTS_PATH / f"payments_{BATCH_DATE}.csv"
)

EVENT_FILE = (
    EVENTS_PATH / f"events_{BATCH_DATE}.csv"
)


# ============================================================
# CREATE DIRECTORIES
# ============================================================

for path in [
    CUSTOMERS_PATH,
    ACCOUNTS_PATH,
    TRANSACTIONS_PATH,
    CARDS_PATH,
    LOANS_PATH,
    PAYMENTS_PATH,
    EVENTS_PATH,
]:
    path.mkdir(parents=True, exist_ok=True)