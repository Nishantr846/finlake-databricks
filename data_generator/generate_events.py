import random

import numpy as np
import pandas as pd
from faker import Faker

from config import (
    CUSTOMER_FILE,
    EVENT_FILE,
    NUM_EVENTS,
    RANDOM_SEED,
)


# ============================================================
# RANDOM SETUP
# ============================================================

fake = Faker("en_IN")

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)


# ============================================================
# CONSTANTS
# ============================================================

EVENT_TYPES = [
    "LOGIN",
    "LOGOUT",
    "TRANSFER",
    "PAYMENT",
    "CARD_ACTIVATION",
    "LOAN_APPLICATION",
    "PROFILE_UPDATE",
    "PASSWORD_CHANGE",
    "CARD_BLOCK",
    "BENEFICIARY_ADDED",
]

DEVICES = [
    "ANDROID",
    "IOS",
    "WEB",
    "ATM",
]

CHANNELS = [
    "MOBILE_APP",
    "WEB",
    "ATM",
    "BRANCH",
]


# ============================================================
# EVENT METADATA
# ============================================================

def generate_event_metadata(event_type):

    if event_type == "LOGIN":

        return {
            "ip": fake.ipv4(),
            "browser": random.choice([
                "Chrome",
                "Safari",
                "Firefox",
            ]),
        }

    if event_type == "TRANSFER":

        return {
            "transfer_type": random.choice([
                "NEFT",
                "IMPS",
                "RTGS",
                "UPI",
            ]),
            "amount": round(
                np.random.uniform(
                    500,
                    250_000
                ),
                2
            ),
        }

    if event_type == "PAYMENT":

        return {
            "payment_type": random.choice([
                "BILL_PAYMENT",
                "MERCHANT_PAYMENT",
                "LOAN_PAYMENT",
            ]),
            "amount": round(
                np.random.uniform(
                    100,
                    100_000
                ),
                2
            ),
        }

    if event_type == "LOAN_APPLICATION":

        return {
            "loan_type": random.choice([
                "HOME",
                "PERSONAL",
                "AUTO",
                "EDUCATION",
                "BUSINESS",
            ]),
            "requested_amount": round(
                np.random.uniform(
                    50_000,
                    5_000_000
                ),
                2
            ),
        }

    if event_type == "CARD_ACTIVATION":

        return {
            "card_type": random.choice([
                "DEBIT",
                "CREDIT",
            ])
        }

    if event_type == "PROFILE_UPDATE":

        return {
            "field_updated": random.choice([
                "email",
                "phone",
                "address",
                "city",
                "communication_preference",
            ])
        }

    if event_type == "PASSWORD_CHANGE":

        return {
            "method": random.choice([
                "OTP",
                "SECURITY_QUESTION",
                "BIOMETRIC",
            ])
        }

    if event_type == "CARD_BLOCK":

        return {
            "reason": random.choice([
                "CUSTOMER_REQUEST",
                "SUSPICIOUS_ACTIVITY",
                "LOST_CARD",
                "STOLEN_CARD",
            ])
        }

    if event_type == "BENEFICIARY_ADDED":

        return {
            "beneficiary_type": random.choice([
                "INDIVIDUAL",
                "BUSINESS",
            ])
        }

    return {}


# ============================================================
# GENERATOR
# ============================================================

def generate_events():

    customers = pd.read_csv(
        CUSTOMER_FILE
    )

    customer_ids = customers[
        "customer_id"
    ].tolist()

    records = []

    for i in range(1, NUM_EVENTS + 1):

        customer_id = random.choice(
            customer_ids
        )

        event_type = random.choice(
            EVENT_TYPES
        )

        records.append({
            "event_id": f"E{i:09d}",

            "customer_id": customer_id,

            "event_type": event_type,

            "event_timestamp": fake.date_time_between(
                start_date="-30d",
                end_date="now"
            ),

            "device": random.choice(
                DEVICES
            ),

            "channel": random.choice(
                CHANNELS
            ),

            # Stored as a string intentionally.
            # We will parse this later in Silver.
            "metadata": str(
                generate_event_metadata(
                    event_type
                )
            ),
        })

    df = pd.DataFrame(records)

    df.to_csv(
        EVENT_FILE,
        index=False
    )

    print(f"Generated {len(df):,} events")
    print(f"Saved to: {EVENT_FILE}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    generate_events()