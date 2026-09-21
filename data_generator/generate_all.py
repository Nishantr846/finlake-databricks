from generate_customers import generate_customers
from generate_accounts import generate_accounts
from generate_transactions import generate_transactions
from generate_cards import generate_cards
from generate_loans import generate_loans
from generate_payments import generate_payments
from generate_events import generate_events


def main():

    print("=" * 60)
    print("Generating FinLake source data")
    print("=" * 60)

    print("\n[1/7] Generating customers...")
    generate_customers()

    print("\n[2/7] Generating accounts...")
    generate_accounts()

    print("\n[3/7] Generating transactions...")
    generate_transactions()

    print("\n[4/7] Generating cards...")
    generate_cards()

    print("\n[5/7] Generating loans...")
    generate_loans()

    print("\n[6/7] Generating payments...")
    generate_payments()

    print("\n[7/7] Generating events...")
    generate_events()

    print("\n" + "=" * 60)
    print("FinLake source data generation completed.")
    print("=" * 60)


if __name__ == "__main__":
    main()