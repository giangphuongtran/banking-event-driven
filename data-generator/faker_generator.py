import time
import psycopg2
from decimal import Decimal, ROUND_DOWN
from faker import Faker
import random
import argparse
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# PROJECT CONFIGURATION
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TXN_AMOUNT = 1000.00
CURRENCY = 'USD'

# Initial balance range for accounts
MIN_INITIAL_BALANCE = 100.00
MAX_INITIAL_BALANCE = 1000.00

# Loop configuration
DEFAULT_LOOP = True
NUM_ITERATIONS = 10
SLEEP_SECONDS = 2

# CLI override
parser = argparse.ArgumentParser(description='Generate synthetic banking data.')
parser.add_argument('--once', action='store_true', help='Run the data generation once and exit.')
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

# Helpers
fake = Faker()

def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    """Generate a random monetary value between min_val and max_val."""
    val = Decimal(str(random.uniform(float(min_val), float(max_val)))).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
    return val

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
)
conn.autocommit = True
cur = conn.cursor()

def run_iteration():
    customers = []
    # Generate customers
    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()
        
        cur.execute("""
            INSERT INTO customers (first_name, last_name, email)
            VALUES (%s, %s, %s) RETURNING id
        """, (first_name, last_name, email))
        customer_id = cur.fetchone()[0]
        customers.append(customer_id)
    
    # Generate accounts
    accounts = []
    account_types = ['CHECKING', 'SAVINGS']
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_type = random.choice(account_types)
            initial_balance = random_money(Decimal(MIN_INITIAL_BALANCE), Decimal(MAX_INITIAL_BALANCE))
            cur.execute("""
                INSERT INTO accounts (customer_id, account_type, balance, currency)
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (customer_id, account_type, initial_balance, CURRENCY))
            account_id = cur.fetchone()[0]
            accounts.append(account_id)
            
    # Generate transactions
    txn_types = ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER']
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(1.00, MAX_TXN_AMOUNT), 2)
        related_account_id = None
        if txn_type == 'TRANSFER' and len(accounts) > 1:
            related_account_id = random.choice([acc for acc in accounts if acc != account_id])
        description = fake.sentence(nb_words=5)
        cur.execute("""
            INSERT INTO transactions (account_id, amount, transaction_type, description, related_account_id, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (account_id, amount, txn_type, description, related_account_id, "COMPLETED"))

        
    print(f"Generated {len(customers)} customers, {len(accounts)} accounts, and {NUM_TRANSACTIONS} transactions.")
    
try:
    iteration = 0
    while True:
        iteration += 1
        print(f"--- Iteration {iteration} started ---", flush=True)
        run_iteration()
        if not LOOP:
            print("Stopping because LOOP=False", flush=True)
            break
        if iteration >= NUM_ITERATIONS:
            print("Stopping because reached NUM_ITERATIONS", flush=True)
            break

        time.sleep(SLEEP_SECONDS)

except KeyboardInterrupt:
    print("Data generation interrupted by user.", flush=True)
except Exception as e:
    print("CRASH:", repr(e), flush=True)
    
finally:
    cur.close()
    conn.close()
    sys.exit(0)