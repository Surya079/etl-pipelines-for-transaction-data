"""
scripts/ingest_transactions.py

Fetches synthetic transactions from the API and inserts them into the raw_transactions table,
mapping each transaction field to its corresponding column.
"""

import argparse
import sys
import time
from pathlib import Path

import requests
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    Boolean,
    Float,
    DateTime,
    MetaData,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datawarehouse.db import get_engine
from config.settings import settings

# Define the raw_transactions table (must match the one in create_raw_table.py)
metadata = MetaData()
raw_transactions = Table(
    "raw_transactions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "ingestion_timestamp",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    ),
    Column("transaction_id", String, nullable=True),
    Column("trace_number", String, nullable=True),
    Column("retrieval_reference_number", String, nullable=True),
    Column("authorization_code", String, nullable=True),
    Column("transaction_datetime", String, nullable=True),
    Column("local_transaction_datetime", String, nullable=True),
    Column("posting_date", String, nullable=True),
    Column("card_number", String, nullable=True),
    Column("card_expiry", String, nullable=True),
    Column("cardholder_name", String, nullable=True),
    Column("card_type", String, nullable=True),
    Column("card_network", String, nullable=True),
    Column("card_present", Boolean, nullable=True),
    Column("merchant_name", String, nullable=True),
    Column("merchant_id", String, nullable=True),
    Column("mcc", String, nullable=True),
    Column("mcc_description", String, nullable=True),
    Column("merchant_city", String, nullable=True),
    Column("merchant_country", String, nullable=True),
    Column("merchant_postal_code", String, nullable=True),
    Column("amount", Float, nullable=True),
    Column("currency", String, nullable=True),
    Column("original_amount", Float, nullable=True),
    Column("transaction_type", String, nullable=True),
    Column("pos_entry_mode", String, nullable=True),
    Column("terminal_id", String, nullable=True),
    Column("acquirer_bank", String, nullable=True),
    Column("issuer_bank", String, nullable=True),
    Column("network", String, nullable=True),
    Column("authorization_response_code", String, nullable=True),
    Column("transaction_status", String, nullable=True),
    Column("fraud_score", Integer, nullable=True),
    Column("fraud_label", String, nullable=True),
    Column("risk_indicators", JSONB, nullable=True),
    Column("device_info", String, nullable=True),
    Column("ip_address", String, nullable=True),
    Column("user_id", String, nullable=True),
    Column("account_id", String, nullable=True),
    Column("api_page", Integer, nullable=True),
)


def fetch_transactions(page: int, limit: int) -> dict:
    url = f"{settings.API_BASE_URL}/transactions?page={page}&limit={limit}"
    print(f"Fetching page {page} (limit={limit})...")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page}: {e}")
        raise


def insert_transactions(transactions: list, page: int) -> int:
    """
    Insert transactions into the raw table, mapping each dict key to a column.
    Only keys that exist in the table columns are inserted; extras are ignored.
    """
    if not transactions:
        return 0

    # Get the set of column names from the table definition
    table_columns = {col.name for col in raw_transactions.columns}

    engine = get_engine()
    with engine.begin() as conn:
        for txn in transactions:
            # Build a dict of only the keys that match table columns
            values = {key: txn.get(key) for key in txn if key in table_columns}
            # Add metadata
            values["api_page"] = page
            if "transaction_id" not in values:
                values["transaction_id"] = txn.get("transaction_id")

            conn.execute(raw_transactions.insert().values(**values))

    return len(transactions)


def main():
    parser = argparse.ArgumentParser(
        description="Ingest synthetic transactions from API to database."
    )
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--end-page", type=int, default=5)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--delay", type=float, default=0.5)
    args = parser.parse_args()

    total_inserted = 0
    for page in range(args.start_page, args.end_page + 1):
        try:
            data = fetch_transactions(page, args.limit)
            items = data.get("data", [])
            if not items:
                print(f"No items found on page {page}, stopping.")
                break
            count = insert_transactions(items, page)
            total_inserted += count
            print(f"Inserted {count} rows from page {page}.")
            time.sleep(args.delay)
        except Exception as e:
            print(f"Failed to process page {page}: {e}")
            continue

    print(f"Ingestion complete. Total rows inserted: {total_inserted}")


if __name__ == "__main__":
    main()
