"""

Transforms data from raw_transactions and loads into core_transactions.
"""

import sys
from pathlib import Path
from uuid import UUID
from datetime import datetime

from sqlalchemy import insert, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from datawarehouse.db import get_engine
from datawarehouse.models import raw_transactions, core_transactions


def parse_datetime(dt_str):
    """Parse ISO 8601 string to datetime, handling timezone offsets."""
    if not dt_str:
        return None
    # Replace 'Z' with '+00:00' for fromisoformat compatibility
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    return datetime.fromisoformat(dt_str)


def transform_row(raw):
    """
    Convert a raw row (SQLAlchemy Row object) into a dict suitable for core table.
    """
    # transaction_id: convert string to UUID if valid, else skip (or use None)
    txn_id_str = raw.transaction_id
    try:
        txn_id = UUID(txn_id_str) if txn_id_str else None
    except ValueError:
        # Invalid UUID; we can skip this row or generate a new one? Better to skip.
        return None

    # Parse transaction_datetime
    dt_str = raw.transaction_datetime
    dt = parse_datetime(dt_str)
    if dt is None:
        return None

    transaction_date = dt.date()
    transaction_hour = dt.hour

    # fraud_label: convert "Yes"/"No" to boolean
    fraud_label = raw.fraud_label
    if fraud_label is not None:
        fraud_bool = fraud_label.strip().lower() == "yes"
    else:
        fraud_bool = False

    # user_id: similar UUID conversion
    user_id_str = raw.user_id
    user_id = None
    if user_id_str:
        try:
            user_id = UUID(user_id_str)
        except ValueError:
            user_id = None

    # amount is already Numeric; round to 2 decimals just in case
    amount = round(raw.amount, 2) if raw.amount is not None else None

    # Build dict for core_transactions
    core_data = {
        "transaction_id": txn_id,
        "transaction_datetime": dt,
        "transaction_date": transaction_date,
        "transaction_hour": transaction_hour,
        "amount": amount,
        "currency": raw.currency.upper() if raw.currency else None,
        "card_type": raw.card_type,
        "card_network": raw.card_network,
        "merchant_name": raw.merchant_name,
        "mcc": raw.mcc,
        "mcc_description": raw.mcc_description,
        "merchant_country": raw.merchant_country,
        "pos_entry_mode": raw.pos_entry_mode,
        "transaction_type": raw.transaction_type,
        "fraud_label": fraud_bool,
        "fraud_score": raw.fraud_score,
        "user_id": user_id,
        "account_id": raw.account_id,
        "authorization_response_code": raw.authorization_response_code,
        "transaction_status": raw.transaction_status,
        "risk_indicators": raw.risk_indicators,
        "card_present": raw.card_present,
        "local_transaction_datetime": parse_datetime(raw.local_transaction_datetime),
    }
    return core_data


def run_transform():
    engine = get_engine()
    with engine.connect() as conn:
        # Fetch all raw rows (you can add WHERE clause to filter unprocessed)
        result = conn.execute(select(raw_transactions)).fetchall()
        print(f"Fetched {len(result)} raw rows.")

        inserted_count = 0
        skipped_count = 0

        for raw_row in result:
            core_data = transform_row(raw_row)
            if core_data is None:
                skipped_count += 1
                continue

            # PostgreSQL-specific insert with ON CONFLICT DO NOTHING
            stmt = pg_insert(core_transactions).values(**core_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=["transaction_id"])
            conn.execute(stmt)
            inserted_count += 1

        print(f"Inserted: {inserted_count}, Skipped: {skipped_count}")


if __name__ == "__main__":
    run_transform()
