"""
scripts/create_raw_table.py

Creates the raw_transactions table with one column per transaction field.
"""

import sys
from pathlib import Path

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
from datawarehouse.db import get_engine


def create_raw_table():
    engine = get_engine()
    metadata = MetaData()

    raw_transactions = Table(
        "raw_transactions",
        metadata,
        # System columns
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column(
            "ingestion_timestamp",
            DateTime(timezone=True),
            server_default=func.now(),
            nullable=False,
        ),
        # Transaction identifiers
        Column("transaction_id", String, nullable=True),
        Column("trace_number", String, nullable=True),
        Column("retrieval_reference_number", String, nullable=True),
        Column("authorization_code", String, nullable=True),
        # Timestamps
        Column("transaction_datetime", String, nullable=True),
        Column("local_transaction_datetime", String, nullable=True),
        Column("posting_date", String, nullable=True),
        # Card details
        Column("card_number", String, nullable=True),
        Column("card_expiry", String, nullable=True),
        Column("cardholder_name", String, nullable=True),
        Column("card_type", String, nullable=True),
        Column("card_network", String, nullable=True),
        Column("card_present", Boolean, nullable=True),
        # Merchant details
        Column("merchant_name", String, nullable=True),
        Column("merchant_id", String, nullable=True),
        Column("mcc", String, nullable=True),
        Column("mcc_description", String, nullable=True),
        Column("merchant_city", String, nullable=True),
        Column("merchant_country", String, nullable=True),
        Column("merchant_postal_code", String, nullable=True),
        # Transaction details
        Column("amount", Float, nullable=True),
        Column("currency", String, nullable=True),
        Column("original_amount", Float, nullable=True),
        Column("transaction_type", String, nullable=True),
        Column("pos_entry_mode", String, nullable=True),
        Column("terminal_id", String, nullable=True),
        # Network / processing
        Column("acquirer_bank", String, nullable=True),
        Column("issuer_bank", String, nullable=True),
        Column("network", String, nullable=True),
        Column("authorization_response_code", String, nullable=True),
        Column("transaction_status", String, nullable=True),
        # Fraud and risk
        Column("fraud_score", Integer, nullable=True),
        Column("fraud_label", String, nullable=True),
        Column("risk_indicators", JSONB, nullable=True),
        # Additional fields
        Column("device_info", String, nullable=True),
        Column("ip_address", String, nullable=True),
        Column("user_id", String, nullable=True),
        Column("account_id", String, nullable=True),
        # Metadata about the ingestion
        Column("api_page", Integer, nullable=True),
    )

    metadata.create_all(engine)
    print(
        "Table 'raw_transactions' created successfully with columns for each transaction field."
    )


if __name__ == "__main__":
    create_raw_table()
