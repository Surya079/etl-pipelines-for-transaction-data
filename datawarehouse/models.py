from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    Date,
    Numeric,
    MetaData,
    ForeignKey,
    Index,
    Float
)

from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func

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


core_transactions = Table(
    "core_transactions",
    metadata,
    Column("transaction_id", UUID(as_uuid=True), primary_key=True),
    Column("transaction_datetime", DateTime(timezone=True), nullable=False),
    Column("transaction_date", Date, nullable=False),
    Column("transaction_hour", Integer, nullable=False),
    Column("amount", Numeric(12, 2), nullable=False),
    Column("currency", String(3), nullable=False),
    Column("card_type", String(20)),
    Column("card_network", String(20)),
    Column("merchant_name", String(255)),
    Column("mcc", String(4)),
    Column("mcc_description", String(255)),
    Column("merchant_country", String(100)),
    Column("pos_entry_mode", String(20)),
    Column("transaction_type", String(20)),
    Column("fraud_label", Boolean, nullable=False, default=False),
    Column("fraud_score", Integer),
    Column("user_id", UUID(as_uuid=True)),
    Column("account_id", String(64)),
    Column("authorization_response_code", String(2)),
    Column("transaction_status", String(20)),
    Column("risk_indicators", JSONB),
    Column("card_present", Boolean),
    Column("local_transaction_datetime", DateTime(timezone=True)),
    Column("ingestion_timestamp", DateTime(timezone=True), server_default=func.now()),
)

# Indexes
Index("idx_core_transactions_datetime", core_transactions.c.transaction_datetime)
Index("idx_core_transactions_merchant", core_transactions.c.merchant_name)
Index("idx_core_transactions_fraud", core_transactions.c.fraud_label)
Index("idx_core_transactions_user", core_transactions.c.user_id)
