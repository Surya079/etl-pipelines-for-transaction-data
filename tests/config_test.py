import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from datawarehouse.models import metadata, raw_transactions, core_transactions
from config.settings import settings

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/banking_etl_test",
)


@pytest.fixture(scope="session")
def db_engine():
    """Create test database and engine, create all tables, drop after session."""

    # Create database if it doesn't exist
    if not database_exists(TEST_DATABASE_URL):
        create_database(TEST_DATABASE_URL)

    engine = create_engine(TEST_DATABASE_URL)
    metadata.create_all(engine)

    yield engine

    # Cleanup: drop all tables and database
    metadata.drop_all(engine)
    engine.dispose()
    if database_exists(TEST_DATABASE_URL):
        drop_database(TEST_DATABASE_URL)


@pytest.fixture
def deb_connection(db_engine):
    """Provide a connection for each test, rollback after."""

    with db_engine.connect() as conn:
        trans = conn.begin()
        yield conn
        trans.rollback()


@pytest.fixture
def sample_transaction():
    """Return a list of sample transaction dicts, fixed seed for reproducibility."""
    return [
        {
            "transaction_id": "11111111-1111-1111-1111-111111111111",
            "trace_number": "123456",
            "retrieval_reference_number": "ABC123XYZ",
            "transaction_datetime": "2025-01-01T10:00:00+00:00",
            "local_transaction_datetime": "2025-01-01T05:00:00-05:00",
            "posting_date": "2025-01-01",
            "card_number": "4532 **** **** 1234",
            "card_expiry": "09/25",
            "cardholder_name": "John Doe",
            "card_type": "Credit",
            "card_network": "Visa",
            "card_present": True,
            "merchant_name": "Starbucks",
            "merchant_id": "MID123456",
            "mcc": "5812",
            "mcc_description": "Eating Places",
            "merchant_city": "New York",
            "merchant_country": "United States",
            "merchant_postal_code": "10001",
            "amount": 4.75,
            "currency": "USD",
            "original_amount": 4.75,
            "transaction_type": "Purchase",
            "pos_entry_mode": "Contactless",
            "terminal_id": "T123456",
            "acquirer_bank": "JPMorgan Chase",
            "issuer_bank": "Chase",
            "network": "Visa",
            "authorization_response_code": "00",
            "transaction_status": "Approved",
            "fraud_score": 5,
            "fraud_label": "No",
            "risk_indicators": [],
            "device_info": "iPhone 15 Pro",
            "ip_address": "192.168.1.100",
            "user_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        }
    ]
