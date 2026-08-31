"""
Creates the raw_transactions table in the database using SQLAlchemy Core.
"""

from sqlalchemy import Table, Column, Integer, String, DateTime, JSON, MetaData
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from datawarehouse.db import get_engine


def create_raw_table():
    engine = get_engine()
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
        Column("api_page", Integer, nullable=True),
        Column("data", JSONB, nullable=True),
    )

    metadata.create_all(engine)
    print("Table 'raw_transactions' created successfully.")


if __name__ == "__main__":
    create_raw_table()
