"""
Creates the core_transactions table using SQLAlchemy Core.
"""

import sys
from pathlib import Path

from datawarehouse.db import get_engine
from datawarehouse.models import core_transactions, metadata

def create_core_table():
    engine = get_engine()

    metadata.create_all(engine, tables=[core_transactions])
    print("Table 'core_transactions' created successfully.")

if __name__ == "__main__":
    create_core_table()
    