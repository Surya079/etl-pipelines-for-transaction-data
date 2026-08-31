"""

Database utility module for creating SQLAlchemy engine and optional declarative base.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from config.settings import settings


def get_engine():
    """
    Database utility module for creating SQLAlchemy engine and optional declarative base.
     Returns:
        sqlalchemy.engine.Engine: Configured engine for PostgreSQL.
    """

    return create_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)


Base = declarative_base()

engine = get_engine()
