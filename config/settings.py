"""
Loads environment variables from a .env file using python-dotenv.
Exposes settings as a class instance and module-level variables.

"""

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, URL

BASE_DIR = Path(__file__).resolve().parent.parent

ENV_FILE = BASE_DIR / ".env"
print(ENV_FILE)
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    load_dotenv(BASE_DIR / ".env")


class Settings:
    def __init__(self):
        self.POSTGRES_USER = os.getenv("POSTGRES_USER")
        self.POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        self.POSTGRES_HOST = os.getenv("POSTGRES_HOST")
        self.POSTGRES_PORT = os.getenv("POSTGRES_PORT")
        self.POSTGRES_DB = os.getenv("POSTGRES_DB")

        url_object = URL.create(
            drivername="postgresql",
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_HOST,
            port=self.POSTGRES_PORT,
            database=self.POSTGRES_DB,
        )
        # Convert to string, making sure password is not hidden
        self.DATABASE_URL = url_object.render_as_string(hide_password=False)

        self.API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


settings = Settings()

DATABASE_URL = settings.DATABASE_URL
API_BASE_URL = settings.API_BASE_URL

print(DATABASE_URL)
engine = create_engine(settings.DATABASE_URL)
try:
    with engine.connect() as conn:
        print("✅ Database server is running and reachable.")
except Exception as e:
    print("❌ Connection failed:", e)

if __name__ == "__main__":
    # Quick test: run `python config/settings.py`
    print(settings)
