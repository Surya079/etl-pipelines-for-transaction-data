import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy.engine import URL

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=False)


class Settings:
    def __init__(self):
        self.DATABASE_URL = os.getenv("DATABASE_URL")

        if not self.DATABASE_URL:
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

            self.DATABASE_URL = url_object.render_as_string(
                hide_password=False
            )

        self.API_BASE_URL = os.getenv(
            "API_BASE_URL",
            "http://api:8000"
        )


settings = Settings()