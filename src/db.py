from contextlib import contextmanager

import psycopg

from src.settings import load_settings


@contextmanager
def get_db_connection():
    settings = load_settings()
    conn = psycopg.connect(settings.database_url)
    try:
        yield conn
    finally:
        conn.close()