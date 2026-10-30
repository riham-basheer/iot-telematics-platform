import os
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv

# Load environment variables from .env file
load_dotenv(find_dotenv())

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "telematics_dwh")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASSWORD", "airflow")
DB_PORT = os.getenv("DB_PORT", "5432")

# Database connection URL for SQLAlchemy / Pandas
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Centralized SQLAlchemy engine with connection health check
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)


def get_db_engine():
    """Returns the shared SQLAlchemy engine."""
    return engine


def get_db_connection():
    """Returns a raw psycopg2 connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


@contextmanager
def get_db_cursor(commit=False, dict_cursor=False):
    """
    Context manager for database cursors.
    Handles automatic cursor/connection cleanup and transaction rollback on errors.
    
    Usage:
        with get_db_cursor(commit=True) as cur:
            cur.execute("INSERT INTO ...")
    """
    conn = get_db_connection()
    cursor_factory = RealDictCursor if dict_cursor else None
    cur = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(" Database connection successful!")
            print(f"Connected to: {version}")
    except Exception as e:
        print(" Database connection failed:", e)
        