import os
from pathlib import Path
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv

# 1. Load environment variables from local .env file
load_dotenv(find_dotenv())

# 2. Safely load Streamlit Cloud secrets if available (ignores errors in CLI/Airflow)
try:
    import streamlit as st
    for k in ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT"]:
        if k in st.secrets:
            os.environ[k] = str(st.secrets[k])
except Exception:
    pass

DB_HOST = os.getenv("DB_HOST", "localhost")
if os.path.exists("/opt/airflow") and DB_HOST in ("localhost", "127.0.0.1", "::1"):
    # Inside Docker container, route DB_HOST to the postgres container
    DB_HOST = "postgres"

DB_NAME = os.getenv("DB_NAME", "telematics_dwh")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASSWORD", "airflow")
DB_PORT = os.getenv("DB_PORT", "5432")

# Database connection URL for SQLAlchemy / Pandas
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Centralized PostgreSQL SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

# Path to the embedded SQLite fallback file for Streamlit Community Cloud (Option B)
SQLITE_FALLBACK_PATH = Path(__file__).resolve().parent.parent.parent / "db" / "telematics_serving.sqlite"


def get_db_connection():
    """Returns a raw psycopg2 connection (with a fast 2-second timeout check)."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=2
    )


def get_db_engine():
    """
    Returns the shared PostgreSQL engine locally, or automatically falls back
    to the embedded SQLite database when deployed on Streamlit Community Cloud.
    """
    try:
        conn = get_db_connection()
        conn.close()
        return engine
    except Exception:
        if SQLITE_FALLBACK_PATH.exists():
            return create_engine(f"sqlite:///{SQLITE_FALLBACK_PATH}")
        return engine


@contextmanager
def get_db_cursor(commit=False, dict_cursor=False):
    """
    Context manager for database cursors.
    Handles automatic cursor/connection cleanup and transaction rollback on errors.
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
