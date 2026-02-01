"""
LOGLIVE - Database connection layer (Supabase/PostgreSQL)
=========================================================
Provides get_connection, get_placeholder, IS_POSTGRES for sascar_sync and app.
"""

import os
from contextlib import contextmanager
from urllib.parse import unquote

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

# Use psycopg2 for raw cursor (sascar_sync, app)
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

IS_POSTGRES = True


def _parse_url(url):
    """Parse postgresql://user:pass@host:port/db into components for psycopg2."""
    if not url:
        return {}
    
    # Handle quoted strings just in case
    url = url.strip('"\'')
    
    if not url.startswith("postgresql://"):
        return {}
        
    url = url.replace("postgresql://", "")
    if "@" in url:
        auth, rest = url.split("@", 1)
        user, _, password = auth.partition(":")
        # URL decode user and password to handle special chars (e.g. %40 -> @)
        user = unquote(user)
        password = unquote(password)
        host_part = rest
    else:
        user, password = "", ""
        host_part = url
    if "/" in host_part:
        host_port, db = host_part.rsplit("/", 1)
        db = db.split("?")[0]
    else:
        host_port, db = host_part, "postgres"
    if ":" in host_port:
        host, port = host_port.rsplit(":", 1)
        port = int(port) if port.isdigit() else 5432
    else:
        host, port = host_port, 5432
    return {"host": host, "port": port, "dbname": db, "user": user, "password": password}


def get_connection():
    """Return a new psycopg2 connection. Caller must close."""
    if not HAS_PSYCOPG2:
        raise RuntimeError("psycopg2 is required. pip install psycopg2-binary")
    kwargs = _parse_url(DATABASE_URL)
    return psycopg2.connect(**kwargs)


def get_placeholder(n=1):
    """Return placeholder for parameterized query. PostgreSQL uses %s."""
    return ", ".join(["%s"] * n)


def get_pois():
    """Return list of POI dicts from poi_data.py."""
    try:
        from poi_data import POIS_NUPORANGA, POI_RADIUS
        return [(name, coords_list, POI_RADIUS) for name, coords_list in POIS_NUPORANGA.items()]
    except ImportError:
        return []


def migrate_db():
    """Legacy: ensure deslocamentos table has expected columns. No-op in new schema."""
    pass


def manutencao_banco(dias_retencao=30):
    """Optional maintenance: e.g. archive old raw data. No-op by default."""
    pass


@contextmanager
def connection_scope():
    """Context manager for a connection (auto commit/rollback and close)."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
