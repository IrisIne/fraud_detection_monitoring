"""PostgreSQL access helpers for the Airflow pipelines."""

from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator

import psycopg2
from psycopg2.extensions import connection

from .config import Settings


@contextmanager
def connect() -> Iterator[connection]:
    """Open a transaction and commit it only when the task succeeds."""

    database = psycopg2.connect(Settings().database_dsn)
    try:
        yield database
        database.commit()
    except Exception:
        database.rollback()
        raise
    finally:
        database.close()
