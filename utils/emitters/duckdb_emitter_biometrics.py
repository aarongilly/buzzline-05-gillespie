"""
duckdb_emitter.py

Emit messages into a DuckDB database.

A DuckDB emitter targets an in-process OLAP engine:
- Columnar storage optimized for analytics.
- Direct support for Arrow and Parquet formats.
- Great for fast queries on medium-sized data.

Use this for interactive SQL queries over live or 
accumulated streaming data.

DuckDB:
prefers IDENTITY for generated keys; 
its TIMESTAMP is a real temporal type (unlike SQLite's TEXT).
"""
from __future__ import annotations

import pathlib
from typing import Mapping, Any

from utils.utils_logger import logger

try:
    import duckdb
except Exception as e:  # pragma: no cover
    duckdb = None
    _import_err = e
else:
    _import_err = None


_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS streamed_messages (
    timestamp TEXT,
    activity TEXT,
    steps INTEGER,
    heart_rate REAL
);
"""


def emit_message(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """
    Insert one message (dict-like) into DuckDB.

    Args:
        message:  Dict-like payload with expected keys.
        db_path:  Path to the DuckDB file (*.duckdb) or a directory/name.

    Returns:
        True on success, False on failure.
    """
    if duckdb is None:
        logger.error(f"[duckdb_emitter] duckdb not installed: {_import_err}")
        return False

    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(database=str(db_path), read_only=False)
        try:
            con.execute(_TABLE_SQL)
            con.execute(
                """
                INSERT INTO streamed_messages (
                    timestamp, activity, steps, heart_rate
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    message.get("timestamp"),
                    message.get("activity"),
                    int(message.get("steps", 0.0)),
                    float(message.get("heart_rate", 0)),
                ),
            )
            logger.debug(f"[duckdb_emitter] inserted message into {db_path}")
            return True
        finally:
            con.close()
    except Exception as e:
        logger.error(f"[duckdb_emitter] failed to insert into {db_path}: {e}")
        return False
