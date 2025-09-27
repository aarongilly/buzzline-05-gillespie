"""
sqlite_emitter.py

Emit messages into a SQLite database.

A SQLite emitter writes each message into a relational table:
- SQLite ships with Python (no install required).
- Provides simple queries and joins via SQL.
- Suitable for local persistence and small projects.

Use this when you want streaming data to land in a local, portable database
or when you want streaming data in a relational store for later analysis.

SQLite: 
INTEGER PRIMARY KEY piggybacks on the rowid and acts like an auto-incrementing key; 
AUTOINCREMENT is not necessary and often discouraged.
"""

import sqlite3
import pathlib
from typing import Mapping, Any

from utils.utils_logger import logger

_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS streamed_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    activity TEXT,
    steps INTEGER,
    heart_rate REAL
);
"""


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(_TABLE_SQL)
    conn.commit()


def emit_message(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """
    Insert one message (dict-like) into SQLite.

    Args:
        message:  Dict-like payload with expected keys.
        db_path:  Path to the SQLite file.

    Returns:
        True on success, False on failure.
    """
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(db_path)) as conn:
            _ensure_table(conn)
            conn.execute(
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
            conn.commit()
        logger.debug(f"[sqlite_emitter] inserted message into {db_path}")
        return True
    except Exception as e:
        logger.error(f"[sqlite_emitter] failed to insert into {db_path}: {e}")
        return False
