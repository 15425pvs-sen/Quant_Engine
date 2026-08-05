#!/usr/bin/env python
"""
Validate SELL->BUY closure integrity in rsi_live_signal_log.

Usage:
    py validate_sell_buy_closure.py
    py validate_sell_buy_closure.py --db d:\\QuantApplication\\Database_Quant\\quant_historic_data.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "quant_historic_data.db"
SIGNAL_LOG_TABLE = "rsi_live_signal_log"
REQUIRED_COLS = {
    "id",
    "source_table",
    "signal_type",
    "signal_timestamp",
    "position_state",
    "buy_signal_id",
    "closed_by_signal_id",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate that SELL rows close their linked BUY rows correctly."
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite DB (default: {DEFAULT_DB_PATH})",
    )
    return parser.parse_args()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def fetch_issues(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        f"""
        SELECT
            s.id AS sell_id,
            s.source_table AS sell_symbol,
            s.signal_timestamp AS sell_ts,
            s.position_state AS sell_state,
            s.buy_signal_id AS linked_buy_id,
            b.id AS buy_id,
            b.signal_type AS buy_type,
            b.position_state AS buy_state,
            b.closed_by_signal_id AS buy_closed_by
        FROM {SIGNAL_LOG_TABLE} s
        LEFT JOIN {SIGNAL_LOG_TABLE} b
          ON b.id = s.buy_signal_id
        WHERE s.signal_type = 'SELL'
          AND s.buy_signal_id IS NOT NULL
          AND (
              b.id IS NULL
              OR b.signal_type <> 'BUY'
              OR COALESCE(b.position_state, 'OPEN') <> 'CLOSED'
              OR COALESCE(b.closed_by_signal_id, -1) <> s.id
          )
        ORDER BY s.signal_timestamp DESC, s.id DESC
        """
    ).fetchall()


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]).lower() for row in rows}


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        print(f"FAIL: DB not found: {db_path}")
        return 2

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if not table_exists(conn, SIGNAL_LOG_TABLE):
            print(f"FAIL: table not found: {SIGNAL_LOG_TABLE}")
            return 2
        present_cols = get_table_columns(conn, SIGNAL_LOG_TABLE)
        missing_cols = sorted(REQUIRED_COLS - present_cols)
        if missing_cols:
            print(
                "FAIL: table schema is missing required column(s): "
                + ", ".join(missing_cols)
            )
            print(
                "Run signal_api.py or rsi_live_signal_engine.py once to auto-migrate the table, then re-run this validator."
            )
            return 2

        total_sell = conn.execute(
            f"SELECT COUNT(*) FROM {SIGNAL_LOG_TABLE} WHERE signal_type = 'SELL'"
        ).fetchone()[0]
        total_sell_linked = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {SIGNAL_LOG_TABLE}
            WHERE signal_type = 'SELL' AND buy_signal_id IS NOT NULL
            """
        ).fetchone()[0]
        issues = fetch_issues(conn)
    finally:
        conn.close()

    print(f"DB: {db_path}")
    print(f"SELL rows total: {total_sell}")
    print(f"SELL rows with buy_signal_id: {total_sell_linked}")

    if not issues:
        print("PASS: All linked SELL rows correctly close their BUY rows.")
        return 0

    print(f"FAIL: Found {len(issues)} SELL->BUY closure issue(s):")
    for row in issues:
        print(
            "  "
            f"sell_id={row['sell_id']} symbol={row['sell_symbol']} sell_state={row['sell_state']} "
            f"linked_buy_id={row['linked_buy_id']} buy_id={row['buy_id']} buy_type={row['buy_type']} "
            f"buy_state={row['buy_state']} buy_closed_by={row['buy_closed_by']}"
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())
