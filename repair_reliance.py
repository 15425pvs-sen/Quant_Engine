#!/usr/bin/env python
"""
Repair the RELIANCE dataset end-to-end without taking a backup.

What this script does:
1. Deletes the RELIANCE equity table from SQLite
2. Deletes RELIANCE rows from heatmap and live signal tables when present
3. Deletes historic_data/RELIANCE.csv
4. Re-downloads RELIANCE history using the patched downloader
5. Re-ingests RELIANCE into SQLite
6. Recomputes missing RSI values
7. Rebuilds the RELIANCE heatmap

Usage:
    py repair_reliance.py
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "quant_historic_data.db"
CSV_PATH = BASE_DIR / "historic_data" / "RELIANCE.csv"
SYMBOL = "RELIANCE"
HEATMAP_TABLE = "rsi_heatmap_data"
SIGNAL_LOG_TABLE = "rsi_live_signal_log"


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Identifier must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Identifier contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def delete_reliance_artifacts() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        if table_exists(conn, SYMBOL):
            conn.execute(f"DROP TABLE {quote_identifier(SYMBOL)}")
            print(f"Dropped table {SYMBOL}.")

        if table_exists(conn, HEATMAP_TABLE):
            conn.execute(
                f"DELETE FROM {quote_identifier(HEATMAP_TABLE)} WHERE source_table = ?",
                (SYMBOL,),
            )
            print(f"Deleted {SYMBOL} rows from {HEATMAP_TABLE}.")

        if table_exists(conn, SIGNAL_LOG_TABLE):
            conn.execute(
                f"DELETE FROM {quote_identifier(SIGNAL_LOG_TABLE)} WHERE source_table = ?",
                (SYMBOL,),
            )
            print(f"Deleted {SYMBOL} rows from {SIGNAL_LOG_TABLE}.")

        conn.commit()
    finally:
        conn.close()

    if CSV_PATH.exists():
        CSV_PATH.unlink()
        print(f"Deleted CSV {CSV_PATH}.")


def run_step(script_name: str, args: list[str] | None = None) -> None:
    cmd = [sys.executable, str(BASE_DIR / script_name)]
    if args:
        cmd.extend(args)

    print(f"\n=== Running {script_name} ===")
    subprocess.run(cmd, check=True, cwd=str(BASE_DIR))


def main() -> None:
    delete_reliance_artifacts()
    run_step("download_indian_stock_history.py", [SYMBOL])
    run_step("ingest_yfinance_to_sqlite.py", [SYMBOL])
    run_step("calculate_rsi_fill_db_frozen.py", [SYMBOL])
    run_step("build_rsi_heatmap_table.py", [SYMBOL, "--force"])
    print("\nRELIANCE repair completed.")


if __name__ == "__main__":
    main()
