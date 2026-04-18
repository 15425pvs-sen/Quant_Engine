#!/usr/bin/env python
"""
Run the end-to-end quant data pipeline in sync.

Pipeline order:
1. Download stock history
2. Ingest CSV data into SQLite
3. Fill RSI only for missing rows
4. Build RSI heatmap data for tables that changed or are missing heatmap rows
5. Generate BUY/SELL signals from the last two stored RSI rows

Usage:
    py quant_engine.py
    py quant_engine.py TCS RELIANCE INFY
    py quant_engine.py TCS --force-heatmap
"""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "quant_historic_data.db"
HEATMAP_TABLE = "rsi_heatmap_data"
IST = timezone(timedelta(hours=5, minutes=30))
SYSTEM_TABLES = {
    "stocks_rsi_cagrs",
    "market_data",
    "sqlite_sequence",
    "sqlite_stat1",
    "sqlite_stat2",
    "sqlite_stat3",
    "sqlite_stat4",
    HEATMAP_TABLE,
    "quant_engine_runs",
    "quant_engine_steps",
}
REQUIRED_COLS = {"trade_date", "close", "rsi"}


def utc_now() -> str:
    return datetime.now(IST).replace(microsecond=0).isoformat()


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Identifier must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Identifier contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full quant data update pipeline."
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        help="Optional stock symbols such as TCS RELIANCE INFY",
    )
    parser.add_argument(
        "--force-heatmap",
        action="store_true",
        help="Rebuild heatmap data even if it already exists.",
    )
    return parser.parse_args()


def normalize_cli_symbols(symbols: list[str]) -> list[str]:
    cleaned = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
    if any(symbol == "RUN-ALL" for symbol in cleaned):
        return []
    return cleaned


def ensure_engine_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quant_engine_runs (
            run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at      TEXT NOT NULL,
            finished_at     TEXT,
            status          TEXT NOT NULL,
            symbols         TEXT,
            force_heatmap   INTEGER NOT NULL DEFAULT 0,
            error_message   TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quant_engine_steps (
            step_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            step_name       TEXT NOT NULL,
            started_at      TEXT NOT NULL,
            finished_at     TEXT,
            status          TEXT NOT NULL,
            details         TEXT,
            FOREIGN KEY (run_id) REFERENCES quant_engine_runs(run_id)
        )
        """
    )
    conn.commit()


def create_run(conn: sqlite3.Connection, symbols: list[str], force_heatmap: bool) -> int:
    cursor = conn.execute(
        """
        INSERT INTO quant_engine_runs (started_at, status, symbols, force_heatmap)
        VALUES (?, ?, ?, ?)
        """,
        (utc_now(), "running", ",".join(symbols), int(force_heatmap)),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, status: str, error_message: str | None = None) -> None:
    conn.execute(
        """
        UPDATE quant_engine_runs
        SET finished_at = ?, status = ?, error_message = ?
        WHERE run_id = ?
        """,
        (utc_now(), status, error_message, run_id),
    )
    conn.commit()


def create_step(conn: sqlite3.Connection, run_id: int, step_name: str) -> int:
    cursor = conn.execute(
        """
        INSERT INTO quant_engine_steps (run_id, step_name, started_at, status)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, step_name, utc_now(), "running"),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_step(conn: sqlite3.Connection, step_id: int, status: str, details: str | None = None) -> None:
    conn.execute(
        """
        UPDATE quant_engine_steps
        SET finished_at = ?, status = ?, details = ?
        WHERE step_id = ?
        """,
        (utc_now(), status, details, step_id),
    )
    conn.commit()


def preflight_database(db_path: Path) -> None:
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute("PRAGMA quick_check").fetchone()
        conn.execute("BEGIN IMMEDIATE")
        conn.rollback()
    finally:
        conn.close()


def run_step(run_conn: sqlite3.Connection, run_id: int, step_name: str, script_name: str, args: list[str] | None = None) -> None:
    cmd = [sys.executable, str(BASE_DIR / script_name)]
    if args:
        cmd.extend(args)

    print(f"\n=== Running {script_name} ===")
    step_id = create_step(run_conn, run_id, step_name)

    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            check=True,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            print(result.stdout.rstrip())
        if result.stderr:
            print(result.stderr.rstrip())
        finish_step(run_conn, step_id, "success", result.stdout[-4000:] if result.stdout else None)
    except subprocess.CalledProcessError as exc:
        combined_output = "\n".join(part for part in [exc.stdout, exc.stderr] if part)
        if exc.stdout:
            print(exc.stdout.rstrip())
        if exc.stderr:
            print(exc.stderr.rstrip())
        finish_step(run_conn, step_id, "failed", combined_output[-4000:] if combined_output else None)
        raise RuntimeError(f"Step failed: {script_name}") from exc


def get_equity_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    valid = []
    for (name,) in rows:
        if name in SYSTEM_TABLES:
            continue
        cols = {
            row[1].lower()
            for row in conn.execute(f"PRAGMA table_info({quote_identifier(name)})")
        }
        if REQUIRED_COLS.issubset(cols):
            valid.append(name)
    return valid


def get_table_snapshot(conn: sqlite3.Connection, tables: list[str] | None = None) -> dict[str, dict[str, object]]:
    target_tables = tables or get_equity_tables(conn)
    snapshot: dict[str, dict[str, object]] = {}

    for table in target_tables:
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS row_count,
                MAX(trade_date) AS max_trade_date,
                SUM(CASE WHEN rsi IS NULL THEN 1 ELSE 0 END) AS null_rsi_count
            FROM {quote_identifier(table)}
            """
        ).fetchone()
        snapshot[table] = {
            "row_count": int(row[0] or 0),
            "max_trade_date": row[1],
            "null_rsi_count": int(row[2] or 0),
        }
    return snapshot


def get_heatmap_covered_tables(conn: sqlite3.Connection) -> set[str]:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (HEATMAP_TABLE,),
    ).fetchone()
    if row is None:
        return set()

    rows = conn.execute(
        f"SELECT DISTINCT source_table FROM {quote_identifier(HEATMAP_TABLE)}"
    ).fetchall()
    return {row[0] for row in rows if row[0]}


def resolve_target_tables(conn: sqlite3.Connection, symbols: list[str]) -> list[str]:
    equity_tables = get_equity_tables(conn)
    if not symbols:
        return equity_tables

    lookup = {table.upper(): table for table in equity_tables}
    resolved = []
    for symbol in symbols:
        matched = lookup.get(symbol.upper())
        if matched is None:
            print(f"Skipping {symbol}: equity table not found.")
            continue
        resolved.append(matched)
    return resolved


def describe_selected_tables(conn: sqlite3.Connection, symbols: list[str]) -> list[str]:
    selected_tables = resolve_target_tables(conn, symbols)
    if symbols:
        return selected_tables

    if not selected_tables:
        print("No equity tables found in the database.")
        return selected_tables

    print(
        "No symbols were provided, so the pipeline will process these equity tables: "
        + ", ".join(selected_tables)
    )
    return selected_tables


def get_tables_for_heatmap(
    conn: sqlite3.Connection,
    symbols: list[str],
    force_heatmap: bool,
) -> list[str]:
    selected_tables = resolve_target_tables(conn, symbols)
    if force_heatmap:
        return selected_tables

    covered_tables = get_heatmap_covered_tables(conn)
    return [table for table in selected_tables if table not in covered_tables]


def describe_heatmap_selection(
    conn: sqlite3.Connection,
    symbols: list[str],
    force_heatmap: bool,
) -> list[str]:
    selected_tables = resolve_target_tables(conn, symbols)

    if force_heatmap:
        if selected_tables:
            print(
                "Heatmap step will rebuild all selected tables: "
                + ", ".join(selected_tables)
            )
        else:
            print("Heatmap step will rebuild all selected tables: none")
        return selected_tables

    covered_tables = get_heatmap_covered_tables(conn)
    heatmap_tables = get_tables_for_heatmap(conn, symbols, force_heatmap=False)
    skipped_tables = [table for table in selected_tables if table in covered_tables]

    if heatmap_tables:
        print(
            "Heatmap step will build: "
            + ", ".join(heatmap_tables)
        )
    else:
        print("Heatmap step will build: none")

    if skipped_tables:
        print(
            "Heatmap step will skip existing rows for: "
            + ", ".join(skipped_tables)
        )

    return heatmap_tables


def main() -> None:
    args = parse_args()
    symbol_args = normalize_cli_symbols(args.symbols)

    run_conn = sqlite3.connect(DB_PATH)
    try:
        ensure_engine_tables(run_conn)
        describe_selected_tables(run_conn, symbol_args)
        run_id = create_run(run_conn, symbol_args, args.force_heatmap)

        try:
            preflight_database(DB_PATH)

            run_step(run_conn, run_id, "download", "download_indian_stock_history.py", symbol_args)
            run_step(run_conn, run_id, "ingest", "ingest_yfinance_to_sqlite.py", symbol_args)
            run_step(run_conn, run_id, "rsi_fill", "calculate_rsi_fill_db_frozen.py", symbol_args)

            heatmap_tables = describe_heatmap_selection(run_conn, symbol_args, args.force_heatmap)
            if not heatmap_tables:
                print("\n=== Heatmap step skipped: no missing heatmap tables found ===")
            else:
                for table in heatmap_tables:
                    heatmap_args = [table]
                    if args.force_heatmap:
                        heatmap_args.append("--force")
                    run_step(run_conn, run_id, f"heatmap:{table}", "build_rsi_heatmap_table.py", heatmap_args)

            signal_args = symbol_args + ["--check-last-rsi"]
            run_step(
                run_conn,
                run_id,
                "signals:last_rsi",
                "rsi_live_signal_engine.py",
                signal_args,
            )

            finish_run(run_conn, run_id, "success")
            print(f"\nPipeline run completed successfully. Run ID: {run_id}")
        except Exception as exc:
            finish_run(run_conn, run_id, "failed", str(exc))
            raise
    finally:
        run_conn.close()


if __name__ == "__main__":
    main()
