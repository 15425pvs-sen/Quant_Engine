#!/usr/bin/env python
"""
Ingest downloaded yfinance CSV files into SQLite, one table per equity.

The table name is derived from the CSV file name, for example:
    historic_data/TCS.csv -> table TCS

The created table schema is compatible with yfinance historic data:
    trade_date, open, high, low, close, adj_close, volume, rsi

RSI is created as NULL for all imported rows. Existing populated equity tables
are updated only with rows newer than the latest stored trade_date.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd


DB_NAME = "quant_historic_data.db"
CSV_BASE_PATH = Path(__file__).resolve().parent / "historic_data"


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Identifier must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Identifier contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


def normalize_table_name(file_name: str) -> str:
    table_name = Path(file_name).stem.strip().upper()
    if not table_name:
        raise ValueError(f"Invalid file name: {file_name}")
    return table_name


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


def table_has_rows(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {quote_identifier(table_name)} LIMIT 1"
    ).fetchone()
    return row is not None


def get_latest_trade_date(conn: sqlite3.Connection, table_name: str) -> str | None:
    if not table_exists(conn, table_name):
        return None

    row = conn.execute(
        f"SELECT MAX(trade_date) FROM {quote_identifier(table_name)}"
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0]).strip()


def create_equity_table(conn: sqlite3.Connection, table_name: str) -> None:
    safe_table = quote_identifier(table_name)
    safe_suffix = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in table_name)

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {safe_table} (
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume INTEGER,
            rsi REAL
        )
        """
    )
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{safe_suffix}_unique_trade_date
        ON {safe_table}(trade_date)
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{safe_suffix}_trade_date
        ON {safe_table}(trade_date)
        """
    )
    conn.commit()


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    raw_dates = df["trade_date"].astype(str).str.strip()

    parsed_dates = pd.to_datetime(raw_dates, format="%Y-%m-%d", errors="coerce")

    missing_mask = parsed_dates.isna()
    if missing_mask.any():
        parsed_dates.loc[missing_mask] = pd.to_datetime(
            raw_dates.loc[missing_mask],
            format="%m-%d-%Y",
            errors="coerce",
        )

    missing_mask = parsed_dates.isna()
    if missing_mask.any():
        parsed_dates.loc[missing_mask] = pd.to_datetime(
            raw_dates.loc[missing_mask],
            format="%d-%m-%Y",
            errors="coerce",
        )

    df["trade_date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    return df.dropna(subset=["trade_date"])


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        col.strip().lower().replace(" ", "_").replace(".", "")
        for col in df.columns
    ]

    column_map = {
        "date": "trade_date",
        "timestamp": "trade_date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "adj_close": "adj_close",
        "adjclose": "adj_close",
        "volume": "volume",
    }
    df = df.rename(columns=column_map)

    if "trade_date" not in df.columns:
        raise RuntimeError("trade_date column missing after column normalization.")

    df = normalize_dates(df)

    for numeric_col in ("open", "high", "low", "close", "adj_close", "volume"):
        if numeric_col in df.columns:
            df[numeric_col] = pd.to_numeric(
                df[numeric_col].astype(str).str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            )

    for price_col in ("open", "high", "close"):
        if price_col in df.columns:
            df[price_col] = df[price_col].round(2)

    df["rsi"] = pd.NA

    final_columns = [
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "rsi",
    ]

    for col in final_columns:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[final_columns].drop_duplicates(subset=["trade_date"]).sort_values("trade_date")
    return df.reset_index(drop=True)


def insert_new_rows(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> int:
    latest_trade_date = get_latest_trade_date(conn, table_name)
    if latest_trade_date is not None:
        df = df[df["trade_date"] > latest_trade_date].copy()

    if df.empty:
        return 0

    df.to_sql(table_name, conn, if_exists="append", index=False)
    conn.commit()
    return len(df)


def import_csv_if_needed(conn: sqlite3.Connection, csv_path: Path) -> int:
    equity = normalize_table_name(csv_path.name)

    print(f"\nProcessing {equity}")

    df = pd.read_csv(csv_path, dtype=str)
    df = normalize_dataframe(df)

    if not table_exists(conn, equity):
        print(f"Creating table {equity}")
        create_equity_table(conn, equity)
    elif table_has_rows(conn, equity):
        print(f"Table {equity} exists. Importing only missing rows.")
    else:
        print(f"Table {equity} exists but is empty. Importing data.")

    inserted = insert_new_rows(conn, equity, df)
    print(f"Inserted {inserted} rows into {equity}")
    return inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest yfinance CSV files into SQLite."
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        help="Optional stock symbols such as TCS RELIANCE INFY",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not CSV_BASE_PATH.exists():
        raise RuntimeError(f"Folder not found: {CSV_BASE_PATH}")

    csv_files = sorted(path for path in CSV_BASE_PATH.iterdir() if path.suffix.lower() == ".csv")
    if not csv_files:
        raise RuntimeError("No CSV files found in historic_data.")

    if args.symbols:
        requested = {symbol.strip().upper() for symbol in args.symbols if symbol.strip()}
        csv_files = [path for path in csv_files if normalize_table_name(path.name) in requested]
        if not csv_files:
            raise RuntimeError("No matching CSV files found for requested symbols.")

    conn = sqlite3.connect(DB_NAME)
    try:
        for csv_file in csv_files:
            import_csv_if_needed(conn, csv_file)
    finally:
        conn.close()

    print("\nYFinance CSV ingestion complete.")


if __name__ == "__main__":
    main()
