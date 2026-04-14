#!/usr/bin/env python
"""
Compute RSI(14) with standard Wilder smoothing and write it back to equity tables.

Usage:
    py calculate_rsi_fill_db_frozen.py
    py calculate_rsi_fill_db_frozen.py TCS
    py calculate_rsi_fill_db_frozen.py TCS INFY RELIANCE
"""

import sys
import sqlite3
import pandas as pd
import numpy as np

DB_NAME = "quant_historic_data.db"
RSI_PERIOD = 14
SYSTEM_TABLES = {
    "stocks_rsi_cagrs",
    "market_data",
    "sqlite_sequence",
    "sqlite_stat1",
    "sqlite_stat2",
    "sqlite_stat3",
    "sqlite_stat4",
}
REQUIRED_COLS = {"trade_date", "close", "rsi"}


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Table name must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Table name contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


def compute_rsi_1day_seed(close: pd.Series, period: int = 14) -> pd.Series:
    """
    Compute RSI using standard Wilder smoothing.

    Returns NaN for the first `period` rows.
    """
    if len(close) < period + 1:
        return pd.Series(np.nan, index=close.index, dtype=float)

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    rsi = pd.Series(np.nan, index=close.index, dtype=float)

    avg_gain = gain.iloc[1 : period + 1].mean()
    avg_loss = loss.iloc[1 : period + 1].mean()

    if avg_loss == 0:
        rsi.iloc[period] = 100.0 if avg_gain > 0 else 50.0
    else:
        rs = avg_gain / avg_loss
        rsi.iloc[period] = 100 - (100 / (1 + rs))

    for idx in range(period + 1, len(close)):
        avg_gain = ((avg_gain * (period - 1)) + gain.iloc[idx]) / period
        avg_loss = ((avg_loss * (period - 1)) + loss.iloc[idx]) / period

        if avg_loss == 0:
            rsi.iloc[idx] = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi.iloc[idx] = 100 - (100 / (1 + rs))

    return rsi.round(2)


def get_equity_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    valid = []
    for (name,) in rows:
        if name in SYSTEM_TABLES:
            continue
        cols = {row[1].lower() for row in conn.execute(f"PRAGMA table_info({quote_identifier(name)})")}
        if REQUIRED_COLS.issubset(cols):
            valid.append(name)
    return valid


def load_equity_data(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    df = pd.read_sql(
        f"""
        SELECT trade_date, close, rsi
        FROM {quote_identifier(table)}
        WHERE close IS NOT NULL
        ORDER BY trade_date ASC
        """,
        conn,
    )

    if df.empty:
        return df

    df["trade_date_raw"] = df["trade_date"].astype(str).str.strip()
    df["trade_date_dt"] = pd.to_datetime(df["trade_date_raw"], errors="coerce")
    df["close"] = pd.to_numeric(
        df["close"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )
    df["rsi_existing"] = pd.to_numeric(
        df["rsi"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )
    df = df.dropna(subset=["trade_date_dt", "close"]).sort_values("trade_date_dt").reset_index(drop=True)
    return df


def write_rsi_to_table(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    df["rsi"] = compute_rsi_1day_seed(df["close"], period=RSI_PERIOD)
    update_data = [
        (float(row["rsi"]), row["trade_date_raw"])
        for _, row in df.iterrows()
        if pd.isna(row["rsi_existing"]) and not pd.isna(row["rsi"])
    ]

    if not update_data:
        return 0

    cursor = conn.cursor()
    cursor.executemany(
        f"""
        UPDATE {quote_identifier(table)}
        SET rsi = ?
        WHERE trade_date = ?
        """,
        update_data,
    )
    conn.commit()
    return len(update_data)


def main() -> None:
    requested_tables = [name.strip() for name in sys.argv[1:] if name.strip()]

    conn = sqlite3.connect(DB_NAME)
    try:
        available_tables = get_equity_tables(conn)
        if requested_tables:
            available_lookup = {name.upper(): name for name in available_tables}
            tables = []
            for requested in requested_tables:
                matched = available_lookup.get(requested.upper())
                if matched is None:
                    print(f"Skipping {requested}: table not found or missing required columns.")
                    continue
                tables.append(matched)
        else:
            tables = available_tables

        if not tables:
            print("No valid equity tables found.")
            return

        for table in tables:
            print(f"Processing {table}...")
            df = load_equity_data(conn, table)
            if df.empty:
                print(f"  No usable rows found in {table}.")
                continue

            written = write_rsi_to_table(conn, table, df)
            print(f"  RSI({RSI_PERIOD}) written for {written} rows.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
