#!/usr/bin/env python
"""
Download historic OHLCV data for Indian stocks and save one CSV per symbol.

Update STOCK_SYMBOLS with the stocks you want to download, then run:
    py download_indian_stock_history.py

Optional:
    py download_indian_stock_history.py TCS RELIANCE INFY
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import timedelta
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'yfinance'. Install it with: pip install yfinance"
    ) from exc


STOCK_SYMBOLS = [
    "TCS",
    "RELIANCE",
    "INFY",
]

START_DATE = "2023-01-01"
END_DATE = None
INTERVAL = "1d"
OUTPUT_DIR = Path(__file__).resolve().parent / "historic_data"
DB_NAME = Path(__file__).resolve().parent / "quant_historic_data.db"


def normalize_symbol(symbol: str) -> str:
    cleaned = symbol.strip().upper()
    if not cleaned:
        raise ValueError("Stock symbol cannot be empty.")
    if not cleaned.endswith(".NS"):
        cleaned = f"{cleaned}.NS"
    return cleaned


def output_name(symbol: str) -> str:
    return symbol.replace(".NS", "")


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


def get_latest_trade_date(conn: sqlite3.Connection, table_name: str) -> pd.Timestamp | None:
    if not table_exists(conn, table_name):
        return None

    row = conn.execute(
        f"SELECT MAX(trade_date) FROM {quote_identifier(table_name)}"
    ).fetchone()

    if row is None or row[0] is None:
        return None

    latest_date = pd.to_datetime(str(row[0]).strip(), errors="coerce")
    if pd.isna(latest_date):
        return None
    return latest_date.normalize()


def determine_download_start(
    conn: sqlite3.Connection,
    symbol: str,
    requested_start: str,
) -> tuple[str | None, pd.Timestamp | None]:
    table_name = output_name(symbol)
    latest_trade_date = get_latest_trade_date(conn, table_name)

    if latest_trade_date is None:
        return requested_start, None

    next_date = latest_trade_date + timedelta(days=1)
    today = pd.Timestamp.today().normalize()

    if next_date > today:
        return None, latest_trade_date

    return next_date.strftime("%Y-%m-%d"), latest_trade_date


def download_symbol_data(symbol: str, start: str, end: str | None, interval: str) -> pd.DataFrame:
    df = yf.download(
        tickers=symbol,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df.columns = [str(col).strip() for col in df.columns]

    if "Date" not in df.columns:
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "Date"})

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["Date"])

    ordered_columns = [
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
    ]

    available_columns = [col for col in ordered_columns if col in df.columns]
    return df[available_columns]


def save_symbol_data(df: pd.DataFrame, symbol: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"{output_name(symbol)}.csv"

    if file_path.exists():
        existing_df = pd.read_csv(file_path, dtype=str)
        combined_df = pd.concat([existing_df, df.astype(str)], ignore_index=True)
        if "Date" in combined_df.columns:
            combined_df["Date_dt"] = pd.to_datetime(
                combined_df["Date"].astype(str).str.strip(),
                errors="coerce",
            )
            combined_df = (
                combined_df.sort_values("Date_dt")
                .drop_duplicates(subset=["Date"], keep="last")
                .drop(columns=["Date_dt"])
            )
        combined_df.to_csv(file_path, index=False)
    else:
        df.to_csv(file_path, index=False)

    return file_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download historic data for Indian stocks."
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        help="Optional stock symbols such as TCS RELIANCE INFY",
    )
    parser.add_argument(
        "--start",
        default=START_DATE,
        help=f"Start date in YYYY-MM-DD format. Default: {START_DATE}",
    )
    parser.add_argument(
        "--end",
        default=END_DATE,
        help="Optional end date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--interval",
        default=INTERVAL,
        help=f"Download interval. Default: {INTERVAL}",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help=f"Output folder for CSV files. Default: {OUTPUT_DIR}",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_symbols = args.symbols or STOCK_SYMBOLS

    if not raw_symbols:
        raise SystemExit("Provide at least one stock symbol in STOCK_SYMBOLS or via CLI.")

    output_dir = Path(args.output_dir).resolve()
    conn = sqlite3.connect(DB_NAME)

    try:
        for raw_symbol in raw_symbols:
            try:
                symbol = normalize_symbol(raw_symbol)
            except ValueError as exc:
                print(f"Skipping invalid symbol '{raw_symbol}': {exc}")
                continue

            download_start, latest_trade_date = determine_download_start(conn, symbol, args.start)

            if download_start is None:
                print(f"Skipping {symbol}: database already has data through {latest_trade_date.date()}.")
                continue

            if latest_trade_date is None:
                print(f"Downloading full history for {symbol} from {download_start}...")
            else:
                print(
                    f"Downloading missing history for {symbol} "
                    f"from {download_start} after {latest_trade_date.date()}..."
                )

            df = download_symbol_data(symbol, download_start, args.end, args.interval)

            if df.empty:
                print(f"No new data returned for {symbol}.")
                continue

            saved_path = save_symbol_data(df, symbol, output_dir)
            print(f"Saved {len(df)} downloaded rows to {saved_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
