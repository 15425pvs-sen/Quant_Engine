#!/usr/bin/env python
"""
Download historic OHLCV data for Indian stocks and save one CSV per symbol.

If no symbols are provided on the command line, the script discovers all
equity tables in the SQLite database and downloads data for each one.

Examples:
    py download_indian_stock_history.py
    py download_indian_stock_history.py TCS RELIANCE INFY
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'yfinance'. Install it with: pip install yfinance"
    ) from exc


START_DATE = "2023-01-01"
END_DATE = None
INTERVAL = "1d"
OUTPUT_DIR = Path(__file__).resolve().parent / "historic_data"
DB_NAME = Path(__file__).resolve().parent / "quant_historic_data.db"
IST = timezone(timedelta(hours=5, minutes=30))
EQUITY_CUTOFF_IST = time(hour=16, minute=0)
SYSTEM_TABLES = {
    "stocks_rsi_cagrs",
    "market_data",
    "sqlite_sequence",
    "sqlite_stat1",
    "sqlite_stat2",
    "sqlite_stat3",
    "sqlite_stat4",
    "rsi_heatmap_data",
    "rsi_live_signal_log",
    "quant_engine_runs",
    "quant_engine_steps",
}
EQUITY_REQUIRED_COLS = {"trade_date", "open", "high", "low", "close", "adj_close", "volume"}


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


def get_equity_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()

    equity_tables: list[str] = []
    for (name,) in rows:
        if name in SYSTEM_TABLES:
            continue

        cols = {
            row[1].lower()
            for row in conn.execute(f"PRAGMA table_info({quote_identifier(name)})")
        }
        if EQUITY_REQUIRED_COLS.issubset(cols):
            equity_tables.append(name)

    return equity_tables


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


def is_before_equity_cutoff_ist() -> tuple[bool, datetime, datetime]:
    now_ist = datetime.now(IST)
    cutoff_ist = datetime.combine(now_ist.date(), EQUITY_CUTOFF_IST, tzinfo=IST)
    return now_ist < cutoff_ist, now_ist, cutoff_ist


def compute_effective_end_date(
    start: str,
    requested_end: str | None,
    before_cutoff: bool,
    now_ist: datetime,
) -> tuple[str | None, bool, bool]:
    start_date = pd.to_datetime(start, errors="coerce")
    if pd.isna(start_date):
        return requested_end, False, True

    start_date = start_date.normalize()
    today_ist = pd.Timestamp(now_ist.date())

    requested_end_date: pd.Timestamp | None = None
    if requested_end:
        requested_end_date = pd.to_datetime(requested_end, errors="coerce")
        if not pd.isna(requested_end_date):
            requested_end_date = requested_end_date.normalize()
        else:
            requested_end_date = None

    effective_end_date = requested_end_date
    deferred_today = False

    # yfinance "end" is exclusive. Before the cutoff, force "end=today"
    # so historical backfills still run while today's candle is deferred.
    if before_cutoff and (effective_end_date is None or effective_end_date > today_ist):
        effective_end_date = today_ist
        deferred_today = True

    if effective_end_date is not None and start_date >= effective_end_date:
        return None, deferred_today, False

    return (
        effective_end_date.strftime("%Y-%m-%d") if effective_end_date is not None else requested_end,
        deferred_today,
        True,
    )


def main() -> None:
    args = parse_args()

    before_cutoff, now_ist, cutoff_ist = is_before_equity_cutoff_ist()
    if before_cutoff:
        print(
            "Current IST time is before equity cutoff; historical missing dates will still download, "
            "but today's data will be skipped until cutoff. "
            f"Current IST time: {now_ist.strftime('%Y-%m-%d %H:%M:%S')}. "
            f"Cutoff IST: {cutoff_ist.strftime('%Y-%m-%d %H:%M:%S')}."
        )

    output_dir = Path(args.output_dir).resolve()
    conn = sqlite3.connect(DB_NAME)

    try:
        raw_symbols = args.symbols
        if not raw_symbols:
            raw_symbols = get_equity_tables(conn)
            if not raw_symbols:
                raise SystemExit(
                    "No equity tables found in the database. Provide symbols on the CLI or create equity tables first."
                )
            print(
                "No symbols were provided, so using all equity tables from the database: "
                + ", ".join(raw_symbols)
            )

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

            effective_end, deferred_today, has_valid_range = compute_effective_end_date(
                start=download_start,
                requested_end=args.end,
                before_cutoff=before_cutoff,
                now_ist=now_ist,
            )
            if not has_valid_range:
                if deferred_today:
                    print(
                        f"Skipping {symbol}: only current date data is pending and cutoff is at "
                        f"{cutoff_ist.strftime('%Y-%m-%d %H:%M:%S')} IST."
                    )
                else:
                    print(
                        f"Skipping {symbol}: no valid download range for start={download_start} and end={args.end}."
                    )
                continue

            df = download_symbol_data(symbol, download_start, effective_end, args.interval)

            if df.empty:
                print(f"No new data returned for {symbol}.")
                continue

            saved_path = save_symbol_data(df, symbol, output_dir)
            print(f"Saved {len(df)} downloaded rows to {saved_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
