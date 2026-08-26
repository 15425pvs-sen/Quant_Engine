#!/usr/bin/env python
"""
Read-only RSI checker for open BUY positions in rsi_live_signal_log_trading.

For every open BUY position present in the live signal log table, this script:
1. Loads recent close history from the stock's own database table.
2. Fetches the latest live LTP with yfinance.
3. Computes the current RSI using the same Wilder RSI math as the live tracker.
4. Reports that position's SELL exit_rsi level and the remaining RSI gap.

No BUY or SELL rows are written and no orders are placed.

Usage:
    py check_rsi_sell_gap.py
    py check_rsi_sell_gap.py --symbols RELIANCE TCS
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'yfinance'. Install it with: pip install yfinance"
    ) from exc


DB_NAME = Path(__file__).resolve().parent / "quant_historic_data.db"
SIGNAL_LOG_TABLE = "rsi_live_signal_log_trading"
RSI_PERIOD = 14
LTP_HISTORY_PERIOD = "5d"
LTP_INTERVAL = "1m"


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Identifier must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Identifier contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check current RSI and the gap to the next SELL threshold."
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="Optional stock symbols to filter, for example RELIANCE TCS INFY.",
    )
    return parser.parse_args()


def load_open_buy_positions(
    conn: sqlite3.Connection,
    requested_symbols: list[str] | None = None,
) -> pd.DataFrame:
    if not table_exists(conn, SIGNAL_LOG_TABLE):
        raise RuntimeError(f"Signal log table '{SIGNAL_LOG_TABLE}' does not exist.")

    df = pd.read_sql(
        f"""
        SELECT id, source_table, entry_rsi, exit_rsi, qty, product, signal_timestamp
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE signal_type = 'BUY'
          AND COALESCE(position_state, 'OPEN') = 'OPEN'
          AND source_table IS NOT NULL
          AND TRIM(source_table) <> ''
        ORDER BY source_table ASC, signal_timestamp ASC, id ASC
        """,
        conn,
    )
    if df.empty:
        return df

    df["source_table"] = df["source_table"].astype(str).str.strip().str.upper()
    if requested_symbols:
        requested = {symbol.strip().upper() for symbol in requested_symbols if symbol.strip()}
        df = df[df["source_table"].isin(requested)].reset_index(drop=True)

    if df.empty:
        return df

    df["entry_rsi"] = pd.to_numeric(df["entry_rsi"], errors="coerce")
    df["exit_rsi"] = pd.to_numeric(df["exit_rsi"], errors="coerce")
    df = df.dropna(subset=["entry_rsi", "exit_rsi"]).reset_index(drop=True)
    if not df.empty:
        df["entry_rsi"] = df["entry_rsi"].astype(int)
        df["exit_rsi"] = df["exit_rsi"].astype(int)
    return df


def load_close_history(conn: sqlite3.Connection, table_name: str, limit: int = 250) -> pd.DataFrame:
    if not table_exists(conn, table_name):
        return pd.DataFrame(columns=["trade_date", "close"])

    df = pd.read_sql(
        f"""
        SELECT trade_date, close
        FROM {quote_identifier(table_name)}
        WHERE close IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        conn,
        params=(int(limit),),
    )
    if df.empty:
        return df

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["close"] = pd.to_numeric(
        df["close"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )
    df = df.dropna(subset=["trade_date", "close"]).reset_index(drop=True)
    if not df.empty:
        df = df.sort_values(by="trade_date", ascending=True).reset_index(drop=True)
    return df


def compute_wilder_rsi(close: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
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


def compute_live_rsi(history_df: pd.DataFrame, ltp: float) -> float | None:
    if history_df.empty:
        return None

    close_series = history_df["close"].copy()
    live_close_series = pd.concat(
        [close_series, pd.Series([float(ltp)], dtype=float)],
        ignore_index=True,
    )
    live_rsi_series = compute_wilder_rsi(live_close_series, period=RSI_PERIOD)
    current_rsi = live_rsi_series.iloc[-1]
    if pd.isna(current_rsi):
        return None
    return round(float(current_rsi), 2)


def fetch_live_ltps(symbols: list[str]) -> dict[str, float]:
    if not symbols:
        return {}

    yahoo_symbols = [f"{symbol}.NS" for symbol in symbols]
    df = yf.download(
        tickers=" ".join(yahoo_symbols),
        period=LTP_HISTORY_PERIOD,
        interval=LTP_INTERVAL,
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=False,
    )

    ltps: dict[str, float] = {}
    if df.empty:
        return ltps

    if isinstance(df.columns, pd.MultiIndex):
        for symbol in symbols:
            yahoo_symbol = f"{symbol}.NS"
            if yahoo_symbol not in df.columns.get_level_values(0):
                continue
            ticker_frame = df[yahoo_symbol]
            if "Close" not in ticker_frame.columns:
                continue
            close_series = pd.to_numeric(ticker_frame["Close"], errors="coerce").dropna()
            if not close_series.empty:
                ltps[symbol] = round(float(close_series.iloc[-1]), 2)
    else:
        close_series = pd.to_numeric(df.get("Close"), errors="coerce").dropna()
        if not close_series.empty and symbols:
            ltps[symbols[0]] = round(float(close_series.iloc[-1]), 2)

    return ltps


def main() -> None:
    args = parse_args()
    requested_symbols = [symbol.strip().upper() for symbol in (args.symbols or []) if symbol.strip()]

    conn = sqlite3.connect(DB_NAME, timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        open_positions = load_open_buy_positions(conn, requested_symbols)
        if open_positions.empty:
            print("No open BUY positions found in the live signal log table.")
            return

        symbols = sorted(open_positions["source_table"].dropna().astype(str).str.upper().unique().tolist())
        ltps = fetch_live_ltps(symbols)
        rsi_cache: dict[str, tuple[float | None, float | None]] = {}
        rows: list[dict[str, object]] = []
        checked_at = datetime.now().isoformat(timespec="seconds")

        for _, position in open_positions.iterrows():
            symbol = str(position["source_table"]).strip().upper()
            position_id = int(position["id"])
            entry_rsi = int(position["entry_rsi"])
            exit_rsi = int(position["exit_rsi"])
            ltp = ltps.get(symbol)
            if ltp is None:
                rows.append(
                    {
                        "position_id": position_id,
                        "stock": symbol,
                        "entry_rsi": entry_rsi,
                        "exit_rsi": exit_rsi,
                        "current_rsi": None,
                        "gap_to_sell": None,
                        "status": "ltp unavailable",
                    }
                )
                continue

            if symbol not in rsi_cache:
                history_df = load_close_history(conn, symbol)
                rsi_cache[symbol] = (compute_live_rsi(history_df, ltp), ltp)

            current_rsi, _ = rsi_cache[symbol]
            if current_rsi is None:
                rows.append(
                    {
                        "position_id": position_id,
                        "stock": symbol,
                        "entry_rsi": entry_rsi,
                        "exit_rsi": exit_rsi,
                        "current_rsi": None,
                        "gap_to_sell": None,
                        "status": "unable to compute RSI",
                    }
                )
                continue

            gap = round(float(exit_rsi) - float(current_rsi), 2)
            rows.append(
                {
                    "position_id": position_id,
                    "stock": symbol,
                    "entry_rsi": entry_rsi,
                    "exit_rsi": exit_rsi,
                    "current_rsi": current_rsi,
                    "gap_to_sell": gap,
                    "status": "ready" if current_rsi >= exit_rsi else "pending",
                }
            )

        if not rows:
            print("No rows to report.")
            return

        report = pd.DataFrame(rows)
        report["current_rsi"] = pd.to_numeric(report["current_rsi"], errors="coerce")
        report["gap_to_sell"] = pd.to_numeric(report["gap_to_sell"], errors="coerce")
        report = report.sort_values(
            by=["gap_to_sell", "stock", "position_id"],
            ascending=[True, True, True],
            na_position="last",
        ).reset_index(drop=True)

        print(f"RSI gap check for {len(report)} open BUY positions from '{SIGNAL_LOG_TABLE}'")
        print(f"Checked at: {checked_at}")
        print("-" * 96)
        print(
            report.to_string(
                index=False,
                justify="left",
                formatters={
                    "current_rsi": lambda v: "" if pd.isna(v) else f"{float(v):.2f}",
                    "gap_to_sell": lambda v: "" if pd.isna(v) else f"{float(v):.2f}",
                },
            )
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
