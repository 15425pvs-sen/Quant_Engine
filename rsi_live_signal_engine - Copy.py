#!/usr/bin/env python
"""
Fetch live prices for stocks present in rsi_heatmap_data, compute a live RSI,
and generate BUY/SELL signals when the RSI crosses configured entry/exit levels.

Signals are logged to SQLite and duplicate calls are suppressed per
(stock, entry_rsi, exit_rsi) bucket by checking the latest signal state.

Usage:
    py rsi_live_signal_engine.py
    py rsi_live_signal_engine.py TCS RELIANCE
    py rsi_live_signal_engine.py --dry-run
    py rsi_live_signal_engine.py --check-last-rsi
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
HEATMAP_TABLE = "rsi_heatmap_data"
SIGNAL_LOG_TABLE = "rsi_live_signal_log"
RSI_PERIOD = 14
LTP_HISTORY_PERIOD = "5d"
LTP_INTERVAL = "1m"
RSI_BUFFER_ROWS = 100


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Identifier must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Identifier contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate live RSI BUY/SELL signals from heatmap thresholds."
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        help="Optional stock symbols such as TCS RELIANCE INFY",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate and print signals without inserting rows into the signal log table.",
    )
    parser.add_argument(
        "--check-last-rsi",
        action="store_true",
        help=(
            "Use the last two stored RSI values from each equity table instead of "
            "computing a live RSI from yfinance."
        ),
    )
    return parser.parse_args()


def ensure_signal_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_identifier(SIGNAL_LOG_TABLE)} (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table      TEXT NOT NULL,
            signal_type       TEXT NOT NULL,
            entry_rsi         INTEGER NOT NULL,
            exit_rsi          INTEGER NOT NULL,
            previous_rsi      REAL NOT NULL,
            current_rsi       REAL NOT NULL,
            ltp               REAL NOT NULL,
            signal_date       TEXT NOT NULL,
            signal_timestamp  TEXT NOT NULL,
            notes             TEXT
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{SIGNAL_LOG_TABLE}_lookup
        ON {quote_identifier(SIGNAL_LOG_TABLE)} (source_table, entry_rsi, exit_rsi, signal_timestamp)
        """
    )
    conn.commit()


def get_heatmap_rows(conn: sqlite3.Connection, symbols: list[str]) -> pd.DataFrame:
    query = f"""
        SELECT source_table, entry_rsi, exit_rsi, avg_return_pct, trades, win_rate_pct
        FROM {quote_identifier(HEATMAP_TABLE)}
    """
    df = pd.read_sql(query, conn)
    if df.empty:
        return df

    df["source_table"] = df["source_table"].astype(str).str.upper().str.strip()
    if symbols:
        requested = {symbol.upper() for symbol in symbols}
        df = df[df["source_table"].isin(requested)].reset_index(drop=True)

    for col in ("entry_rsi", "exit_rsi"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df = df.dropna(subset=["source_table", "entry_rsi", "exit_rsi"]).reset_index(drop=True)
    return df


def fetch_ltps(symbols: list[str]) -> dict[str, float]:
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

    if df.empty:
        return {}

    ltps: dict[str, float] = {}

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
        if close_series.empty:
            return {}
        ltps[symbols[0]] = round(float(close_series.iloc[-1]), 2)

    return ltps


def compute_wilder_rsi(close: pd.Series, period: int = 14) -> pd.Series:
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


def load_stock_history(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    history_row_limit = RSI_PERIOD + RSI_BUFFER_ROWS
    df = pd.read_sql(
        f"""
        SELECT trade_date, close, rsi
        FROM (
            SELECT trade_date, close, rsi
            FROM {quote_identifier(table_name)}
            WHERE close IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT {history_row_limit}
        )
        ORDER BY trade_date ASC
        """,
        conn,
    )

    if df.empty:
        return df

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["close"] = pd.to_numeric(
        df["close"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )
    df["rsi"] = pd.to_numeric(
        df["rsi"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )

    return df.dropna(subset=["trade_date", "close"]).reset_index(drop=True)


def load_last_rsi_snapshot(
    conn: sqlite3.Connection,
    table_name: str,
) -> tuple[float | None, float | None, float | None, str | None]:
    df = pd.read_sql(
        f"""
        SELECT trade_date, close, rsi
        FROM (
            SELECT trade_date, close, rsi
            FROM {quote_identifier(table_name)}
            WHERE close IS NOT NULL AND rsi IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 2
        )
        ORDER BY trade_date ASC
        """,
        conn,
    )

    if len(df) < 2:
        return None, None, None, None

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["close"] = pd.to_numeric(
        df["close"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )
    df["rsi"] = pd.to_numeric(
        df["rsi"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )
    df = df.dropna(subset=["trade_date", "close", "rsi"]).reset_index(drop=True)

    if len(df) < 2:
        return None, None, None, None

    previous_rsi = round(float(df.iloc[0]["rsi"]), 2)
    current_rsi = round(float(df.iloc[1]["rsi"]), 2)
    ltp = round(float(df.iloc[1]["close"]), 2)
    signal_date = pd.Timestamp(df.iloc[1]["trade_date"]).strftime("%Y-%m-%d")
    return previous_rsi, current_rsi, ltp, signal_date


def compute_live_rsi(history_df: pd.DataFrame, ltp: float) -> tuple[float | None, float | None]:
    if history_df.empty:
        return None, None

    previous_rsi_series = history_df["rsi"].dropna()
    if previous_rsi_series.empty:
        return None, None

    previous_rsi = round(float(previous_rsi_series.iloc[-1]), 2)
    close_series = history_df["close"].copy()
    live_close_series = pd.concat(
        [close_series, pd.Series([float(ltp)], dtype=float)],
        ignore_index=True,
    )
    live_rsi_series = compute_wilder_rsi(live_close_series, period=RSI_PERIOD)
    current_rsi = live_rsi_series.iloc[-1]

    if pd.isna(current_rsi):
        return previous_rsi, None

    return previous_rsi, round(float(current_rsi), 2)


def get_last_signal_type(
    conn: sqlite3.Connection,
    source_table: str,
    entry_rsi: int,
    exit_rsi: int,
) -> str | None:
    row = conn.execute(
        f"""
        SELECT signal_type
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE source_table = ? AND entry_rsi = ? AND exit_rsi = ?
        ORDER BY signal_timestamp DESC, id DESC
        LIMIT 1
        """,
        (source_table, entry_rsi, exit_rsi),
    ).fetchone()
    return row[0] if row else None


def build_signal_rows(
    conn: sqlite3.Connection,
    heatmap_df: pd.DataFrame,
    ltps: dict[str, float],
    check_last_rsi: bool = False,
) -> list[dict[str, object]]:
    signal_rows: list[dict[str, object]] = []
    runtime_timestamp = datetime.now().isoformat(timespec="seconds")
    runtime_date = datetime.now().date().isoformat()

    for source_table, stock_rules in heatmap_df.groupby("source_table"):
        if check_last_rsi:
            previous_rsi, current_rsi, ltp, signal_date = load_last_rsi_snapshot(conn, source_table)
            signal_timestamp = (
                f"{signal_date}T15:30:00" if signal_date is not None else runtime_timestamp
            )
            if previous_rsi is None or current_rsi is None or ltp is None or signal_date is None:
                print(f"Skipping {source_table}: unable to read the last two stored RSI rows.")
                continue
            print(
                f"{source_table}: mode=last_rsi, close={ltp:.2f}, previous_RSI={previous_rsi:.2f}, current_RSI={current_rsi:.2f}"
            )
        else:
            ltp = ltps.get(source_table)
            if ltp is None:
                print(f"Skipping {source_table}: live price not available.")
                continue

            history_df = load_stock_history(conn, source_table)
            previous_rsi, current_rsi = compute_live_rsi(history_df, ltp)
            signal_timestamp = runtime_timestamp
            signal_date = runtime_date
            if previous_rsi is None or current_rsi is None:
                print(f"Skipping {source_table}: unable to compute live RSI.")
                continue

            print(
                f"{source_table}: mode=live, LTP={ltp:.2f}, previous_RSI={previous_rsi:.2f}, current_RSI={current_rsi:.2f}"
            )

        # When multiple buckets share the same entry RSI, prefer the historically
        # strongest return profile first so the best BUY bucket is emitted first.
        ranked_rules = stock_rules.sort_values(
            by=["entry_rsi", "avg_return_pct", "trades", "exit_rsi"],
            ascending=[True, False, False, True],
        )

        for _, rule in ranked_rules.iterrows():
            entry_rsi = int(rule["entry_rsi"])
            exit_rsi = int(rule["exit_rsi"])
            last_signal_type = get_last_signal_type(conn, source_table, entry_rsi, exit_rsi)
            in_position = last_signal_type == "BUY"

            entry_crossed = previous_rsi < entry_rsi <= current_rsi
            exit_crossed = previous_rsi < exit_rsi <= current_rsi

            if not in_position and entry_crossed and current_rsi < exit_rsi:
                signal_rows.append(
                    {
                        "source_table": source_table,
                        "signal_type": "BUY",
                        "entry_rsi": entry_rsi,
                        "exit_rsi": exit_rsi,
                        "previous_rsi": previous_rsi,
                        "current_rsi": current_rsi,
                        "ltp": round(float(ltp), 2),
                        "signal_date": signal_date,
                        "signal_timestamp": signal_timestamp,
                        "notes": (
                            "Entry RSI crossover detected from last stored RSI rows."
                            if check_last_rsi
                            else "Entry RSI crossover detected from live price."
                        ),
                    }
                )
                print(
                    f"BUY  {source_table}  bucket=({entry_rsi},{exit_rsi})  "
                    f"RSI {previous_rsi:.2f}->{current_rsi:.2f}"
                )

            elif in_position and exit_crossed:
                signal_rows.append(
                    {
                        "source_table": source_table,
                        "signal_type": "SELL",
                        "entry_rsi": entry_rsi,
                        "exit_rsi": exit_rsi,
                        "previous_rsi": previous_rsi,
                        "current_rsi": current_rsi,
                        "ltp": round(float(ltp), 2),
                        "signal_date": signal_date,
                        "signal_timestamp": signal_timestamp,
                        "notes": (
                            "Exit RSI crossover detected from last stored RSI rows."
                            if check_last_rsi
                            else "Exit RSI crossover detected from live price."
                        ),
                    }
                )
                print(
                    f"SELL {source_table}  bucket=({entry_rsi},{exit_rsi})  "
                    f"RSI {previous_rsi:.2f}->{current_rsi:.2f}"
                )

    return signal_rows


def insert_signal_rows(conn: sqlite3.Connection, signal_rows: list[dict[str, object]]) -> int:
    if not signal_rows:
        return 0

    conn.executemany(
        f"""
        INSERT INTO {quote_identifier(SIGNAL_LOG_TABLE)} (
            source_table,
            signal_type,
            entry_rsi,
            exit_rsi,
            previous_rsi,
            current_rsi,
            ltp,
            signal_date,
            signal_timestamp,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["source_table"],
                row["signal_type"],
                row["entry_rsi"],
                row["exit_rsi"],
                row["previous_rsi"],
                row["current_rsi"],
                row["ltp"],
                row["signal_date"],
                row["signal_timestamp"],
                row["notes"],
            )
            for row in signal_rows
        ],
    )
    conn.commit()
    return len(signal_rows)


def main() -> None:
    args = parse_args()
    requested_symbols = [symbol.strip().upper() for symbol in args.symbols if symbol.strip()]

    conn = sqlite3.connect(DB_NAME)
    try:
        ensure_signal_log_table(conn)
        heatmap_df = get_heatmap_rows(conn, requested_symbols)
        if heatmap_df.empty:
            print("No heatmap rows found for the requested symbols.")
            return

        symbols = sorted(heatmap_df["source_table"].dropna().unique().tolist())
        ltps: dict[str, float] = {}
        if not args.check_last_rsi:
            ltps = fetch_ltps(symbols)
            if not ltps:
                print("No live prices could be fetched.")
                return

        signal_rows = build_signal_rows(
            conn,
            heatmap_df,
            ltps,
            check_last_rsi=args.check_last_rsi,
        )
        if not signal_rows:
            print("No BUY/SELL signals generated.")
            return

        if args.dry_run:
            print(f"Dry run complete. {len(signal_rows)} signal(s) detected but not logged.")
            return

        inserted = insert_signal_rows(conn, signal_rows)
        print(f"Inserted {inserted} signal log row(s) into {SIGNAL_LOG_TABLE}.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
