#!/usr/bin/env python
"""
Continuously track live LTP prices for stocks in rsi_heatmap_data_for_trading
and log BUY/SELL hits into rsi_live_signal_log_trading.

Usage:
    py live_rsi_tracking.py
    py live_rsi_tracking.py --interval 30
    py live_rsi_tracking.py --symbols RELIANCE TCS
    py live_rsi_tracking.py --hybrid
    py live_rsi_tracking.py --hybrid --interval 15
    py live_rsi_tracking.py --hybrid --results
    py live_rsi_tracking.py --hybrid --telegram --confirmOrder
    py live_rsi_tracking.py --hybrid --buy-rsi-protection 1.0 --telegram --confirmOrder
    py live_rsi_tracking.py --hybrid --telegram --confirmOrder --buy-rsi-protection 1.0

$env:UPSTOX_ALLOW_LIVE_ORDERS="true"
py live_rsi_tracking.py --hybrid --telegram
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import re
import threading
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import upstox_client
except ImportError:
    upstox_client = None

try:
    import yfinance as yf
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'yfinance'. Install it with: pip install yfinance"
    ) from exc

DB_NAME = Path(__file__).resolve().parent / "quant_historic_data.db"
HEATMAP_TABLE = "rsi_heatmap_data_for_trading"
SIGNAL_LOG_TABLE = "rsi_live_signal_log_trading"
ALL_SIGNAL_LOG_TABLE = "rsi_live_signal_log_trading_all_signals"
LTP_HISTORY_PERIOD = "5d"
LTP_INTERVAL = "1m"
DEFAULT_INTERVAL_SECONDS = 30
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 20
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30
SELL_AMC_CHARGE = 26.0

BUY_OPEN_STATE = "OPEN"
BUY_CLOSED_STATE = "CLOSED"
SELL_CONFIRMED_STATE = "CONFIRMED"
UPSTOX_LIVE_MODE = "ltpc"

REQUIRED_HEATMAP_COLS = {
    "source_table",
    "entry_rsi",
    "exit_rsi",
    "avg_return_pct",
    "trades",
    "win_rate_pct",
    "min_return_pct",
    "max_return_pct",
}


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Identifier must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Identifier contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously track live LTP and log trading signal hits."
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Polling interval in seconds for live LTP checks.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="Optional stock symbols to filter.",
    )
    parser.add_argument(
        "--hybrid",
        action="store_true",
        help="Enable hybrid entry: require price momentum (LTP > latest close) in addition to RSI entry.",
    )
    parser.add_argument(
        "--results",
        action="store_true",
        help="Print realized P&L from all completed BUY/SELL signals in the trading signal log.",
    )
    parser.add_argument(
        "--telegram",
        action="store_true",
        help="Send BUY/SELL alerts to Telegram if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are configured.",
    )
    parser.add_argument(
        "--telegram-bot-token",
        dest="telegram_bot_token",
        help="Telegram bot token to use for sending alerts (overrides TELEGRAM_BOT_TOKEN env var).",
    )
    parser.add_argument(
        "--telegram-chat-id",
        dest="telegram_chat_id",
        help="Telegram chat id to send alerts to (overrides TELEGRAM_CHAT_ID env var).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Do not write BUY/SELL rows to the database; print and (optionally) send alerts only for testing.",
    )
    parser.add_argument(
        "--min-profit-pct",
        dest="min_profit_pct",
        type=float,
        default=0.0,
        help="Minimum profit percent required to allow a SELL (skip tiny profits).",
    )
    parser.add_argument(
        "--buy-rsi-protection",
        dest="buy_rsi_protection",
        type=float,
        default=0.0,
        help="Minimum difference required between exit RSI and current RSI to allow a BUY (e.g. 1.0).",
    )
    parser.add_argument(
        "--confirmOrder",
        action="store_true",
        help="Prompt for Y/N before placing each Upstox order.",
    )
    parser.add_argument(
        "--upstox-live",
        action="store_true",
        default=True,
        help="Use the Upstox websocket feed for live LTPs instead of yfinance polling.",
    )
    parser.add_argument(
        "--no-upstox-live",
        action="store_false",
        dest="upstox_live",
        help="Disable the Upstox websocket feed and fall back to yfinance polling.",
    )
    return parser.parse_args()


def is_market_open_now(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    current_minutes = now.hour * 60 + now.minute
    open_minutes = MARKET_OPEN_HOUR * 60 + MARKET_OPEN_MINUTE
    close_minutes = MARKET_CLOSE_HOUR * 60 + MARKET_CLOSE_MINUTE
    return open_minutes <= current_minutes <= close_minutes


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
            qty               INTEGER,
            product           TEXT,
            signal_date       TEXT NOT NULL,
            signal_timestamp  TEXT NOT NULL,
            notes             TEXT,
            position_state    TEXT NOT NULL DEFAULT 'OPEN',
            buy_signal_id     INTEGER,
            trigger_exit_rsi  INTEGER,
            action_timestamp  TEXT,
            closed_by_signal_id INTEGER
        )
        """
    )
    existing_cols = {
        str(row[1]).lower()
        for row in conn.execute(f"PRAGMA table_info({quote_identifier(SIGNAL_LOG_TABLE)})")
    }
    if "qty" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} ADD COLUMN qty INTEGER"
        )
    if "product" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} ADD COLUMN product TEXT"
        )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_identifier(ALL_SIGNAL_LOG_TABLE)} (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table      TEXT NOT NULL,
            signal_type       TEXT NOT NULL,
            entry_rsi         INTEGER NOT NULL,
            exit_rsi          INTEGER NOT NULL,
            previous_rsi      REAL NOT NULL,
            current_rsi       REAL NOT NULL,
            ltp               REAL NOT NULL,
            qty               INTEGER,
            product           TEXT,
            signal_date       TEXT NOT NULL,
            signal_timestamp  TEXT NOT NULL,
            notes             TEXT,
            position_state    TEXT NOT NULL DEFAULT 'OPEN',
            buy_signal_id     INTEGER,
            trigger_exit_rsi  INTEGER,
            action_timestamp  TEXT,
            closed_by_signal_id INTEGER,
            order_status      TEXT,
            order_reason      TEXT
        )
        """
    )
    all_existing_cols = {
        str(row[1]).lower()
        for row in conn.execute(f"PRAGMA table_info({quote_identifier(ALL_SIGNAL_LOG_TABLE)})")
    }
    for column_name, column_type in (
        ("qty", "INTEGER"),
        ("product", "TEXT"),
        ("order_status", "TEXT"),
        ("order_reason", "TEXT"),
    ):
        if column_name not in all_existing_cols:
            conn.execute(
                f"ALTER TABLE {quote_identifier(ALL_SIGNAL_LOG_TABLE)} ADD COLUMN {column_name} {column_type}"
            )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{SIGNAL_LOG_TABLE}_lookup
        ON {quote_identifier(SIGNAL_LOG_TABLE)} (source_table, entry_rsi, exit_rsi, signal_timestamp)
        """
    )
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{SIGNAL_LOG_TABLE}_buy_unique_entry
        ON {quote_identifier(SIGNAL_LOG_TABLE)} (
            trim(upper(source_table)),
            entry_rsi
        )
        WHERE signal_type = 'BUY'
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{ALL_SIGNAL_LOG_TABLE}_lookup
        ON {quote_identifier(ALL_SIGNAL_LOG_TABLE)} (source_table, signal_type, signal_timestamp)
        """
    )
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{ALL_SIGNAL_LOG_TABLE}_unique_bucket
        ON {quote_identifier(ALL_SIGNAL_LOG_TABLE)} (
            trim(upper(source_table)),
            signal_type,
            entry_rsi,
            exit_rsi
        )
        """
    )
    conn.commit()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _extract_upstox_ltp(feed: dict[str, object]) -> float | None:
    if not isinstance(feed, dict):
        return None

    candidates: list[dict[str, object]] = []
    for key in ("ltpc",):
        value = feed.get(key)
        if isinstance(value, dict):
            candidates.append(value)

    oc = feed.get("oc")
    if isinstance(oc, dict):
        value = oc.get("ltpc")
        if isinstance(value, dict):
            candidates.append(value)

    full_feed = feed.get("fullFeed")
    if isinstance(full_feed, dict):
        market_ff = full_feed.get("marketFF")
        if isinstance(market_ff, dict):
            value = market_ff.get("ltpc")
            if isinstance(value, dict):
                candidates.append(value)

    for candidate in candidates:
        try:
            ltp = candidate.get("ltp")
            if ltp is None:
                continue
            return round(float(ltp), 2)
        except Exception:
            continue
    return None


class UpstoxLivePriceFeed:
    def __init__(self, symbols: list[str]) -> None:
        if upstox_client is None:
            raise RuntimeError(
                "Upstox SDK is not installed. Install it with: pip install upstox-python-sdk"
            )

        from upstox_order_manager import UpstoxOrderManager

        self.symbols = [symbol.upper().strip() for symbol in symbols if symbol and symbol.strip()]
        self.symbol_by_instrument: dict[str, str] = {}
        self.instrument_by_symbol: dict[str, str] = {}
        self.latest_prices: dict[str, float] = {}
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.error: str | None = None
        self.unresolved_symbols: list[str] = []
        self.streamer = None

        resolver = UpstoxOrderManager()
        for symbol in self.symbols:
            instrument = None
            for exchange in ("NSE", "BSE"):
                try:
                    instrument = resolver.resolve_instrument(symbol, exchange=exchange)
                    break
                except Exception:
                    continue
            if not instrument:
                self.unresolved_symbols.append(symbol)
                continue

            instrument_key = str(instrument.get("instrument_key") or "").strip()
            if not instrument_key:
                self.unresolved_symbols.append(symbol)
                continue

            self.symbol_by_instrument[instrument_key] = symbol
            self.instrument_by_symbol[symbol] = instrument_key

        if self.unresolved_symbols:
            print(
                "Upstox live feed could not resolve these symbols: "
                + ", ".join(self.unresolved_symbols)
            )

        if not self.instrument_by_symbol:
            self.error = "No Upstox instrument keys could be resolved for the watchlist."

    def start(self) -> None:
        if self.error:
            raise RuntimeError(self.error)

        configuration = upstox_client.Configuration()
        configuration.access_token = os.getenv("UPSTOX_ACCESS_TOKEN") or ""
        if not configuration.access_token:
            from upstox_auth import get_valid_access_token

            configuration.access_token = get_valid_access_token(force_login=False)

        instrument_keys = list(self.instrument_by_symbol.values())
        self.streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(configuration),
            instrument_keys,
            UPSTOX_LIVE_MODE,
        )
        self.streamer.on("open", self._on_open)
        self.streamer.on("message", self._on_message)
        self.streamer.on("error", self._on_error)
        self.streamer.on("close", self._on_close)
        self.streamer.connect()

    def _on_open(self) -> None:
        self.ready.set()

    def _on_message(self, message: dict[str, object]) -> None:
        feeds = message.get("feeds") if isinstance(message, dict) else None
        if not isinstance(feeds, dict):
            return

        updated = False
        with self.lock:
            for instrument_key, feed in feeds.items():
                symbol = self.symbol_by_instrument.get(str(instrument_key))
                if not symbol or not isinstance(feed, dict):
                    continue
                ltp = _extract_upstox_ltp(feed)
                if ltp is None:
                    continue
                self.latest_prices[symbol] = ltp
                updated = True
        if updated:
            self.ready.set()

    def _on_error(self, error: object) -> None:
        self.error = str(error)
        self.ready.set()

    def _on_close(self, *args: object) -> None:
        self.ready.set()

    def snapshot(self) -> dict[str, float]:
        with self.lock:
            return dict(self.latest_prices)

    def close(self) -> None:
        if self.streamer is not None:
            try:
                self.streamer.disconnect()
            except Exception:
                pass


def load_heatmap(conn: sqlite3.Connection, requested_symbols: list[str]) -> pd.DataFrame:
    if not table_exists(conn, HEATMAP_TABLE):
        raise RuntimeError(f"Heatmap table '{HEATMAP_TABLE}' does not exist.")

    df = pd.read_sql(f"SELECT * FROM {quote_identifier(HEATMAP_TABLE)}", conn)
    if df.empty:
        return df

    df["source_table"] = df["source_table"].astype(str).str.upper().str.strip()
    if requested_symbols:
        requested = {symbol.upper() for symbol in requested_symbols}
        df = df[df["source_table"].isin(requested)].reset_index(drop=True)

    for col in REQUIRED_HEATMAP_COLS:
        if col not in df.columns:
            raise RuntimeError(f"Heatmap table is missing required column: {col}")

    df["entry_rsi"] = pd.to_numeric(df["entry_rsi"], errors="coerce").astype("Int64")
    df["exit_rsi"] = pd.to_numeric(df["exit_rsi"], errors="coerce").astype("Int64")
    return df.dropna(subset=["entry_rsi", "exit_rsi"]).reset_index(drop=True)


def get_live_ltps(
    symbols: list[str],
    upstox_feed: UpstoxLivePriceFeed | None = None,
) -> dict[str, float]:
    if not symbols:
        return {}

    if upstox_feed is not None:
        cached = upstox_feed.snapshot()
        ltps: dict[str, float] = {}
        for symbol in symbols:
            ltp = cached.get(symbol)
            if ltp is None:
                continue
            ltps[symbol] = round(float(ltp), 2)
        return ltps

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
        if not close_series.empty:
            ltps[symbols[0]] = round(float(close_series.iloc[-1]), 2)

    return ltps


def load_latest_rsi(conn: sqlite3.Connection, table_name: str) -> tuple[float | None, float | None, str | None]:
    if not table_exists(conn, table_name):
        return None, None, None

    df = pd.read_sql(
        f"""
        SELECT trade_date, close, rsi
        FROM {quote_identifier(table_name)}
        WHERE close IS NOT NULL AND rsi IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        conn,
    )

    if df.empty:
        return None, None, None

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
    if df.empty:
        return None, None, None

    return (
        round(float(df.iloc[0]["rsi"]), 2),
        round(float(df.iloc[0]["close"]), 2),
        pd.Timestamp(df.iloc[0]["trade_date"]).strftime("%Y-%m-%d"),
    )


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


def get_latest_signal_bucket_exists(
    conn: sqlite3.Connection,
    source_table: str,
    signal_type: str,
    entry_rsi: int,
    exit_rsi: int,
) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE source_table = ?
          AND signal_type = ?
          AND entry_rsi = ?
          AND exit_rsi = ?
        LIMIT 1
        """,
        (source_table, signal_type, entry_rsi, exit_rsi),
    ).fetchone()
    return row is not None


def has_open_buy_bucket(
    conn: sqlite3.Connection,
    source_table: str,
    entry_rsi: int,
    exit_rsi: int,
) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE trim(upper(source_table)) = trim(upper(?))
          AND signal_type = 'BUY'
          AND entry_rsi = ?
          AND exit_rsi = ?
          AND COALESCE(position_state, '{BUY_OPEN_STATE}') = '{BUY_OPEN_STATE}'
        LIMIT 1
        """,
        (source_table, entry_rsi, exit_rsi),
    ).fetchone()
    return row is not None


def get_open_buy_rows(conn: sqlite3.Connection, source_table: str) -> list[tuple[int, int, int, float, int | None, str | None]]:
    rows = conn.execute(
        f"""
        SELECT id, entry_rsi, exit_rsi, ltp, qty, product
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE trim(upper(source_table)) = trim(upper(?))
          AND signal_type = 'BUY'
          AND COALESCE(position_state, '{BUY_OPEN_STATE}') = '{BUY_OPEN_STATE}'
        ORDER BY signal_timestamp ASC, id ASC
        """,
        (source_table,),
    ).fetchall()
    return [
        (
            int(row[0]),
            int(row[1]),
            int(row[2]),
            float(row[3]),
            None if row[4] is None else int(row[4]),
            None if row[5] is None else str(row[5]),
        )
        for row in rows
    ]


def has_buy_entry_rsi(conn: sqlite3.Connection, source_table: str, entry_rsi: int) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE trim(upper(source_table)) = trim(upper(?))
          AND signal_type = 'BUY'
          AND entry_rsi = ?
        LIMIT 1
        """,
        (source_table, entry_rsi),
    ).fetchone()
    return row is not None


def has_buy_current_rsi(conn: sqlite3.Connection, source_table: str, current_rsi: float) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE trim(upper(source_table)) = trim(upper(?))
          AND signal_type = 'BUY'
          AND current_rsi = ?
        LIMIT 1
        """,
        (source_table, current_rsi),
    ).fetchone()
    return row is not None


def close_buy_bucket(
    conn: sqlite3.Connection,
    buy_id: int,
    sell_id: int,
    signal_timestamp: str,
) -> None:
    conn.execute(
        f"""
        UPDATE {quote_identifier(SIGNAL_LOG_TABLE)}
        SET position_state = ?,
            action_timestamp = ?,
            closed_by_signal_id = ?
        WHERE id = ?
          AND signal_type = 'BUY'
          AND COALESCE(position_state, '{BUY_OPEN_STATE}') = '{BUY_OPEN_STATE}'
        """,
        (BUY_CLOSED_STATE, signal_timestamp, sell_id, buy_id),
    )
    conn.commit()


def get_buy_signal_reason_latest_rsi(current_rsi: float, entry_rsi: int, exit_rsi: int) -> str | None:
    if current_rsi < entry_rsi or current_rsi >= exit_rsi:
        return None
    return "Latest RSI hit the entry bucket for this stock."


def get_sell_signal_reason_latest_rsi(current_rsi: float, exit_rsi: int) -> str | None:
    if current_rsi < exit_rsi:
        return None
    return "Latest RSI hit the exit bucket for this stock."


def get_matching_exit_bucket(
    heatmap_df: pd.DataFrame,
    source_table: str,
    current_rsi: float,
    previous_rsi: float | None = None,
) -> tuple[int, int] | None:
    table_rules = heatmap_df[heatmap_df["source_table"] == source_table]
    if table_rules.empty:
        return None

    matches: list[tuple[int, int]] = []
    for _, rule in table_rules.iterrows():
        exit_rsi = int(rule["exit_rsi"])
        if current_rsi < exit_rsi:
            continue
        if previous_rsi is not None and previous_rsi >= exit_rsi:
            continue
        matches.append((int(rule["entry_rsi"]), exit_rsi))

    if not matches:
        return None

    return sorted(matches, key=lambda item: item[1])[0]


def get_trade_results(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
        SELECT
            source_table,
            id,
            buy_signal_id,
            ltp,
            signal_timestamp,
            qty,
            position_state
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE signal_type = 'SELL'
          AND buy_signal_id IS NOT NULL
          AND position_state IN ('CONFIRMED', 'CLOSED')
        ORDER BY source_table ASC, id ASC
        """
    sell_rows = pd.read_sql(query, conn)
    if sell_rows.empty:
        return pd.DataFrame(columns=["source_table", "trades", "entry_price", "exit_price", "pnl_pct", "pnl_abs"])

    results: list[dict[str, object]] = []
    for _, row in sell_rows.iterrows():
        buy_row = pd.read_sql(
            f"""
            SELECT ltp, signal_timestamp
            FROM {quote_identifier(SIGNAL_LOG_TABLE)}
            WHERE id = ?
              AND signal_type = 'BUY'
            """,
            conn,
            params=(int(row["buy_signal_id"]),),
        )
        if buy_row.empty:
            continue

        entry_price = float(buy_row.iloc[0]["ltp"])
        exit_price = float(row["ltp"])
        qty = int(row["qty"] or 0)
        if entry_price <= 0:
            continue

        pnl_pct = ((exit_price - entry_price) / entry_price) * 100.0
        pnl_abs = (exit_price - entry_price) * qty
        results.append(
            {
                "source_table": str(row["source_table"]).strip().upper(),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl_pct": round(float(pnl_pct), 2),
                "pnl_abs": round(float(pnl_abs), 2),
            }
        )

    if not results:
        return pd.DataFrame(columns=["source_table", "trades", "entry_price", "exit_price", "pnl_pct", "pnl_abs"])

    results_df = pd.DataFrame(results)
    summary = (
        results_df.groupby("source_table", sort=True)
        .agg(
            trades=("pnl_pct", "size"),
            entry_price=("entry_price", "mean"),
            exit_price=("exit_price", "mean"),
            pnl_pct=("pnl_pct", "mean"),
            pnl_abs=("pnl_abs", "sum"),
        )
        .reset_index()
    )
    summary = summary.sort_values(by=["pnl_pct", "source_table"], ascending=[False, True]).reset_index(drop=True)
    return summary


def print_trade_results(conn: sqlite3.Connection) -> None:
    results_df = get_trade_results(conn)
    if results_df.empty:
        print("No completed BUY/SELL trades found in the signal log.")
        return

    print("\nTrade results summary")
    print("-" * 60)
    for _, row in results_df.iterrows():
        print(
            f"{str(row['source_table']).upper():<12} trades={int(row['trades']):>2}  "
            f"P&L={float(row['pnl_abs']):>10.2f}  P&L%={float(row['pnl_pct']):>8.2f}"
        )

    overall_pnl = round(float(results_df["pnl_pct"].mean()), 2)
    overall_abs_pnl = round(float(results_df["pnl_abs"].sum()), 2)
    print("-" * 60)
    print(f"Overall P&L: {overall_abs_pnl:.2f} ({overall_pnl:.2f}%)")


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


def compute_live_rsi(history_df: pd.DataFrame, ltp: float) -> float | None:
    if history_df.empty:
        return None

    close_series = history_df["close"].copy()
    live_close_series = pd.concat(
        [close_series, pd.Series([float(ltp)], dtype=float)],
        ignore_index=True,
    )
    live_rsi_series = compute_wilder_rsi(live_close_series, period=14)
    current_rsi = live_rsi_series.iloc[-1]

    if pd.isna(current_rsi):
        return None

    return round(float(current_rsi), 2)


def send_telegram_message(message: str, bot_token: str | None = None, chat_id: str | None = None) -> bool:
    if not message.strip():
        return False

    token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
    chat = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        print(
            "Telegram not sent: missing token or chat. ",
            f"token_provided={bool(token)} chat_provided={bool(chat)}",
        )
        return False

    # Basic validation & masked debug info to help diagnose common configuration issues
    def _mask(s: str) -> str:
        if not s:
            return ""
        return s if len(s) <= 12 else f"{s[:6]}...{s[-6:]}"

    if ":" not in token:
        print(f"Telegram token looks malformed (missing ':'). token_sample={_mask(token)}")

    if not str(chat).lstrip("-").isdigit():
        print(f"Telegram chat id looks non-numeric: {chat!r}")

    masked_url = f"https://api.telegram.org/bot{_mask(token)}/sendMessage"
    # print(f"Attempting Telegram send -> url={masked_url} chat_id={_mask(str(chat))} message_len={len(message)}")

    try:
        import requests
    except ImportError:
        return False

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if not response.ok:
            try:
                text = response.text
            except Exception:
                text = "<no response body>"
            print(
                f"Telegram send failed: status={response.status_code} response={text}"
            )
        return response.ok
    except Exception as exc:
        print(f"Telegram send exception: {exc}")
        return False


def insert_signal_row(
    conn: sqlite3.Connection,
    row: dict[str, object],
    table_name: str = SIGNAL_LOG_TABLE,
    order_status: str | None = None,
    order_reason: str | None = None,
) -> int:
    if table_name == ALL_SIGNAL_LOG_TABLE:
        cursor = conn.execute(
            f"""
            INSERT OR IGNORE INTO {quote_identifier(table_name)} (
                source_table,
                signal_type,
                entry_rsi,
                exit_rsi,
                previous_rsi,
                current_rsi,
                ltp,
                qty,
                product,
                signal_date,
                signal_timestamp,
                notes,
                position_state,
                buy_signal_id,
                trigger_exit_rsi,
                action_timestamp,
                closed_by_signal_id,
                order_status,
                order_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["source_table"],
                row["signal_type"],
                row["entry_rsi"],
                row["exit_rsi"],
                row["previous_rsi"],
                row["current_rsi"],
                row["ltp"],
                row.get("qty"),
                row.get("product"),
                row["signal_date"],
                row["signal_timestamp"],
                row["notes"],
                row["position_state"],
                row["buy_signal_id"],
                row["trigger_exit_rsi"],
                row["action_timestamp"],
                row["closed_by_signal_id"],
                order_status,
                order_reason,
            ),
        )
        if cursor.rowcount == 0:
            return 0
    else:
        cursor = conn.execute(
            f"""
            INSERT INTO {quote_identifier(table_name)} (
                source_table,
                signal_type,
                entry_rsi,
                exit_rsi,
                previous_rsi,
                current_rsi,
                ltp,
                qty,
                signal_date,
                signal_timestamp,
                product,
                notes,
                position_state,
                buy_signal_id,
                trigger_exit_rsi,
                action_timestamp,
                closed_by_signal_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["source_table"],
                row["signal_type"],
                row["entry_rsi"],
                    row["exit_rsi"],
                    row["previous_rsi"],
                    row["current_rsi"],
                    row["ltp"],
                    row.get("qty"),
                    row["signal_date"],
                row["signal_timestamp"],
                row.get("product"),
                row["notes"],
                row["position_state"],
                row["buy_signal_id"],
                row["trigger_exit_rsi"],
                row["action_timestamp"],
                row["closed_by_signal_id"],
            ),
        )
    conn.commit()
    return int(cursor.lastrowid)


def trigger_order_execution(
    side: str,
    symbol: str,
    ltp: float,
    confirm_order: bool = False,
    signal_id: int | None = None,
) -> dict[str, object]:
    script_path = Path(__file__).resolve().parent / "upstox_order_manager.py"
    if not script_path.exists():
        print(f"Skipping order execution: {script_path.name} not found.")
        return {"success": False, "reason": "order manager not found"}

    if os.getenv("UPSTOX_ALLOW_LIVE_ORDERS", "false").lower() != "true":
        print(
            f"Skipping order execution for {side} {symbol}: "
            "UPSTOX_ALLOW_LIVE_ORDERS is not true."
        )
        return {"success": False, "reason": "live orders disabled"}

    if side.upper() == "BUY":
        try:
            from upstox_order_manager import UpstoxOrderManager

            funds_manager = UpstoxOrderManager()
            available_funds = funds_manager.get_available_margin()
            print(f"Available Upstox funds: {available_funds:.2f}")
            if available_funds <= 0:
                print("No available funds in Upstox")
                return {"success": False, "reason": "funds lower than per trade value"}
        except Exception as exc:
            print(f"Unable to read Upstox funds before BUY {symbol}: {exc}")
            return {"success": False, "reason": str(exc)}
    elif signal_id is None:
        print(f"Skipping order execution for SELL {symbol}: signal id is required.")
        return {"success": False, "reason": "missing signal id"}

    try:
        cmd = [
            sys.executable,
            str(script_path),
            "--side",
            side,
            "--symbol",
            symbol,
            "--ltp",
            str(float(ltp)),
            "--db-path",
            str(DB_NAME),
            "--live",
        ]
        if side.upper() == "SELL" and signal_id is not None:
            cmd.extend(["--signal-id", str(int(signal_id))])
        completed = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
        )
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        if stdout_text:
            print(stdout_text, end="" if stdout_text.endswith("\n") else "\n")
        if stderr_text:
            print(stderr_text, end="" if stderr_text.endswith("\n") else "\n")

        if completed.returncode != 0:
            print(
                f"Upstox order manager failed for {side} {symbol} "
                f"with exit code {completed.returncode}."
            )
            return {
                "success": False,
                "exit_code": completed.returncode,
                "stdout": stdout_text,
                "stderr": stderr_text,
            }

        qty = None
        product = None
        available_funds = None
        order_value = None
        order_id = None
        for line in stdout_text.splitlines():
            line = line.strip()
            m = re.search(r"Calculated qty:\s*(\d+)", line)
            if m:
                qty = int(m.group(1))
            m = re.search(r"Resolved SELL qty from BUY signal_id=.*:\s*(\d+)", line)
            if m:
                qty = int(m.group(1))
            m = re.search(r"Resolved SELL product:\s*([A-Z]+)", line)
            if m:
                product = m.group(1)
            m = re.search(r"Available Upstox funds:\s*([0-9]+(?:\.[0-9]+)?)", line)
            if m:
                available_funds = float(m.group(1))
            m = re.search(r"Proposed value:\s*([0-9]+(?:\.[0-9]+)?)", line)
            if m:
                order_value = float(m.group(1))
            m = re.search(r"SELL order value:\s*([0-9]+(?:\.[0-9]+)?)", line)
            if m:
                order_value = float(m.group(1))
            m = re.search(r"order_id['\"]?\s*[:=]\s*['\"]?([A-Za-z0-9_-]+)", line)
            if m:
                order_id = m.group(1)
            if order_id is None:
                m = re.search(r"order_ids['\"]?\s*[:=]\s*\[\s*['\"]?([A-Za-z0-9_-]+)", line)
                if m:
                    order_id = m.group(1)

        if order_id is None:
            m = re.search(r"'order_ids'\s*:\s*\[\s*'([^']+)'\s*\]", stdout_text)
            if m:
                order_id = m.group(1)
            else:
                m = re.search(r'"order_ids"\s*:\s*\[\s*"([^"]+)"\s*\]', stdout_text)
                if m:
                    order_id = m.group(1)
        if order_id is None:
            for candidate in re.findall(r"order_ids['\"]?\s*[:=]\s*\[[^\]]+\]", stdout_text):
                m = re.search(r"([A-Za-z0-9_-]{8,})", candidate)
                if m:
                    order_id = m.group(1)
                    break

        if order_id is None:
            return {
                "success": False,
                "reason": "Upstox order was not placed",
                "exit_code": completed.returncode,
                "stdout": stdout_text,
                "stderr": stderr_text,
                "qty": qty,
                "product": product,
                "available_funds": available_funds,
                "order_value": order_value,
                "order_id": None,
            }

        return {
            "success": True,
            "exit_code": completed.returncode,
            "stdout": stdout_text,
            "stderr": stderr_text,
            "qty": qty,
            "product": product,
            "available_funds": available_funds,
            "order_value": order_value,
            "order_id": order_id,
        }
    except Exception as exc:
        print(f"Failed to launch upstox_order_manager.py for {side} {symbol}: {exc}")
        return {"success": False, "reason": str(exc)}


def _failure_reason_from_order_result(order_result: dict[str, object]) -> str:
    reason = str(order_result.get("reason") or "").strip()
    if reason:
        return reason

    stdout_text = str(order_result.get("stdout") or "").strip()
    stderr_text = str(order_result.get("stderr") or "").strip()
    combined = "\n".join(part for part in (stdout_text, stderr_text) if part)
    if not combined:
        return "order was not placed on Upstox"

    explicit_markers = (
        "Funds are lower than Per trade value",
        "Insufficient Upstox funds",
        "Unable to read Upstox funds",
        "SELL orders require --signal-id",
        "Linked BUY signal row",
        "Insufficient live Upstox inventory",
        "Upstox order manager failed",
        "ValueError:",
        "Upstox API error",
        "Network error while calling Upstox",
    )
    for marker in explicit_markers:
        if marker in combined:
            return marker

    for line in combined.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^(BUY|SELL)\s+.+?\s+skipped:\s*(.+?)\.?$", line)
        if m:
            return f"{m.group(1)} skipped: {m.group(2)}"

    lines = [line.strip() for line in combined.splitlines() if line.strip()]
    if lines:
        return lines[-1]
    return "order was not placed on Upstox"


def _prompt_order_confirmation(message: str) -> bool:
    try:
        while True:
            print(message, end="", flush=True)
            response = input().strip().upper()
            if response in {"Y", "N"}:
                return response == "Y"
            print("Please type Y or N.")
    except EOFError:
        print("Skipping order execution: confirmation input unavailable.")
        return False


def _print_live_signal_context(source_table: str, current_rsi: float, latest_close: float | None, ltp: float) -> None:
    if latest_close is None:
        print(f"Live RSI {source_table}: {current_rsi:.2f} | LTP: {ltp:.2f}")
    else:
        print(
            f"Live RSI {source_table}: {current_rsi:.2f} | "
            f"Latest stored close: {latest_close:.2f} | LTP: {ltp:.2f}"
        )


def _insert_all_signal_buy_skip(
    conn: sqlite3.Connection,
    source_table: str,
    entry_rsi: int,
    exit_rsi: int,
    current_rsi: float,
    ltp: float,
    signal_date: str,
    signal_timestamp: str,
    note_text: str,
    buy_reason: str,
    order_reason: str,
) -> int:
    return insert_signal_row(
        conn,
        {
            "source_table": source_table,
            "signal_type": "BUY",
            "entry_rsi": entry_rsi,
            "exit_rsi": exit_rsi,
            "previous_rsi": current_rsi,
            "current_rsi": current_rsi,
            "ltp": ltp,
            "qty": None,
            "product": None,
            "signal_date": signal_date,
            "signal_timestamp": signal_timestamp,
            "notes": f"{note_text} | {buy_reason}",
            "position_state": BUY_OPEN_STATE,
            "buy_signal_id": None,
            "trigger_exit_rsi": None,
            "action_timestamp": None,
            "closed_by_signal_id": None,
        },
        table_name=ALL_SIGNAL_LOG_TABLE,
        order_status="SKIPPED",
        order_reason=order_reason,
    )


def _send_telegram_if_new_all_signal(
    inserted_row_id: int,
    message: str,
    send_to_telegram: bool,
    telegram_bot_token: str | None,
    telegram_chat_id: str | None,
) -> None:
    if inserted_row_id <= 0 or not send_to_telegram:
        return
    ok = send_telegram_message(message, bot_token=telegram_bot_token, chat_id=telegram_chat_id)
    if not ok:
        print("Telegram alert not delivered for signal.")


def handle_source_table(
    conn: sqlite3.Connection,
    source_table: str,
    heatmap_df: pd.DataFrame,
    current_rsi: float,
    ltp: float,
    signal_date: str,
    signal_timestamp: str,
    latest_close: float | None = None,
    use_hybrid: bool = False,
    previous_rsi: float | None = None,
    send_to_telegram: bool = False,
    dry_run: bool = False,
    telegram_bot_token: str | None = None,
    telegram_chat_id: str | None = None,
    min_profit_pct: float = 0.0,
    buy_rsi_protection: float = 0.0,
    confirm_order: bool = False,
 ) -> None:
    table_rules = heatmap_df[heatmap_df["source_table"] == source_table]
    if table_rules.empty:
        return

    ranked_rules = table_rules.sort_values(
        by=["exit_rsi", "entry_rsi"],
        ascending=[True, True],
    )

    # First attempt to close any open BUY buckets with matching exit conditions.
    for buy_id, buy_entry_rsi, buy_exit_rsi, buy_ltp, buy_qty, buy_product in get_open_buy_rows(conn, source_table):
        if buy_qty is None or int(buy_qty) <= 0:
            continue
        sell_reason = get_sell_signal_reason_latest_rsi(current_rsi, buy_exit_rsi)
        alternative_bucket = None
        if not sell_reason:
            alternative_bucket = get_matching_exit_bucket(
                heatmap_df,
                source_table,
                current_rsi,
                previous_rsi=previous_rsi,
            )
            if alternative_bucket is not None:
                alternative_entry_rsi, alternative_exit_rsi = alternative_bucket
                if alternative_exit_rsi != buy_exit_rsi:
                    sell_reason = (
                        f"Current RSI crossed alternate exit bucket ({alternative_entry_rsi},{alternative_exit_rsi})."
                    )
                    buy_entry_rsi = alternative_entry_rsi
                    buy_exit_rsi = alternative_exit_rsi

        if not sell_reason:
            continue

        if ltp <= buy_ltp:
            continue

        # Require minimum profit percentage before creating a SELL to avoid tiny P&L trades
        try:
            profit_pct = ((float(ltp) - float(buy_ltp)) / float(buy_ltp)) * 100.0
        except Exception:
            profit_pct = 0.0

        if profit_pct <= float(min_profit_pct):
            print(
                f"Skipping SELL {source_table} for buy_id={buy_id}: profit={profit_pct:.4f}% <= min_profit_pct={min_profit_pct}"
            )
            continue

        qty = int(buy_qty)
        pnl = round((float(ltp) - float(buy_ltp)) * float(qty), 2)
        pnl_pct = round(((float(ltp) - float(buy_ltp)) / float(buy_ltp)) * 100.0, 4) if float(buy_ltp) else 0.0
        if pnl <= SELL_AMC_CHARGE:
            print(
                f"Skipping SELL {source_table} for buy_id={buy_id}: "
                f"PnL={pnl:.2f} <= AMC charge {SELL_AMC_CHARGE:.2f}"
            )
            continue
        _print_live_signal_context(source_table, current_rsi, latest_close, ltp)
        message = (
            f"SELL {source_table} | closed_buy_id={buy_id} | bucket=({buy_entry_rsi},{buy_exit_rsi}) | "
            f"BUY PRICE {buy_ltp:.2f} | SELL LTP {ltp:.2f} | Qty {qty} | PnL {pnl:.2f} ({pnl_pct:.4f}%) | "
            f"RSI {current_rsi:.2f}"
        )
        print(message)
        if confirm_order:
            prompt = (
                f"Confirm SELL {source_table} | qty={qty} | BUY LTP={buy_ltp:.2f} | "
                f"SELL LTP={ltp:.2f} | PnL={pnl:.2f} ? [Y/N]: "
            )
            if not _prompt_order_confirmation(prompt):
                fail_reason = "user declined order confirmation"
                print(f"SELL {source_table} skipped: {fail_reason}.")
                inserted_id = insert_signal_row(
                    conn,
                    {
                        "source_table": source_table,
                        "signal_type": "SELL",
                        "entry_rsi": buy_entry_rsi,
                        "exit_rsi": buy_exit_rsi,
                        "previous_rsi": current_rsi,
                        "current_rsi": current_rsi,
                        "ltp": ltp,
                        "qty": qty,
                        "product": buy_product,
                        "signal_date": signal_date,
                        "signal_timestamp": signal_timestamp,
                        "notes": f"SKIPPED | {fail_reason} | BUY_PRICE={buy_ltp:.2f} | PnL={pnl:.2f}",
                        "position_state": SELL_CONFIRMED_STATE,
                        "buy_signal_id": buy_id,
                        "trigger_exit_rsi": buy_exit_rsi,
                        "action_timestamp": signal_timestamp,
                        "closed_by_signal_id": None,
                    },
                    table_name=ALL_SIGNAL_LOG_TABLE,
                    order_status="SKIPPED",
                    order_reason=fail_reason,
                )
                _send_telegram_if_new_all_signal(
                    inserted_id,
                    (
                        f"SELL {source_table} skipped | bucket=({buy_entry_rsi},{buy_exit_rsi}) | "
                        f"RSI {current_rsi:.2f} | LTP {ltp:.2f}"
                    ),
                    send_to_telegram,
                    telegram_bot_token,
                    telegram_chat_id,
                )
                return
        if dry_run:
            inserted_id = insert_signal_row(
                conn,
                {
                    "source_table": source_table,
                    "signal_type": "SELL",
                    "entry_rsi": buy_entry_rsi,
                    "exit_rsi": buy_exit_rsi,
                    "previous_rsi": current_rsi,
                    "current_rsi": current_rsi,
                    "ltp": ltp,
                    "qty": qty,
                    "product": buy_product,
                    "signal_date": signal_date,
                    "signal_timestamp": signal_timestamp,
                    "notes": f"DRY_RUN | {sell_reason} | BUY_PRICE={buy_ltp:.2f} | PnL={pnl:.2f}",
                    "position_state": SELL_CONFIRMED_STATE,
                    "buy_signal_id": buy_id,
                    "trigger_exit_rsi": buy_exit_rsi,
                    "action_timestamp": signal_timestamp,
                    "closed_by_signal_id": None,
                },
                table_name=ALL_SIGNAL_LOG_TABLE,
                order_status="DRY_RUN",
                order_reason="dry run",
            )
            _send_telegram_if_new_all_signal(
                inserted_id,
                message,
                send_to_telegram,
                telegram_bot_token,
                telegram_chat_id,
            )
            return

        order_result = trigger_order_execution(
            "SELL",
            source_table,
            ltp,
            confirm_order=False,
            signal_id=buy_id,
        )
        if not order_result.get("success"):
            fail_reason = _failure_reason_from_order_result(order_result)
            fail_message = f"SELL {source_table} skipped: {fail_reason}."
            print(fail_message)
            inserted_id = insert_signal_row(
                conn,
                {
                    "source_table": source_table,
                    "signal_type": "SELL",
                    "entry_rsi": buy_entry_rsi,
                    "exit_rsi": buy_exit_rsi,
                    "previous_rsi": current_rsi,
                    "current_rsi": current_rsi,
                    "ltp": ltp,
                    "qty": qty,
                    "product": buy_product,
                    "signal_date": signal_date,
                    "signal_timestamp": signal_timestamp,
                    "notes": f"SKIPPED | {fail_reason} | BUY_PRICE={buy_ltp:.2f} | PnL={pnl:.2f}",
                    "position_state": SELL_CONFIRMED_STATE,
                    "buy_signal_id": buy_id,
                    "trigger_exit_rsi": buy_exit_rsi,
                    "action_timestamp": signal_timestamp,
                    "closed_by_signal_id": None,
                },
                table_name=ALL_SIGNAL_LOG_TABLE,
                order_status="SKIPPED",
                order_reason=fail_reason,
            )
            _send_telegram_if_new_all_signal(
                inserted_id,
                (
                    f"SELL {source_table} skipped | bucket=({buy_entry_rsi},{buy_exit_rsi}) | "
                    f"RSI {current_rsi:.2f} | LTP {ltp:.2f}"
                ),
                send_to_telegram,
                telegram_bot_token,
                telegram_chat_id,
            )
            return
        if not str(order_result.get("order_id") or "").strip():
            print(
                f"SELL {source_table} skipped: Upstox did not return an order id."
            )
            return

        try:
            from upstox_order_manager import update_signal_execution_details

            resolved_qty = int(order_result.get("qty") or qty)
            resolved_product = str(order_result.get("product") or buy_product or "").strip() or None
            if update_signal_execution_details(
                buy_id,
                resolved_qty,
                resolved_product,
                db_path=DB_PATH,
            ):
                print(
                    f"Updated {SIGNAL_LOG_TABLE} for signal_id={buy_id} "
                    f"with qty={resolved_qty}."
                )
        except Exception as exc:
            print(
                f"Warning: failed to update execution details for signal_id={buy_id}: {exc}"
            )

        sell_id = insert_signal_row(
            conn,
            {
                "source_table": source_table,
                "signal_type": "SELL",
                "entry_rsi": buy_entry_rsi,
                "exit_rsi": buy_exit_rsi,
                "previous_rsi": current_rsi,
                "current_rsi": current_rsi,
                "ltp": ltp,
                "qty": qty,
                "product": buy_product,
                "signal_date": signal_date,
                "signal_timestamp": signal_timestamp,
                "notes": f"{sell_reason} | BUY_PRICE={buy_ltp:.2f} | PnL={pnl:.2f}",
                "position_state": SELL_CONFIRMED_STATE,
                "buy_signal_id": buy_id,
                "trigger_exit_rsi": buy_exit_rsi,
                "action_timestamp": signal_timestamp,
                "closed_by_signal_id": None,
            },
        )
        inserted_id = insert_signal_row(
            conn,
            {
                "source_table": source_table,
                "signal_type": "SELL",
                "entry_rsi": buy_entry_rsi,
                "exit_rsi": buy_exit_rsi,
                "previous_rsi": current_rsi,
                "current_rsi": current_rsi,
                "ltp": ltp,
                "qty": qty,
                "product": buy_product,
                "signal_date": signal_date,
                "signal_timestamp": signal_timestamp,
                "notes": f"{sell_reason} | BUY_PRICE={buy_ltp:.2f} | PnL={pnl:.2f}",
                "position_state": SELL_CONFIRMED_STATE,
                "buy_signal_id": buy_id,
                "trigger_exit_rsi": buy_exit_rsi,
                "action_timestamp": signal_timestamp,
                "closed_by_signal_id": None,
            },
            table_name=ALL_SIGNAL_LOG_TABLE,
            order_status="PLACED",
            order_reason=None,
        )
        _send_telegram_if_new_all_signal(
            inserted_id,
            message,
            send_to_telegram,
            telegram_bot_token,
            telegram_chat_id,
        )
        close_buy_bucket(conn, buy_id, sell_id, signal_timestamp)
        return

    # If no SELL was generated, allow new BUY signals only when no open BUY exists for that bucket
    # and when this entry RSI has not already produced a BUY.
    for _, rule in ranked_rules.iterrows():
        entry_rsi = int(rule["entry_rsi"])
        exit_rsi = int(rule["exit_rsi"])
        buy_reason = get_buy_signal_reason_latest_rsi(current_rsi, entry_rsi, exit_rsi)
        if not buy_reason:
            continue

        if has_open_buy_bucket(conn, source_table, entry_rsi, exit_rsi):
            continue

        if has_buy_entry_rsi(conn, source_table, entry_rsi):
            continue

        if has_buy_current_rsi(conn, source_table, current_rsi):
            continue

        # Hybrid mode: require simple price momentum confirmation (live LTP > latest stored close)
        if use_hybrid and latest_close is not None:
            try:
                if float(ltp) <= float(latest_close):
                    # no momentum, skip this BUY
                    continue
            except Exception:
                # if conversion fails, skip hybrid check and allow usual RSI-only behavior
                pass

        # BUY RSI protection: ensure current RSI is sufficiently below exit_rsi
        try:
            rsi_gap = float(exit_rsi) - float(current_rsi)
        except Exception:
            rsi_gap = 0.0

        if float(rsi_gap) < float(buy_rsi_protection):
            print(
                f"Skipping BUY {source_table}: exit_rsi={exit_rsi} current_rsi={current_rsi:.2f} gap={rsi_gap:.2f} < buy_rsi_protection={buy_rsi_protection}"
            )
            continue

        message = (
            f"BUY {source_table} | bucket=({entry_rsi},{exit_rsi}) | RSI {current_rsi:.2f} | LTP {ltp:.2f}"
        )
        _print_live_signal_context(source_table, current_rsi, latest_close, ltp)
        print(message)
        if confirm_order:
            try:
                from upstox_order_manager import (
                    DAILY_LIMIT,
                    add_today_reject,
                    UpstoxOrderManager,
                    is_today_rejected,
                    validate_trade_amount,
                )

                if is_today_rejected(source_table):
                    fail_reason = "symbol declined earlier today"
                    print(f"BUY {source_table} skipped: {fail_reason}.")
                    inserted_id = _insert_all_signal_buy_skip(
                        conn,
                        source_table,
                        entry_rsi,
                        exit_rsi,
                        current_rsi,
                        ltp,
                        signal_date,
                        signal_timestamp,
                        "SKIPPED | symbol declined earlier today",
                        buy_reason,
                        fail_reason,
                    )
                    _send_telegram_if_new_all_signal(
                        inserted_id,
                        (
                            f"BUY {source_table} skipped | bucket=({entry_rsi},{exit_rsi}) | "
                            f"RSI {current_rsi:.2f} | LTP {ltp:.2f}"
                        ),
                        send_to_telegram,
                        telegram_bot_token,
                        telegram_chat_id,
                    )
                    return

                available_funds = UpstoxOrderManager().get_available_margin()
                prompt_qty, prompt_order_value, used_today = validate_trade_amount(
                    source_table,
                    ltp,
                    available_margin=available_funds,
                )
                prompt = (
                    f"Confirm BUY {source_table} | qty={prompt_qty} | order_value={prompt_order_value:.2f} | "
                    f"available_funds={available_funds:.2f} | "
                    f"remaining_daily_limit={max(0.0, DAILY_LIMIT - used_today):.2f} ? [Y/N]: "
                )
                if not _prompt_order_confirmation(prompt):
                    fail_reason = "user declined order confirmation"
                    print(f"BUY {source_table} skipped: {fail_reason}.")
                    inserted_id = _insert_all_signal_buy_skip(
                        conn,
                        source_table,
                        entry_rsi,
                        exit_rsi,
                        current_rsi,
                        ltp,
                        signal_date,
                        signal_timestamp,
                        "SKIPPED | user declined order confirmation",
                        buy_reason,
                        fail_reason,
                    )
                    try:
                        add_today_reject(source_table)
                    except Exception as reject_exc:
                        print(f"Warning: unable to add {source_table} to reject list: {reject_exc}")
                    _send_telegram_if_new_all_signal(
                        inserted_id,
                        (
                            f"BUY {source_table} skipped | bucket=({entry_rsi},{exit_rsi}) | "
                            f"RSI {current_rsi:.2f} | LTP {ltp:.2f}"
                        ),
                        send_to_telegram,
                        telegram_bot_token,
                        telegram_chat_id,
                    )
                    return
            except Exception as exc:
                reason_text = str(exc)
                print(f"Unable to prepare BUY confirmation for {source_table}: {exc}")
                inserted_id = _insert_all_signal_buy_skip(
                    conn,
                    source_table,
                    entry_rsi,
                    exit_rsi,
                    current_rsi,
                    ltp,
                    signal_date,
                    signal_timestamp,
                    f"SKIPPED | {reason_text}",
                    buy_reason,
                    reason_text,
                )
                _send_telegram_if_new_all_signal(
                    inserted_id,
                    (
                        f"BUY {source_table} skipped | bucket=({entry_rsi},{exit_rsi}) | "
                        f"RSI {current_rsi:.2f} | LTP {ltp:.2f}"
                    ),
                    send_to_telegram,
                    telegram_bot_token,
                    telegram_chat_id,
                )
                return

        if dry_run:
            inserted_id = insert_signal_row(
                conn,
                {
                    "source_table": source_table,
                    "signal_type": "BUY",
                    "entry_rsi": entry_rsi,
                    "exit_rsi": exit_rsi,
                    "previous_rsi": current_rsi,
                    "current_rsi": current_rsi,
                    "ltp": ltp,
                    "qty": None,
                    "product": None,
                    "signal_date": signal_date,
                    "signal_timestamp": signal_timestamp,
                    "notes": f"DRY_RUN | {buy_reason}",
                    "position_state": BUY_OPEN_STATE,
                    "buy_signal_id": None,
                    "trigger_exit_rsi": None,
                    "action_timestamp": None,
                    "closed_by_signal_id": None,
                },
                table_name=ALL_SIGNAL_LOG_TABLE,
                order_status="DRY_RUN",
                order_reason="dry run",
            )
            _send_telegram_if_new_all_signal(
                inserted_id,
                message,
                send_to_telegram,
                telegram_bot_token,
                telegram_chat_id,
            )
            return

        order_result = trigger_order_execution("BUY", source_table, ltp, confirm_order=False)
        if not order_result.get("success"):
            fail_reason = _failure_reason_from_order_result(order_result)
            fail_message = f"BUY {source_table} skipped: {fail_reason}."
            print(fail_message)
            inserted_id = insert_signal_row(
                conn,
                {
                    "source_table": source_table,
                    "signal_type": "BUY",
                    "entry_rsi": entry_rsi,
                    "exit_rsi": exit_rsi,
                    "previous_rsi": current_rsi,
                    "current_rsi": current_rsi,
                    "ltp": ltp,
                    "qty": None,
                    "product": None,
                    "signal_date": signal_date,
                    "signal_timestamp": signal_timestamp,
                    "notes": f"SKIPPED | {fail_reason} | {buy_reason}",
                    "position_state": BUY_OPEN_STATE,
                    "buy_signal_id": None,
                    "trigger_exit_rsi": None,
                    "action_timestamp": None,
                    "closed_by_signal_id": None,
                },
                table_name=ALL_SIGNAL_LOG_TABLE,
                order_status="SKIPPED",
                order_reason=fail_reason,
            )
            _send_telegram_if_new_all_signal(
                inserted_id,
                (
                    f"BUY {source_table} skipped | bucket=({entry_rsi},{exit_rsi}) | "
                    f"RSI {current_rsi:.2f} | LTP {ltp:.2f}"
                ),
                send_to_telegram,
                telegram_bot_token,
                telegram_chat_id,
            )
            return

        buy_qty = order_result.get("qty")
        if buy_qty is None:
            try:
                from upstox_order_manager import PER_TRADE_VALUE as UPS_PCT_PER_TRADE_VALUE
            except Exception:
                UPS_PCT_PER_TRADE_VALUE = 0.0
            buy_qty = max(1, int(float(UPS_PCT_PER_TRADE_VALUE) // float(ltp))) if float(UPS_PCT_PER_TRADE_VALUE) > 0 else 1
        buy_product = order_result.get("product") or "D"
        buy_id = insert_signal_row(
            conn,
            {
                "source_table": source_table,
                "signal_type": "BUY",
                "entry_rsi": entry_rsi,
                "exit_rsi": exit_rsi,
                "previous_rsi": current_rsi,
                "current_rsi": current_rsi,
                "ltp": ltp,
                "qty": int(buy_qty),
                "product": str(buy_product),
                "signal_date": signal_date,
                "signal_timestamp": signal_timestamp,
                "notes": buy_reason,
                "position_state": BUY_OPEN_STATE,
                "buy_signal_id": None,
                "trigger_exit_rsi": None,
                "action_timestamp": None,
                "closed_by_signal_id": None,
            },
        )
        inserted_id = insert_signal_row(
            conn,
            {
                "source_table": source_table,
                "signal_type": "BUY",
                "entry_rsi": entry_rsi,
                "exit_rsi": exit_rsi,
                "previous_rsi": current_rsi,
                "current_rsi": current_rsi,
                "ltp": ltp,
                "qty": int(buy_qty),
                "product": str(buy_product),
                "signal_date": signal_date,
                "signal_timestamp": signal_timestamp,
                "notes": buy_reason,
                "position_state": BUY_OPEN_STATE,
                "buy_signal_id": None,
                "trigger_exit_rsi": None,
                "action_timestamp": None,
                "closed_by_signal_id": None,
            },
            table_name=ALL_SIGNAL_LOG_TABLE,
            order_status="PLACED",
            order_reason=None,
        )
        _send_telegram_if_new_all_signal(
            inserted_id,
            message,
            send_to_telegram,
            telegram_bot_token,
            telegram_chat_id,
        )
        return


def main() -> None:
    args = parse_args()
    def _mask_val(v: str | None) -> str:
        if not v:
            return ""
        s = str(v)
        return s if len(s) <= 12 else f"{s[:6]}...{s[-6:]}"

    # Print startup summary of active protections and important flags
    try:
        symbols_display = ", ".join(args.symbols) if getattr(args, "symbols", None) else "ALL"
    except Exception:
        symbols_display = "ALL"

    protections: list[str] = []
    if getattr(args, "dry_run", False):
        protections.append("dry_run")
    min_p = float(getattr(args, "min_profit_pct", 0.0) or 0.0)
    if min_p > 0:
        protections.append(f"min_profit_pct={min_p}")
    buy_prot = float(getattr(args, "buy_rsi_protection", 0.0) or 0.0)
    if buy_prot > 0:
        protections.append(f"buy_rsi_protection={buy_prot}")
    if getattr(args, "hybrid", False):
        protections.append("hybrid")

    bot_sample = _mask_val(getattr(args, "telegram_bot_token", None) or os.getenv("TELEGRAM_BOT_TOKEN"))
    chat_sample = _mask_val(getattr(args, "telegram_chat_id", None) or os.getenv("TELEGRAM_CHAT_ID"))

    print("\nStartup configuration summary")
    print("-" * 48)
    print(f"Interval: {getattr(args, 'interval', DEFAULT_INTERVAL_SECONDS)}s   Symbols: {symbols_display}")
    print(f"Confirm order: {getattr(args, 'confirmOrder', False)}")
    print(f"Telegram: {'ENABLED' if getattr(args, 'telegram', False) else 'DISABLED'}  token_sample={bot_sample} chat_sample={chat_sample}")
    print("BUY sizing: PER_TRADE_VALUE is the maximum order cap, with DAILY_LIMIT and available funds checks.")
    print(f"Active protections: {', '.join(protections) if protections else 'None'}")
    print("-" * 48 + "\n")
    interval_seconds = max(5, args.interval)
    requested_symbols = [symbol.strip().upper() for symbol in (args.symbols or []) if symbol.strip()]
    live_feed: UpstoxLivePriceFeed | None = None
    active_upstox_symbols: set[str] = set()

    if args.upstox_live and upstox_client is None:
        raise RuntimeError(
            "The Upstox SDK is required for --upstox-live. Install it with: pip install upstox-python-sdk"
        )

    if args.upstox_live:
        try:
            from upstox_order_manager import UpstoxOrderManager

            funds_manager = UpstoxOrderManager()
            available_funds = funds_manager.get_available_margin()
            print(f"Available Upstox funds: {available_funds:.2f}")
            print("-" * 48 + "\n")
        except Exception as exc:
            print(f"Unable to read Upstox available funds at startup: {exc}")
            print("-" * 48 + "\n")

    conn = sqlite3.connect(DB_NAME, timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        ensure_signal_log_table(conn)
        if args.results:
            if not table_exists(conn, SIGNAL_LOG_TABLE):
                raise RuntimeError(f"Signal log table '{SIGNAL_LOG_TABLE}' does not exist.")
            print_trade_results(conn)
            return

        if not table_exists(conn, HEATMAP_TABLE):
            raise RuntimeError(f"Heatmap table '{HEATMAP_TABLE}' must exist before tracking.")

        while True:
            heatmap_df = load_heatmap(conn, requested_symbols)
            if heatmap_df.empty:
                print("No heatmap rows found.")
                time.sleep(interval_seconds)
                continue

            symbols = sorted(heatmap_df["source_table"].dropna().unique().tolist())
            if args.upstox_live:
                current_upstox_symbols = set(symbols)
                if live_feed is None or current_upstox_symbols != active_upstox_symbols:
                    if live_feed is not None:
                        live_feed.close()
                    live_feed = UpstoxLivePriceFeed(symbols)
                    live_feed.start()
                    if live_feed.error:
                        raise RuntimeError(live_feed.error)
                    if not live_feed.ready.wait(timeout=15):
                        raise RuntimeError(
                            "Timed out waiting for the Upstox market-data websocket."
                        )
                    active_upstox_symbols = current_upstox_symbols
                    print(
                        f"Upstox websocket connected for {len(active_upstox_symbols)} symbols."
                    )
                    time.sleep(2)
                    continue

            ltps = get_live_ltps(symbols, live_feed if args.upstox_live else None)
            if not ltps:
                print("Unable to fetch live prices. Retrying.")
                time.sleep(interval_seconds)
                continue

            if getattr(args, "dry_run", False):
                snapshot = ", ".join(
                    f"{symbol}={ltp:.2f}"
                    for symbol, ltp in sorted(ltps.items())
                )
                print(f"Live price snapshot: {snapshot}")

            if not is_market_open_now():
                now = datetime.now()
                print(
                    f"Outside market window ({MARKET_OPEN_HOUR:02d}:{MARKET_OPEN_MINUTE:02d} "
                    f"to {MARKET_CLOSE_HOUR:02d}:{MARKET_CLOSE_MINUTE:02d}). "
                    f"Skipping signal generation at {now.strftime('%H:%M:%S')}."
                )
                time.sleep(interval_seconds)
                continue

            now = datetime.now()
            signal_date = now.strftime("%Y-%m-%d")
            signal_timestamp = now.isoformat(timespec="seconds")

            for source_table, ltp in ltps.items():
                history_df = load_close_history(conn, source_table)
                if history_df.empty:
                    print(f"Skipping {source_table}: no close history available.")
                    continue

                latest_close = round(float(history_df.iloc[-1]["close"]), 2)
                current_rsi = compute_live_rsi(history_df, ltp)
                if current_rsi is None:
                    print(f"Skipping {source_table}: unable to compute live RSI from history and LTP.")
                    continue

                previous_rsi = None
                if current_rsi is not None:
                    previous_rsi = current_rsi

                handle_source_table(
                    conn,
                    source_table,
                    heatmap_df,
                    current_rsi,
                    ltp,
                    signal_date,
                    signal_timestamp,
                    latest_close=latest_close,
                    use_hybrid=args.hybrid,
                    previous_rsi=previous_rsi,
                    send_to_telegram=args.telegram,
                    dry_run=getattr(args, "dry_run", False),
                    telegram_bot_token=getattr(args, "telegram_bot_token", None),
                    telegram_chat_id=getattr(args, "telegram_chat_id", None),
                    min_profit_pct=getattr(args, "min_profit_pct", 0.0),
                    buy_rsi_protection=getattr(args, "buy_rsi_protection", 0.0),
                    confirm_order=getattr(args, "confirmOrder", False),
                )

            print("-----------------------------------------------------------------------------------------------")
            time.sleep(interval_seconds)
    finally:
        if live_feed is not None:
            live_feed.close()
        conn.close()


if __name__ == "__main__":
    main()
