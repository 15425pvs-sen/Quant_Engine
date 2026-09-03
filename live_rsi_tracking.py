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

$env:ORDER_EXECUTION_BROKER="zerodha"
$env:ZERODHA_ALLOW_LIVE_ORDERS="true"
py live_rsi_tracking.py --hybrid --telegram

Live prices continue to come from Upstox. BUY/SELL order placement goes to Zerodha.
"""

from __future__ import annotations

import argparse
import hashlib
import json
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
ZERODHA_TRADE_LIMITS_FILE = Path(__file__).resolve().parent / "zerodha_trade_limits.json"
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
DEFAULT_BASKET_SELL_RSI_THRESHOLD = 60.0
DEFAULT_BASKET_BUY_PRICE_DISPERSION_PCT = 2.0
DEFAULT_ORDER_BROKER = "zerodha"
ORDER_BROKER_CHOICES = {"zerodha"}

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


def _parse_signal_date(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_same_calendar_day(left: object, right: object) -> bool:
    left_dt = _parse_signal_date(left)
    right_dt = _parse_signal_date(right)
    if left_dt is None or right_dt is None:
        return False
    return left_dt.date() == right_dt.date()


def _load_zerodha_trade_limits() -> tuple[float, float]:
    default_per_trade_value = 15000.0
    default_daily_limit = 60000.0
    if not ZERODHA_TRADE_LIMITS_FILE.exists():
        return default_per_trade_value, default_daily_limit

    try:
        with ZERODHA_TRADE_LIMITS_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default_per_trade_value, default_daily_limit

    if not isinstance(data, dict):
        return default_per_trade_value, default_daily_limit

    try:
        per_trade_value = float(data.get("PER_TRADE_VALUE", default_per_trade_value))
    except (TypeError, ValueError):
        per_trade_value = default_per_trade_value
    try:
        daily_limit = float(data.get("DAILY_LIMIT", default_daily_limit))
    except (TypeError, ValueError):
        daily_limit = default_daily_limit

    if per_trade_value <= 0:
        per_trade_value = default_per_trade_value
    if daily_limit <= 0:
        daily_limit = default_daily_limit
    return per_trade_value, daily_limit


def _zerodha_product_key(product: str | None) -> str:
    return str(product or "").strip().upper()


def estimate_zerodha_order_cost_breakdown(
    order_value: float,
    qty: int,
    product: str | None,
    side: str,
    same_day: bool = False,
) -> dict[str, float]:
    trade_value = max(0.0, float(order_value))
    qty = max(0, int(qty))
    product_key = _zerodha_product_key(product)
    side_key = str(side or "").strip().upper()

    breakdown = {
        "brokerage": 0.0,
        "stt": 0.0,
        "transaction_charges": 0.0,
        "sebi_charges": 0.0,
        "gst": 0.0,
        "stamp_duty": 0.0,
        "dp_charges": 0.0,
        "total": 0.0,
    }

    if trade_value <= 0 or qty <= 0:
        return breakdown

    is_intraday_equivalent = product_key == "I" or bool(same_day)
    dp_charges = 0.0

    if is_intraday_equivalent:
        brokerage = min(20.0, trade_value * 0.0003)
        stt = trade_value * (0.00025 if side_key == "SELL" else 0.0)
        transaction_charges = trade_value * 0.0000307
        sebi_charges = trade_value * 0.000001
        gst = 0.18 * (brokerage + transaction_charges + sebi_charges)
        stamp_duty = trade_value * (0.00003 if side_key == "BUY" else 0.0)
        total = brokerage + stt + transaction_charges + sebi_charges + gst + stamp_duty
    else:
        # Delivery: brokerage is zero, stamp duty applies on buy side, and DP charges
        # apply on the sell side per order.
        brokerage = 0.0
        stt = trade_value * 0.001
        transaction_charges = trade_value * 0.0000307
        sebi_charges = trade_value * 0.000001
        gst = 0.18 * (brokerage + transaction_charges + sebi_charges)
        stamp_duty = trade_value * (0.00015 if side_key == "BUY" else 0.0)
        dp_charges = 15.34 if side_key == "SELL" else 0.0
        total = brokerage + stt + transaction_charges + sebi_charges + gst + stamp_duty + dp_charges

    breakdown.update(
        {
            "brokerage": round(brokerage, 2),
            "stt": round(stt, 2),
            "transaction_charges": round(transaction_charges, 2),
            "sebi_charges": round(sebi_charges, 2),
            "gst": round(gst, 2),
            "stamp_duty": round(stamp_duty, 2),
            "dp_charges": round(dp_charges if product_key != "I" else 0.0, 2),
            "total": round(total, 2),
        }
    )
    return breakdown


def estimate_zerodha_sell_cost(order_value: float, qty: int, product: str | None, same_day: bool = False) -> float:
    return round(estimate_zerodha_sell_cost_breakdown(order_value, qty, product, same_day=same_day)["total"], 2)


def estimate_zerodha_buy_cost(order_value: float, qty: int, product: str | None, same_day: bool = False) -> float:
    return round(estimate_zerodha_buy_cost_breakdown(order_value, qty, product, same_day=same_day)["total"], 2)


def estimate_zerodha_sell_cost_breakdown(
    order_value: float,
    qty: int,
    product: str | None,
    same_day: bool = False,
) -> dict[str, float]:
    return estimate_zerodha_order_cost_breakdown(order_value, qty, product, side="SELL", same_day=same_day)


def estimate_zerodha_buy_cost_breakdown(
    order_value: float,
    qty: int,
    product: str | None,
    same_day: bool = False,
) -> dict[str, float]:
    return estimate_zerodha_order_cost_breakdown(order_value, qty, product, side="BUY", same_day=same_day)


def format_zerodha_sell_cost_breakdown(breakdown: dict[str, float]) -> str:
    return (
        f"brokerage={breakdown.get('brokerage', 0.0):.2f}, "
        f"stt={breakdown.get('stt', 0.0):.2f}, "
        f"txn={breakdown.get('transaction_charges', 0.0):.2f}, "
        f"sebi={breakdown.get('sebi_charges', 0.0):.2f}, "
        f"gst={breakdown.get('gst', 0.0):.2f}, "
        f"stamp={breakdown.get('stamp_duty', 0.0):.2f}, "
        f"dp={breakdown.get('dp_charges', 0.0):.2f}, "
        f"total={breakdown.get('total', 0.0):.2f}"
    )


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
        help="Prompt for Y/N before placing each broker order.",
    )
    parser.add_argument(
        "--confirmOrderTelegram",
        action="store_true",
        help="Request order approval through Telegram instead of the local terminal prompt.",
    )
    parser.add_argument(
        "--telegramConfirmTest",
        action="store_true",
        help="Send a Telegram approval test message and wait for YES/NO reply before exiting.",
    )
    parser.add_argument(
        "--broker",
        choices=sorted(ORDER_BROKER_CHOICES),
        default=DEFAULT_ORDER_BROKER,
        help="Order execution broker. Zerodha is the only supported execution broker.",
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
    parser.add_argument(
        "--no-sync-daily-data",
        action="store_false",
        dest="sync_daily_data",
        default=True,
        help="Disable the automatic after-hours quant_engine sync step.",
    )
    return parser.parse_args()


def normalize_broker_name(value: str | None) -> str:
    return "zerodha"


def get_broker_env_var(broker: str) -> str:
    return "ZERODHA_ALLOW_LIVE_ORDERS"


def get_broker_script_path(broker: str) -> Path:
    return Path(__file__).resolve().parent / "zerodha_order_manager.py"


def get_broker_module_name(broker: str) -> str:
    return "zerodha_order_manager"


def get_broker_display_name(broker: str) -> str:
    return "Zerodha"


def get_order_manager_class(broker: str):
    from zerodha_order_manager import ZerodhaOrderManager

    return ZerodhaOrderManager


def get_update_signal_execution_details(broker: str):
    from zerodha_order_manager import update_signal_execution_details

    return update_signal_execution_details


def is_market_open_now(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    current_minutes = now.hour * 60 + now.minute
    open_minutes = MARKET_OPEN_HOUR * 60 + MARKET_OPEN_MINUTE
    close_minutes = MARKET_CLOSE_HOUR * 60 + MARKET_CLOSE_MINUTE
    return open_minutes <= current_minutes <= close_minutes


def run_daily_quant_sync(requested_symbols: list[str]) -> None:
    quant_engine_path = Path(__file__).resolve().parent / "quant_engine.py"
    if not quant_engine_path.exists():
        print("Daily sync skipped: quant_engine.py not found.")
        return

    cmd = [sys.executable, str(quant_engine_path)]
    if requested_symbols:
        cmd.extend(requested_symbols)

    print("\nRunning daily data sync via quant_engine.py ...")
    completed = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.stdout:
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n")
    if completed.stderr:
        print(completed.stderr, end="" if completed.stderr.endswith("\n") else "\n")
    if completed.returncode != 0:
        raise RuntimeError(
            f"quant_engine.py failed with exit code {completed.returncode}."
        )


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
            net_pnl           REAL,
            signal_date       TEXT NOT NULL,
            signal_timestamp  TEXT NOT NULL,
            notes             TEXT,
            position_state    TEXT NOT NULL DEFAULT 'OPEN',
            buy_signal_id     INTEGER,
            trigger_exit_rsi  INTEGER,
            action_timestamp  TEXT,
            closed_by_signal_id INTEGER,
            basket_buy_ids    TEXT
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
    if "net_pnl" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} ADD COLUMN net_pnl REAL"
        )
    if "basket_buy_ids" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} ADD COLUMN basket_buy_ids TEXT"
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
            net_pnl           REAL,
            signal_date       TEXT NOT NULL,
            signal_timestamp  TEXT NOT NULL,
            notes             TEXT,
            position_state    TEXT NOT NULL DEFAULT 'OPEN',
            buy_signal_id     INTEGER,
            trigger_exit_rsi  INTEGER,
            action_timestamp  TEXT,
            closed_by_signal_id INTEGER,
            basket_buy_ids    TEXT,
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
        ("net_pnl", "REAL"),
        ("basket_buy_ids", "TEXT"),
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
        f"DROP INDEX IF EXISTS idx_{ALL_SIGNAL_LOG_TABLE}_unique_bucket"
    )
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{ALL_SIGNAL_LOG_TABLE}_unique_bucket
        ON {quote_identifier(ALL_SIGNAL_LOG_TABLE)} (
            trim(upper(source_table)),
            signal_type,
            entry_rsi,
            exit_rsi,
            signal_date
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


def get_open_buy_rows(
    conn: sqlite3.Connection,
    source_table: str,
) -> list[tuple[int, int, int, float, int | None, str | None, str | None]]:
    rows = conn.execute(
        f"""
        SELECT id, entry_rsi, exit_rsi, ltp, qty, product, signal_date
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
            None if row[6] is None else str(row[6]),
        )
        for row in rows
    ]


def get_open_basket_buy_rows(
    conn: sqlite3.Connection,
    source_table: str,
    current_signal_date: str,
    current_rsi: float,
) -> tuple[
    list[tuple[int, int, int, float, int, str | None, str | None]],
    list[int],
]:
    eligible: list[tuple[int, int, int, float, int, str | None, str | None]] = []
    excluded_today: list[int] = []
    for buy_id, entry_rsi, exit_rsi, buy_ltp, buy_qty, buy_product, buy_signal_date in get_open_buy_rows(conn, source_table):
        if buy_qty is None or int(buy_qty) <= 0:
            continue
        parsed_buy_date = _parse_signal_date(buy_signal_date)
        if parsed_buy_date is not None and parsed_buy_date.date().isoformat() == current_signal_date:
            excluded_today.append(int(buy_id))
            continue
        if float(current_rsi) <= float(exit_rsi):
            continue
        eligible.append(
            (
                int(buy_id),
                int(entry_rsi),
                int(exit_rsi),
                float(buy_ltp),
                int(buy_qty),
                buy_product,
                buy_signal_date,
            )
        )
    return eligible, excluded_today


def group_open_basket_buy_rows_by_product(
    conn: sqlite3.Connection,
    source_table: str,
    current_signal_date: str,
    current_rsi: float,
) -> tuple[
    dict[str, list[tuple[int, int, int, float, int, str | None, str | None]]],
    list[int],
]:
    grouped: dict[str, list[tuple[int, int, int, float, int, str | None, str | None]]] = {}
    excluded_today: list[int] = []
    rows, excluded_today = get_open_basket_buy_rows(conn, source_table, current_signal_date, current_rsi)
    for row in rows:
        product_key = str(row[5] or "").strip().upper()
        if product_key not in {"D", "I"}:
            continue
        grouped.setdefault(product_key, []).append(row)
    return grouped, excluded_today


def split_basket_buy_rows_by_dispersion(
    basket_buy_rows: list[tuple[int, int, int, float, int, str | None, str | None]],
    max_dispersion_pct: float = DEFAULT_BASKET_BUY_PRICE_DISPERSION_PCT,
) -> list[list[tuple[int, int, int, float, int, str | None, str | None]]]:
    if len(basket_buy_rows) < 2:
        return []

    rows = sorted(basket_buy_rows, key=lambda row: float(row[3]))
    clusters: list[list[tuple[int, int, int, float, int, str | None, str | None]]] = []
    current_cluster: list[tuple[int, int, int, float, int, str | None, str | None]] = []
    cluster_min = 0.0
    cluster_max = 0.0

    for row in rows:
        buy_ltp = float(row[3])
        if buy_ltp <= 0:
            continue
        if not current_cluster:
            current_cluster = [row]
            cluster_min = buy_ltp
            cluster_max = buy_ltp
            continue

        prospective_min = min(cluster_min, buy_ltp)
        prospective_max = max(cluster_max, buy_ltp)
        prospective_dispersion_pct = ((prospective_max - prospective_min) / prospective_min) * 100.0 if prospective_min > 0 else 0.0
        if prospective_dispersion_pct <= float(max_dispersion_pct):
            current_cluster.append(row)
            cluster_min = prospective_min
            cluster_max = prospective_max
            continue

        if len(current_cluster) >= 2:
            clusters.append(current_cluster)
        current_cluster = [row]
        cluster_min = buy_ltp
        cluster_max = buy_ltp

    if len(current_cluster) >= 2:
        clusters.append(current_cluster)

    return clusters


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


def close_buy_buckets(
    conn: sqlite3.Connection,
    buy_ids: list[int],
    sell_id: int,
    signal_timestamp: str,
) -> None:
    if not buy_ids:
        return

    placeholders = ",".join("?" for _ in buy_ids)
    params: list[object] = [BUY_CLOSED_STATE, signal_timestamp, sell_id, *buy_ids]
    conn.execute(
        f"""
        UPDATE {quote_identifier(SIGNAL_LOG_TABLE)}
        SET position_state = ?,
            action_timestamp = ?,
            closed_by_signal_id = ?
        WHERE id IN ({placeholders})
          AND signal_type = 'BUY'
          AND COALESCE(position_state, '{BUY_OPEN_STATE}') = '{BUY_OPEN_STATE}'
        """,
        params,
    )
    conn.commit()


def attempt_basket_sell(
    conn: sqlite3.Connection,
    source_table: str,
    current_rsi: float,
    ltp: float,
    latest_close: float | None,
    signal_date: str,
    signal_timestamp: str,
    basket_buy_rows: list[tuple[int, int, int, float, int, str | None, str | None]],
    excluded_today_buy_ids: list[int],
    min_profit_pct: float,
    send_to_telegram: bool,
    telegram_bot_token: str | None,
    telegram_chat_id: str | None,
    broker: str,
    confirm_order: bool,
    confirm_order_telegram: bool,
    dry_run: bool,
) -> bool:
    if len(basket_buy_rows) < 2 or float(current_rsi) < float(DEFAULT_BASKET_SELL_RSI_THRESHOLD):
        return False

    basket_buy_ids = [int(row[0]) for row in basket_buy_rows]
    basket_qty = sum(int(row[4]) for row in basket_buy_rows)
    basket_cost = sum(float(row[3]) * int(row[4]) for row in basket_buy_rows)
    if basket_qty <= 0:
        return False

    basket_avg_buy = basket_cost / basket_qty
    basket_sell_value = float(ltp) * float(basket_qty)
    basket_pnl = round(basket_sell_value - basket_cost, 2)
    basket_pnl_pct = round(((float(ltp) - float(basket_avg_buy)) / float(basket_avg_buy)) * 100.0, 4) if basket_avg_buy else 0.0
    basket_buy_prices = [float(row[3]) for row in basket_buy_rows if float(row[3]) > 0]
    basket_buy_price_min = min(basket_buy_prices) if basket_buy_prices else 0.0
    basket_buy_price_max = max(basket_buy_prices) if basket_buy_prices else 0.0
    basket_buy_price_dispersion_pct = (
        ((basket_buy_price_max - basket_buy_price_min) / basket_buy_price_min) * 100.0
        if basket_buy_price_min > 0
        else 0.0
    )
    basket_buy_ids_text = ",".join(str(buy_id) for buy_id in basket_buy_ids)
    basket_detail_text = "; ".join(
        f"id={buy_id} entry={entry_rsi} exit={exit_rsi} buy_ltp={buy_ltp:.2f} qty={qty} date={buy_date}"
        for buy_id, entry_rsi, exit_rsi, buy_ltp, qty, _product, buy_date in basket_buy_rows
    )
    basket_same_day = bool(signal_date) and all(
        _is_same_calendar_day(buy_date, signal_date)
        for _buy_id, _entry_rsi, _exit_rsi, _buy_ltp, _qty, _product, buy_date in basket_buy_rows
    )
    excluded_today_text = ",".join(str(buy_id) for buy_id in excluded_today_buy_ids) if excluded_today_buy_ids else "none"
    basket_notes = (
        f"BASKET SELL | buy_ids=[{basket_buy_ids_text}] | avg_buy={basket_avg_buy:.2f} | "
        f"qty={basket_qty} | buy_cost={basket_cost:.2f} | sell_value={basket_sell_value:.2f} | "
        f"PnL={basket_pnl:.2f} ({basket_pnl_pct:.4f}%) | RSI={current_rsi:.2f} | "
        f"dispersion={basket_buy_price_dispersion_pct:.4f}% | "
        f"details={basket_detail_text} | excluded_today_buy_ids={excluded_today_text}"
    )

    if float(ltp) <= float(basket_avg_buy):
        print(
            f"Skipping basket SELL {source_table}: "
            f"LTP={ltp:.2f} <= basket_avg_buy={basket_avg_buy:.2f} even though RSI={current_rsi:.2f} >= {DEFAULT_BASKET_SELL_RSI_THRESHOLD:.2f}"
        )
        return False

    if basket_pnl_pct <= float(min_profit_pct):
        print(
            f"Skipping basket SELL {source_table}: "
            f"profit={basket_pnl_pct:.4f}% <= min_profit_pct={min_profit_pct}"
        )
        return False

    estimated_sell_breakdown = estimate_zerodha_sell_cost_breakdown(
        basket_sell_value,
        basket_qty,
        basket_buy_rows[0][5],
        same_day=basket_same_day,
    )
    estimated_sell_cost = float(estimated_sell_breakdown["total"])
    estimated_buy_cost_total = round(
        sum(
            float(
                estimate_zerodha_buy_cost_breakdown(
                    float(buy_ltp) * int(qty),
                    int(qty),
                    product,
                    same_day=_is_same_calendar_day(buy_date, signal_date),
                )["total"]
            )
            for _buy_id, _entry_rsi, _exit_rsi, buy_ltp, qty, product, buy_date in basket_buy_rows
        ),
        2,
    )
    estimated_net_pnl = round(basket_pnl - estimated_buy_cost_total - estimated_sell_cost, 2)
    if estimated_net_pnl <= 0:
        print(
            f"Skipping basket SELL {source_table}: "
            f"estimated net pnl={estimated_net_pnl:.2f} after charges is not positive."
        )
        return False
    if basket_pnl <= estimated_sell_cost:
        print(
            f"Skipping basket SELL {source_table}: "
            f"PnL={basket_pnl:.2f} <= estimated Zerodha sell cost {estimated_sell_cost:.2f}"
        )
        return False

    _print_live_signal_context(source_table, current_rsi, latest_close, ltp)
    basket_message = (
        f"SELL {source_table} | basket_buy_ids=[{basket_buy_ids_text}] | "
        f"avg_buy={basket_avg_buy:.2f} | qty={basket_qty} | "
        f"SELL LTP {ltp:.2f} | PnL {basket_pnl:.2f} ({basket_pnl_pct:.4f}%) | "
        f"est_cost {estimated_sell_cost:.2f} | est_net_pnl {estimated_net_pnl:.2f} | "
        f"cost_breakdown [{format_zerodha_sell_cost_breakdown(estimated_sell_breakdown)}] | "
        f"RSI {current_rsi:.2f} | excluded_today_buy_ids={excluded_today_text}"
    )
    print(basket_message)

    if confirm_order or confirm_order_telegram:
        prompt = (
            f"Confirm BASKET SELL {source_table} | qty={basket_qty} | "
            f"avg_buy={basket_avg_buy:.2f} | SELL LTP={ltp:.2f} | "
            f"PnL={basket_pnl:.2f} | est_cost={estimated_sell_cost:.2f} | "
            f"est_net_pnl={estimated_net_pnl:.2f} | "
            f"breakdown=[{format_zerodha_sell_cost_breakdown(estimated_sell_breakdown)}] ? [Y/N]: "
        )
        approval_key = _make_approval_key("BST", source_table, basket_qty, signal_timestamp, basket_buy_ids_text)
        if not _resolve_order_confirmation(
            prompt,
            confirm_order_telegram,
            approval_key,
            telegram_bot_token,
            telegram_chat_id,
        ):
            fail_reason = "user declined basket order confirmation"
            print(f"BASKET SELL {source_table} skipped: {fail_reason}.")
            inserted_id = insert_signal_row(
                conn,
                {
                    "source_table": source_table,
                    "signal_type": "SELL",
                    "entry_rsi": int(basket_buy_rows[0][1]),
                    "exit_rsi": int(basket_buy_rows[0][2]),
                    "previous_rsi": current_rsi,
                    "current_rsi": current_rsi,
                    "ltp": ltp,
                    "qty": basket_qty,
                    "product": basket_buy_rows[0][5],
                    "signal_date": signal_date,
                    "signal_timestamp": signal_timestamp,
                    "notes": f"SKIPPED | {fail_reason} | {basket_notes}",
                    "position_state": SELL_CONFIRMED_STATE,
                    "buy_signal_id": None,
                    "net_pnl": estimated_net_pnl,
                    "trigger_exit_rsi": int(basket_buy_rows[0][2]),
                    "action_timestamp": signal_timestamp,
                    "closed_by_signal_id": None,
                    "basket_buy_ids": basket_buy_ids_text,
                },
                table_name=ALL_SIGNAL_LOG_TABLE,
                order_status="SKIPPED",
                order_reason=fail_reason,
            )
            _send_telegram_if_new_all_signal(
                inserted_id,
                basket_message,
                send_to_telegram,
                telegram_bot_token,
                telegram_chat_id,
            )
            return True

    if dry_run:
        print(
            f"BASKET SELL TAKING PLACE {source_table} | "
            f"buy_ids=[{basket_buy_ids_text}] | avg_buy={basket_avg_buy:.2f} | "
            f"final_pnl={basket_pnl:.2f}"
        )
        inserted_id = insert_signal_row(
            conn,
            {
                "source_table": source_table,
                "signal_type": "SELL",
                "entry_rsi": int(basket_buy_rows[0][1]),
                "exit_rsi": int(basket_buy_rows[0][2]),
                "previous_rsi": current_rsi,
                "current_rsi": current_rsi,
                "ltp": ltp,
                "qty": basket_qty,
                "product": basket_buy_rows[0][5],
                "signal_date": signal_date,
                "signal_timestamp": signal_timestamp,
                "notes": f"DRY_RUN | {basket_notes}",
                "position_state": SELL_CONFIRMED_STATE,
                "buy_signal_id": None,
                "net_pnl": estimated_net_pnl,
                "trigger_exit_rsi": int(basket_buy_rows[0][2]),
                "action_timestamp": signal_timestamp,
                "closed_by_signal_id": None,
                "basket_buy_ids": basket_buy_ids_text,
            },
            table_name=ALL_SIGNAL_LOG_TABLE,
            order_status="DRY_RUN",
            order_reason="dry run",
        )
        _send_telegram_if_new_all_signal(
            inserted_id,
            basket_message,
            send_to_telegram,
            telegram_bot_token,
            telegram_chat_id,
        )
        return True

    order_result = trigger_order_execution(
        "SELL",
        source_table,
        ltp,
        broker=broker,
        confirm_order=False,
        basket_buy_ids=basket_buy_ids,
    )
    if not order_result.get("success"):
        fail_reason = _failure_reason_from_order_result(order_result, broker=broker)
        print(f"SELL {source_table} skipped: {fail_reason}.")
        inserted_id = insert_signal_row(
            conn,
            {
                "source_table": source_table,
                "signal_type": "SELL",
                "entry_rsi": int(basket_buy_rows[0][1]),
                "exit_rsi": int(basket_buy_rows[0][2]),
                "previous_rsi": current_rsi,
                "current_rsi": current_rsi,
                "ltp": ltp,
                "qty": basket_qty,
                "product": basket_buy_rows[0][5],
                "signal_date": signal_date,
                "signal_timestamp": signal_timestamp,
                "notes": f"SKIPPED | {fail_reason} | {basket_notes}",
                "position_state": SELL_CONFIRMED_STATE,
                "buy_signal_id": None,
                "net_pnl": estimated_net_pnl,
                "trigger_exit_rsi": int(basket_buy_rows[0][2]),
                "action_timestamp": signal_timestamp,
                "closed_by_signal_id": None,
                "basket_buy_ids": basket_buy_ids_text,
            },
            table_name=ALL_SIGNAL_LOG_TABLE,
            order_status="SKIPPED",
            order_reason=fail_reason,
        )
        _send_telegram_if_new_all_signal(
            inserted_id,
            basket_message,
            send_to_telegram,
            telegram_bot_token,
            telegram_chat_id,
        )
        return True

    order_id = str(order_result.get("order_id") or "").strip()
    if not order_id:
        print(f"SELL {source_table} skipped: Upstox did not return an order id.")
        return True

    print(
        f"BASKET SELL TAKING PLACE {source_table} | "
        f"buy_ids=[{basket_buy_ids_text}] | avg_buy={basket_avg_buy:.2f} | "
        f"final_pnl={basket_pnl:.2f}"
    )
    resolved_qty = int(order_result.get("qty") or basket_qty)
    resolved_product = str(order_result.get("product") or basket_buy_rows[0][5] or "").strip() or None
    sell_breakdown = estimate_zerodha_sell_cost_breakdown(
        float(ltp) * float(resolved_qty),
        resolved_qty,
        resolved_product,
        same_day=basket_same_day,
    )
    sell_cost_total = float(sell_breakdown["total"])
    buy_cost_total = round(
        sum(
            float(
                estimate_zerodha_buy_cost_breakdown(
                    float(buy_ltp) * int(qty),
                    int(qty),
                    product,
                    same_day=_is_same_calendar_day(buy_date, signal_date),
                )["total"]
            )
            for _buy_id, _entry_rsi, _exit_rsi, buy_ltp, qty, product, buy_date in basket_buy_rows
        ),
        2,
    )
    basket_sell_value = float(ltp) * float(resolved_qty)
    basket_gross_pnl = round(basket_sell_value - (basket_avg_buy * float(resolved_qty)), 2)
    basket_net_pnl = round(basket_gross_pnl - buy_cost_total - sell_cost_total, 2)
    basket_sell_id = insert_signal_row(
        conn,
        {
            "source_table": source_table,
            "signal_type": "SELL",
            "entry_rsi": int(basket_buy_rows[0][1]),
            "exit_rsi": int(basket_buy_rows[0][2]),
            "previous_rsi": current_rsi,
            "current_rsi": current_rsi,
            "ltp": ltp,
            "qty": resolved_qty,
            "product": resolved_product,
            "signal_date": signal_date,
            "signal_timestamp": signal_timestamp,
            "notes": basket_notes,
            "position_state": SELL_CONFIRMED_STATE,
            "buy_signal_id": None,
            "net_pnl": basket_net_pnl,
            "trigger_exit_rsi": int(basket_buy_rows[0][2]),
            "action_timestamp": signal_timestamp,
            "closed_by_signal_id": None,
            "basket_buy_ids": basket_buy_ids_text,
        },
    )
    close_buy_buckets(conn, basket_buy_ids, basket_sell_id, signal_timestamp)
    return True


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


def _parse_basket_buy_ids_text(value: object) -> list[int]:
    raw = str(value or "").strip()
    if not raw:
        return []
    ids: list[int] = []
    for part in raw.split(","):
        try:
            buy_id = int(part.strip())
        except (TypeError, ValueError):
            continue
        if buy_id > 0:
            ids.append(buy_id)
    return ids


def _load_signal_rows_by_ids(
    conn: sqlite3.Connection,
    ids: list[int],
    signal_type: str = "BUY",
) -> pd.DataFrame:
    cleaned_ids = [int(signal_id) for signal_id in ids if int(signal_id) > 0]
    if not cleaned_ids:
        return pd.DataFrame(columns=["id", "ltp", "qty", "product"])

    placeholders = ",".join("?" for _ in cleaned_ids)
    query = f"""
        SELECT id, ltp, qty, product, signal_timestamp
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE id IN ({placeholders})
          AND signal_type = ?
        ORDER BY id ASC
    """
    return pd.read_sql(query, conn, params=(*cleaned_ids, signal_type))


def _estimate_order_cost_total_for_rows(
    rows: pd.DataFrame,
    side: str,
    reference_timestamp: object | None = None,
) -> float:
    if rows.empty:
        return 0.0

    total = 0.0
    for _, row in rows.iterrows():
        ltp = pd.to_numeric(pd.Series([row.get("ltp")]), errors="coerce").iloc[0]
        qty = pd.to_numeric(pd.Series([row.get("qty")]), errors="coerce").iloc[0]
        if pd.isna(ltp) or pd.isna(qty):
            continue
        order_value = float(ltp) * float(qty)
        product = row.get("product")
        same_day = False
        if reference_timestamp is not None and pd.notna(reference_timestamp):
            same_day = _is_same_calendar_day(row.get("signal_timestamp"), reference_timestamp)
        if str(side).strip().upper() == "BUY":
            total += float(estimate_zerodha_buy_cost_breakdown(order_value, int(qty), product, same_day=same_day)["total"])
        else:
            total += float(estimate_zerodha_sell_cost_breakdown(order_value, int(qty), product, same_day=same_day)["total"])
    return round(total, 2)


def get_trade_result_detail(conn: sqlite3.Connection) -> pd.DataFrame:
    query = f"""
        SELECT
            source_table,
            id,
            buy_signal_id,
            basket_buy_ids,
            ltp,
            signal_timestamp,
            qty,
            product,
            net_pnl,
            position_state
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE signal_type = 'SELL'
          AND (buy_signal_id IS NOT NULL OR COALESCE(basket_buy_ids, '') <> '')
          AND position_state IN ('CONFIRMED', 'CLOSED')
        ORDER BY source_table ASC, id ASC
        """
    sell_rows = pd.read_sql(query, conn)
    if sell_rows.empty:
        return pd.DataFrame(
            columns=[
                "source_table",
                "sell_id",
                "buy_ids",
                "trade_type",
                "buy_qty",
                "sell_qty",
                "buy_cost",
                "buy_cost_total",
                "sell_value",
                "sell_cost_total",
                "gross_pnl_abs",
                "gross_pnl_pct",
                "net_pnl_abs",
                "net_pnl_pct",
                "signal_timestamp",
            ]
        )

    results: list[dict[str, object]] = []
    for _, row in sell_rows.iterrows():
        buy_ids = []
        buy_signal_id = int(row.get("buy_signal_id") or 0)
        if buy_signal_id > 0:
            buy_ids = [buy_signal_id]
        else:
            buy_ids = _parse_basket_buy_ids_text(row.get("basket_buy_ids"))

        buy_rows = _load_signal_rows_by_ids(conn, buy_ids, signal_type="BUY")
        if buy_rows.empty:
            continue

        buy_rows = buy_rows.copy()
        buy_rows["ltp"] = pd.to_numeric(buy_rows["ltp"], errors="coerce")
        buy_rows["qty"] = pd.to_numeric(buy_rows["qty"], errors="coerce")
        buy_rows = buy_rows.dropna(subset=["ltp", "qty"])
        if buy_rows.empty:
            continue

        buy_qty = int(buy_rows["qty"].sum())
        buy_cost = float((buy_rows["ltp"] * buy_rows["qty"]).sum())
        if buy_qty <= 0 or buy_cost <= 0:
            continue

        sell_qty = int(row.get("qty") or buy_qty)
        sell_price = float(row.get("ltp") or 0.0)
        sell_value = sell_price * float(sell_qty)
        sell_timestamp = row.get("signal_timestamp")
        gross_pnl_abs = round(sell_value - buy_cost, 2)
        gross_pnl_pct = round((gross_pnl_abs / buy_cost) * 100.0, 2) if buy_cost else 0.0

        sell_product = str(row.get("product") or buy_rows.iloc[0].get("product") or "").strip() or None
        buy_cost_total = _estimate_order_cost_total_for_rows(buy_rows, side="BUY", reference_timestamp=sell_timestamp)
        sell_same_day = pd.notna(sell_timestamp) and all(
            _is_same_calendar_day(buy_row_ts, sell_timestamp) for buy_row_ts in buy_rows["signal_timestamp"].tolist()
        )
        sell_cost_total = float(
            estimate_zerodha_sell_cost_breakdown(sell_value, sell_qty, sell_product, same_day=sell_same_day)["total"]
        )
        net_pnl_abs = round(gross_pnl_abs - buy_cost_total - sell_cost_total, 2)
        net_pnl_pct = round((net_pnl_abs / buy_cost) * 100.0, 2) if buy_cost else 0.0

        results.append(
            {
                "source_table": str(row["source_table"]).strip().upper(),
                "sell_id": int(row["id"]),
                "buy_ids": ",".join(str(item) for item in buy_ids),
                "trade_type": "BASKET" if len(buy_ids) > 1 else "SINGLE",
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "buy_cost": round(buy_cost, 2),
                "buy_cost_total": round(buy_cost_total, 2),
                "sell_value": round(sell_value, 2),
                "sell_cost_total": round(sell_cost_total, 2),
                "gross_pnl_abs": gross_pnl_abs,
                "net_pnl_abs": net_pnl_abs,
                "gross_pnl_pct": gross_pnl_pct,
                "net_pnl_pct": net_pnl_pct,
                "signal_timestamp": row.get("signal_timestamp"),
            }
        )

    if not results:
        return pd.DataFrame(columns=[
            "source_table",
            "sell_id",
            "buy_ids",
            "trade_type",
            "buy_qty",
            "sell_qty",
            "buy_cost",
            "buy_cost_total",
            "sell_value",
            "sell_cost_total",
            "gross_pnl_abs",
            "gross_pnl_pct",
            "net_pnl_abs",
            "net_pnl_pct",
            "signal_timestamp",
        ])

    results_df = pd.DataFrame(results)
    results_df["signal_timestamp"] = pd.to_datetime(results_df["signal_timestamp"], errors="coerce")
    return results_df


def get_trade_results(conn: sqlite3.Connection) -> pd.DataFrame:
    detail_df = get_trade_result_detail(conn)
    if detail_df.empty:
        return pd.DataFrame(
            columns=[
                "source_table",
                "trades",
                "gross_pnl_abs",
                "net_pnl_abs",
                "gross_pnl_pct",
                "net_pnl_pct",
                "buy_cost",
                "sell_value",
                "buy_cost_total",
                "sell_cost_total",
            ]
        )

    summary = (
        detail_df.groupby("source_table", sort=True)
        .agg(
            trades=("sell_id", "count"),
            buy_cost=("buy_cost", "sum"),
            sell_value=("sell_value", "sum"),
            buy_cost_total=("buy_cost_total", "sum"),
            sell_cost_total=("sell_cost_total", "sum"),
            gross_pnl_abs=("gross_pnl_abs", "sum"),
            net_pnl_abs=("net_pnl_abs", "sum"),
        )
        .reset_index()
    )
    summary["gross_pnl_pct"] = summary.apply(
        lambda row: round((float(row["gross_pnl_abs"]) / float(row["buy_cost"])) * 100.0, 2) if float(row["buy_cost"]) else 0.0,
        axis=1,
    )
    summary["net_pnl_pct"] = summary.apply(
        lambda row: round((float(row["net_pnl_abs"]) / float(row["buy_cost"])) * 100.0, 2) if float(row["buy_cost"]) else 0.0,
        axis=1,
    )
    summary = summary.sort_values(by=["net_pnl_pct", "source_table"], ascending=[False, True]).reset_index(drop=True)
    return summary


def print_trade_results(conn: sqlite3.Connection) -> None:
    detail_df = get_trade_result_detail(conn)
    if detail_df.empty:
        print("No completed BUY/SELL trades found in the signal log.")
        return

    summary_df = get_trade_results(conn)
    print("\nTrade results detail")
    print("-" * 120)
    print(
        detail_df[
            [
                "source_table",
                "sell_id",
                "trade_type",
                "buy_ids",
                "buy_qty",
                "sell_qty",
                "gross_pnl_abs",
                "net_pnl_abs",
                "signal_timestamp",
            ]
        ].to_string(index=False)
    )

    print("\nTrade results summary")
    print("-" * 120)
    for _, row in summary_df.iterrows():
        print(
            f"{str(row['source_table']).upper():<12} trades={int(row['trades']):>2}  "
            f"GROSS={float(row['gross_pnl_abs']):>10.2f}  "
            f"NET={float(row['net_pnl_abs']):>10.2f}  "
            f"NET%={float(row['net_pnl_pct']):>8.2f}"
        )

    overall_buy_cost = round(float(summary_df["buy_cost"].sum()), 2)
    overall_gross_pnl = round(float(summary_df["gross_pnl_abs"].sum()), 2)
    overall_net_pnl = round(float(summary_df["net_pnl_abs"].sum()), 2)
    overall_gross_pct = round((overall_gross_pnl / overall_buy_cost) * 100.0, 2) if overall_buy_cost else 0.0
    overall_net_pct = round((overall_net_pnl / overall_buy_cost) * 100.0, 2) if overall_buy_cost else 0.0
    print("-" * 120)
    print(f"Overall Trades: {int(summary_df['trades'].sum())}")
    print(f"Overall Buy Cost: {overall_buy_cost:.2f}")
    print(f"Overall Gross P&L: {overall_gross_pnl:.2f} ({overall_gross_pct:.2f}%)")
    print(f"Overall Net P&L: {overall_net_pnl:.2f} ({overall_net_pct:.2f}%)")

    open_buy_query = f"""
        SELECT id, source_table, entry_rsi, exit_rsi, ltp, qty, product, signal_timestamp, position_state
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE signal_type = 'BUY'
          AND position_state = 'OPEN'
        ORDER BY source_table ASC, id ASC
    """
    open_buy_rows = pd.read_sql(open_buy_query, conn)
    print("\nOpen BUY positions")
    print("-" * 120)
    if open_buy_rows.empty:
        print("None")
    else:
        print(
            open_buy_rows[
                [
                    "id",
                    "source_table",
                    "entry_rsi",
                    "exit_rsi",
                    "ltp",
                    "qty",
                    "product",
                    "signal_timestamp",
                ]
            ].to_string(index=False)
        )


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
                net_pnl,
                signal_date,
                signal_timestamp,
                notes,
                position_state,
                buy_signal_id,
                trigger_exit_rsi,
                action_timestamp,
                closed_by_signal_id,
                basket_buy_ids,
                order_status,
                order_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                row.get("net_pnl"),
                row["signal_date"],
                row["signal_timestamp"],
                row["notes"],
                row["position_state"],
                row["buy_signal_id"],
                row["trigger_exit_rsi"],
                row["action_timestamp"],
                row["closed_by_signal_id"],
                row.get("basket_buy_ids"),
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
                net_pnl,
                signal_date,
                signal_timestamp,
                product,
                notes,
                position_state,
                buy_signal_id,
                trigger_exit_rsi,
                action_timestamp,
                closed_by_signal_id,
                basket_buy_ids
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
                row.get("net_pnl"),
                row["signal_date"],
                row["signal_timestamp"],
                row.get("product"),
                row["notes"],
                row["position_state"],
                row["buy_signal_id"],
                row["trigger_exit_rsi"],
                row["action_timestamp"],
                row["closed_by_signal_id"],
                row.get("basket_buy_ids"),
            ),
        )
    conn.commit()
    return int(cursor.lastrowid)


def update_signal_execution_details_local(
    conn: sqlite3.Connection,
    signal_id: int,
    qty: int,
    product: str | None,
) -> bool:
    if signal_id <= 0:
        return False

    try:
        cursor = conn.execute(
            f"UPDATE {quote_identifier(SIGNAL_LOG_TABLE)} SET qty = ?, product = ? WHERE id = ?",
            (int(qty), product, int(signal_id)),
        )
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as exc:
        print(f"Warning: failed to update execution details for signal_id={signal_id}: {exc}")
        return False


def trigger_order_execution(
    side: str,
    symbol: str,
    ltp: float,
    broker: str,
    confirm_order: bool = False,
    signal_id: int | None = None,
    basket_buy_ids: list[int] | None = None,
) -> dict[str, object]:
    broker = normalize_broker_name(broker)
    script_path = get_broker_script_path(broker)
    if not script_path.exists():
        raise RuntimeError(f"Order manager not found: {script_path.name}")

    env_var = get_broker_env_var(broker)
    if os.getenv(env_var, "false").lower() != "true":
        raise RuntimeError(
            f"Live order execution is disabled. Set {env_var}=true before placing {side} {symbol}."
        )

    if side.upper() == "BUY":
        try:
            order_manager_class = get_order_manager_class(broker)
            funds_manager = order_manager_class()
            available_funds = funds_manager.get_available_margin()
            broker_label = get_broker_display_name(broker)
            if available_funds is None:
                print(f"Available {broker_label} funds: unavailable")
            else:
                print(f"Available {broker_label} funds: {available_funds:.2f}")
            if available_funds is not None and available_funds <= 0:
                raise RuntimeError(f"No available funds in {broker_label}.")
        except Exception as exc:
            raise RuntimeError(
                f"Unable to read {get_broker_display_name(broker)} funds before BUY {symbol}: {exc}"
            ) from exc
    elif signal_id is None and not basket_buy_ids:
        raise RuntimeError(f"SELL {symbol} requires --signal-id or --basket-buy-ids.")

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
        if side.upper() == "SELL" and basket_buy_ids:
            basket_arg = ",".join(str(int(buy_id)) for buy_id in basket_buy_ids if int(buy_id) > 0)
            if basket_arg:
                cmd.extend(["--basket-buy-ids", basket_arg])
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
            raise RuntimeError(
                f"{get_broker_display_name(broker)} order manager failed for {side} {symbol} "
                f"with exit code {completed.returncode}.\n"
                f"stdout:\n{stdout_text}\n"
                f"stderr:\n{stderr_text}"
            )

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
            m = re.search(r"Resolved SELL qty from BASKET buy_ids=.*:\s*(\d+)", line)
            if m:
                qty = int(m.group(1))
            m = re.search(r"Resolved SELL product:\s*([A-Z]+)", line)
            if m:
                product = m.group(1)
            m = re.search(r"Available (?:Upstox funds|Zerodha margin):\s*([0-9]+(?:\.[0-9]+)?)", line)
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
            raise RuntimeError(
                f"{get_broker_display_name(broker)} order was not placed for {side} {symbol}.\n"
                f"stdout:\n{stdout_text}\n"
                f"stderr:\n{stderr_text}"
            )

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
        raise RuntimeError(f"Failed to launch {script_path.name} for {side} {symbol}: {exc}") from exc


def _failure_reason_from_order_result(order_result: dict[str, object], broker: str = "upstox") -> str:
    broker_label = get_broker_display_name(broker)
    reason = str(order_result.get("reason") or "").strip()
    if reason:
        return reason

    stdout_text = str(order_result.get("stdout") or "").strip()
    stderr_text = str(order_result.get("stderr") or "").strip()
    combined = "\n".join(part for part in (stdout_text, stderr_text) if part)
    if not combined:
        return f"order was not placed on {broker_label}"

    explicit_markers = (
        "Funds are lower than Per trade value",
        "Insufficient Upstox funds",
        "Insufficient Zerodha margin",
        "Unable to read Zerodha funds",
        "Failed to place Zerodha order",
        "Unable to read Upstox funds",
        "SELL orders require --signal-id",
        "Linked BUY signal row",
        "Insufficient live Upstox inventory",
        "Insufficient live Zerodha inventory",
        "Upstox order manager failed",
        "Zerodha order manager failed",
        "ValueError:",
        "Upstox API error",
        "KiteConnect error",
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
    return f"order was not placed on {broker_label}"


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


def _normalize_confirmation_text(value: str) -> str:
    return " ".join(str(value or "").strip().upper().split())


def _make_approval_key(prefix: str, *parts: object) -> str:
    raw = "|".join(str(part or "") for part in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest().upper()[:6]
    return f"{prefix}-{digest}"


def _telegram_get_updates(token: str, offset: int | None = None) -> list[dict[str, object]]:
    try:
        import requests
    except ImportError:
        raise RuntimeError("Telegram confirmation requires the 'requests' package.")

    params: dict[str, object] = {"timeout": 1, "limit": 100}
    if offset is not None:
        params["offset"] = offset

    response = requests.get(
        f"https://api.telegram.org/bot{token}/getUpdates",
        params=params,
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return []
    result = payload.get("result", [])
    if isinstance(result, list):
        return [item for item in result if isinstance(item, dict)]
    return []


def _telegram_confirmation_reply(
    token: str,
    chat_id: str | None,
    approval_key: str,
    prompt_message: str,
    timeout_seconds: int = 120,
    poll_interval_seconds: float = 2.5,
) -> bool:
    chat = str(chat_id or os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat:
        print("Telegram confirmation skipped: missing bot token or chat id.")
        return False

    approval_key = _normalize_confirmation_text(approval_key)
    if not approval_key:
        print("Telegram confirmation skipped: missing approval key.")
        return False

    approval_message = (
        f"{prompt_message}\n\n"
        f"Reply with <b>YES {approval_key}</b> to approve or <b>NO {approval_key}</b> to reject."
    )
    if not send_telegram_message(approval_message, bot_token=token, chat_id=chat):
        print("Telegram confirmation request could not be sent.")
        return False

    pending_offset: int | None = None
    try:
        latest_updates = _telegram_get_updates(token, offset=None)
        if latest_updates:
            pending_offset = int(latest_updates[-1].get("update_id", 0)) + 1
    except Exception:
        pending_offset = None

    deadline = time.time() + max(5, int(timeout_seconds))
    approval_yes = {f"YES {approval_key}", f"Y {approval_key}", f"APPROVE {approval_key}"}
    approval_no = {f"NO {approval_key}", f"N {approval_key}", f"REJECT {approval_key}"}

    while time.time() < deadline:
        try:
            updates = _telegram_get_updates(token, offset=pending_offset)
        except Exception as exc:
            print(f"Telegram confirmation poll failed: {exc}")
            time.sleep(poll_interval_seconds)
            continue

        for update in updates:
            update_id = update.get("update_id")
            try:
                if update_id is not None:
                    pending_offset = max(int(update_id) + 1, pending_offset or 0)
            except Exception:
                pass

            message = update.get("message")
            if not isinstance(message, dict):
                message = update.get("edited_message")
            if not isinstance(message, dict):
                continue

            chat = message.get("chat")
            if not isinstance(chat, dict):
                continue
            if str(chat.get("id")) != chat_id:
                continue

            text = _normalize_confirmation_text(str(message.get("text") or ""))
            if not text:
                continue

            if text in approval_yes:
                return True
            if text in approval_no:
                return False

        time.sleep(poll_interval_seconds)

    print(f"Telegram confirmation timed out after {timeout_seconds} seconds.")
    return False


def _resolve_order_confirmation(
    message: str,
    use_telegram_confirmation: bool,
    approval_key: str,
    telegram_bot_token: str | None,
    telegram_chat_id: str | None,
) -> bool:
    if use_telegram_confirmation:
        token = telegram_bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        return _telegram_confirmation_reply(
            token=token or "",
            chat_id=telegram_chat_id,
            approval_key=approval_key,
            prompt_message=message,
        )
    return _prompt_order_confirmation(message)


def _run_telegram_confirmation_test(
    telegram_bot_token: str | None,
    telegram_chat_id: str | None,
) -> bool:
    token = telegram_bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
    chat = telegram_chat_id or os.getenv("TELEGRAM_CHAT_ID")
    approval_key = _make_approval_key("TST", datetime.now().strftime("%Y%m%d%H%M%S"))
    message = (
        "Telegram confirmation test.\n\n"
        f"Reply with <b>YES {approval_key}</b> to confirm the bot can receive replies or "
        f"<b>NO {approval_key}</b> to reject."
    )
    print(f"Sending Telegram confirmation test with approval key: {approval_key}")
    return _telegram_confirmation_reply(
        token=token or "",
        chat_id=chat,
        approval_key=approval_key,
        prompt_message=message,
        timeout_seconds=180,
        poll_interval_seconds=2.5,
    )


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
    broker: str = "upstox",
    confirm_order: bool = False,
    confirm_order_telegram: bool = False,
 ) -> None:
    table_rules = heatmap_df[heatmap_df["source_table"] == source_table]
    if table_rules.empty:
        return

    ranked_rules = table_rules.sort_values(
        by=["exit_rsi", "entry_rsi"],
        ascending=[True, True],
    )

    # Basket SELL: driven by row-level exit_rsi crossings.
    basket_executed = False
    basket_groups, excluded_today_buy_ids = group_open_basket_buy_rows_by_product(
        conn,
        source_table,
        signal_date,
        current_rsi,
    )
    for basket_product, basket_buy_rows in basket_groups.items():
        qualifying_rows = [row for row in basket_buy_rows if float(ltp) > float(row[3])]
        if len(qualifying_rows) < 2:
            continue
        basket_clusters = split_basket_buy_rows_by_dispersion(qualifying_rows)
        for basket_buy_cluster in basket_clusters:
            if len(basket_buy_cluster) < 2:
                continue
            if attempt_basket_sell(
                conn,
                source_table,
                current_rsi,
                ltp,
                latest_close,
                signal_date,
                signal_timestamp,
                basket_buy_cluster,
                excluded_today_buy_ids,
                min_profit_pct,
                send_to_telegram,
                telegram_bot_token,
                telegram_chat_id,
                broker,
                confirm_order,
                confirm_order_telegram,
                dry_run,
            ):
                basket_executed = True
    if basket_executed:
        return

    # First attempt to close any open BUY buckets with matching exit conditions.
    for buy_id, buy_entry_rsi, buy_exit_rsi, buy_ltp, buy_qty, buy_product, buy_signal_date in get_open_buy_rows(conn, source_table):
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
            print(
                f"Skipping SELL {source_table} for buy_id={buy_id}: "
                f"LTP={ltp:.2f} <= BUY_LTP={buy_ltp:.2f} even though RSI={current_rsi:.2f} >= exit_rsi={buy_exit_rsi}"
            )
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
        same_day_sell = _is_same_calendar_day(buy_signal_date, signal_date)
        estimated_sell_breakdown = estimate_zerodha_sell_cost_breakdown(
            float(ltp) * float(qty),
            qty,
            buy_product,
            same_day=same_day_sell,
        )
        estimated_sell_cost = float(estimated_sell_breakdown["total"])
        estimated_buy_cost = float(
            estimate_zerodha_buy_cost_breakdown(
                float(buy_ltp) * float(qty),
                qty,
                buy_product,
                same_day=same_day_sell,
            )["total"]
        )
        estimated_net_pnl = round(pnl - estimated_buy_cost - estimated_sell_cost, 2)
        if pnl <= estimated_sell_cost:
            print(
                f"Skipping SELL {source_table} for buy_id={buy_id}: "
                f"PnL={pnl:.2f} <= estimated Zerodha sell cost {estimated_sell_cost:.2f}"
            )
            continue
        _print_live_signal_context(source_table, current_rsi, latest_close, ltp)
        message = (
            f"SELL {source_table} | closed_buy_id={buy_id} | bucket=({buy_entry_rsi},{buy_exit_rsi}) | "
            f"BUY PRICE {buy_ltp:.2f} | SELL LTP {ltp:.2f} | Qty {qty} | PnL {pnl:.2f} ({pnl_pct:.4f}%) | "
            f"est_cost {estimated_sell_cost:.2f} | est_net_pnl {estimated_net_pnl:.2f} | "
            f"cost_breakdown [{format_zerodha_sell_cost_breakdown(estimated_sell_breakdown)}] | "
            f"RSI {current_rsi:.2f}"
        )
        print(message)
        if confirm_order or confirm_order_telegram:
            prompt = (
                f"Confirm SELL {source_table} | qty={qty} | BUY LTP={buy_ltp:.2f} | "
                f"SELL LTP={ltp:.2f} | PnL={pnl:.2f} | est_cost={estimated_sell_cost:.2f} | "
                f"est_net_pnl={estimated_net_pnl:.2f} | "
                f"breakdown=[{format_zerodha_sell_cost_breakdown(estimated_sell_breakdown)}] ? [Y/N]: "
            )
            approval_key = _make_approval_key("SL", source_table, buy_id, signal_timestamp)
            if not _resolve_order_confirmation(
                prompt,
                confirm_order_telegram,
                approval_key,
                telegram_bot_token,
                telegram_chat_id,
            ):
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
                        "net_pnl": estimated_net_pnl,
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
                        "net_pnl": estimated_net_pnl,
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
            broker=broker,
            confirm_order=False,
            signal_id=buy_id,
        )
        if not order_result.get("success"):
            fail_reason = _failure_reason_from_order_result(order_result, broker=broker)
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
                    "net_pnl": estimated_net_pnl,
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
            resolved_qty = int(order_result.get("qty") or qty)
            resolved_product = str(order_result.get("product") or buy_product or "").strip() or None
            same_day_sell = _is_same_calendar_day(buy_signal_date, signal_date)
            sell_breakdown = estimate_zerodha_sell_cost_breakdown(
                float(ltp) * float(resolved_qty),
                resolved_qty,
                resolved_product,
                same_day=same_day_sell,
            )
            sell_cost_total = float(sell_breakdown["total"])
            buy_cost_total = float(
                estimate_zerodha_buy_cost_breakdown(
                    float(buy_ltp) * float(resolved_qty),
                    resolved_qty,
                    resolved_product,
                    same_day=same_day_sell,
                )["total"]
            )
            sell_gross_pnl = round((float(ltp) - float(buy_ltp)) * float(resolved_qty), 2)
            sell_net_pnl = round(sell_gross_pnl - buy_cost_total - sell_cost_total, 2)
            if update_signal_execution_details_local(conn, buy_id, resolved_qty, resolved_product):
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
                "net_pnl": sell_net_pnl,
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
                "net_pnl": sell_net_pnl,
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
        if confirm_order or confirm_order_telegram:
            try:
                from zerodha_order_manager import (
                    add_today_reject,
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

                available_funds = get_order_manager_class("zerodha")().get_available_margin()
                configured_per_trade_value, configured_daily_limit = _load_zerodha_trade_limits()
                prompt_qty, prompt_order_value, used_today = validate_trade_amount(
                    source_table,
                    ltp,
                    available_margin=available_funds,
                )
                prompt = (
                    f"Confirm BUY {source_table} | qty={prompt_qty} | order_value={prompt_order_value:.2f} | "
                    f"available_funds={available_funds:.2f} | "
                    f"remaining_daily_limit={max(0.0, configured_daily_limit - used_today):.2f} | "
                    f"PER_TRADE_VALUE={configured_per_trade_value:.2f} ? [Y/N]: "
                )
                approval_key = _make_approval_key("BK", source_table, entry_rsi, signal_timestamp, prompt_qty)
                if not _resolve_order_confirmation(
                    prompt,
                    confirm_order_telegram,
                    approval_key,
                    telegram_bot_token,
                    telegram_chat_id,
                ):
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

        order_result = trigger_order_execution("BUY", source_table, ltp, broker=broker, confirm_order=False)
        if not order_result.get("success"):
            fail_reason = _failure_reason_from_order_result(order_result, broker=broker)
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
                buy_limit, _daily_limit = _load_zerodha_trade_limits()
            except Exception:
                buy_limit = 0.0
            buy_qty = max(1, int(float(buy_limit) // float(ltp))) if float(buy_limit) > 0 else 1
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
    broker = normalize_broker_name(getattr(args, "broker", None))
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
    print("Price feed: Upstox live websocket")
    print(f"Order execution broker: {get_broker_display_name(broker)}")
    print(f"Confirm order: {getattr(args, 'confirmOrder', False)}")
    print(f"Confirm order via Telegram: {getattr(args, 'confirmOrderTelegram', False)}")
    configured_per_trade_value, configured_daily_limit = _load_zerodha_trade_limits()
    print(f"Zerodha trade limits: PER_TRADE_VALUE={configured_per_trade_value:.2f} DAILY_LIMIT={configured_daily_limit:.2f}")
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
            order_manager_class = get_order_manager_class(broker)
            funds_manager = order_manager_class()
            available_funds = funds_manager.get_available_margin()
            broker_label = get_broker_display_name(broker)
            if available_funds is None:
                print(f"Available {broker_label} funds: unavailable")
            else:
                print(f"Available {broker_label} funds: {available_funds:.2f}")
            print("-" * 48 + "\n")
        except Exception as exc:
            print(f"Unable to read {get_broker_display_name(broker)} available funds at startup: {exc}")
            print("-" * 48 + "\n")

    if getattr(args, "telegramConfirmTest", False):
        ok = _run_telegram_confirmation_test(
            getattr(args, "telegram_bot_token", None),
            getattr(args, "telegram_chat_id", None),
        )
        if not ok:
            raise RuntimeError("Telegram confirmation test failed or timed out.")
        print("Telegram confirmation test completed successfully.")
        return

    if getattr(args, "sync_daily_data", True):
        try:
            run_daily_quant_sync(requested_symbols)
        except Exception as exc:
            raise RuntimeError(f"Daily data sync failed: {exc}") from exc

    conn = sqlite3.connect(DB_NAME, timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        ensure_signal_log_table(conn)
        if args.results:
            report_script = Path(__file__).resolve().with_name("trade_report.py")
            if not report_script.exists():
                raise RuntimeError(f"Report script not found: {report_script.name}")
            completed = subprocess.run(
                [sys.executable, str(report_script)],
                check=False,
            )
            if completed.returncode != 0:
                raise RuntimeError(f"trade_report.py exited with status {completed.returncode}")
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
                    broker=broker,
                    confirm_order=getattr(args, "confirmOrder", False),
                    confirm_order_telegram=getattr(args, "confirmOrderTelegram", False),
                )

            print("-----------------------------------------------------------------------------------------------")
            time.sleep(interval_seconds)
    finally:
        if live_feed is not None:
            live_feed.close()
        conn.close()


if __name__ == "__main__":
    main()
