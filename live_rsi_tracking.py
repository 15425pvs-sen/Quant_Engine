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
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
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
HEATMAP_TABLE = "rsi_heatmap_data_for_trading"
SIGNAL_LOG_TABLE = "rsi_live_signal_log_trading"
LTP_HISTORY_PERIOD = "5d"
LTP_INTERVAL = "1m"
DEFAULT_INTERVAL_SECONDS = 30

BUY_OPEN_STATE = "OPEN"
BUY_CLOSED_STATE = "CLOSED"
SELL_CONFIRMED_STATE = "CONFIRMED"

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
            notes             TEXT,
            position_state    TEXT NOT NULL DEFAULT 'OPEN',
            buy_signal_id     INTEGER,
            trigger_exit_rsi  INTEGER,
            action_timestamp  TEXT,
            closed_by_signal_id INTEGER
        )
        """
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


def get_live_ltps(symbols: list[str]) -> dict[str, float]:
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


def get_open_buy_rows(conn: sqlite3.Connection, source_table: str) -> list[tuple[int, int, int]]:
    rows = conn.execute(
        f"""
        SELECT id, entry_rsi, exit_rsi
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE trim(upper(source_table)) = trim(upper(?))
          AND signal_type = 'BUY'
          AND COALESCE(position_state, '{BUY_OPEN_STATE}') = '{BUY_OPEN_STATE}'
        ORDER BY signal_timestamp ASC, id ASC
        """,
        (source_table,),
    ).fetchall()
    return [(int(row[0]), int(row[1]), int(row[2])) for row in rows]


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
            position_state
        FROM {quote_identifier(SIGNAL_LOG_TABLE)}
        WHERE signal_type = 'SELL'
          AND buy_signal_id IS NOT NULL
          AND position_state IN ('CONFIRMED', 'CLOSED')
        ORDER BY source_table ASC, id ASC
        """
    sell_rows = pd.read_sql(query, conn)
    if sell_rows.empty:
        return pd.DataFrame(columns=["source_table", "trades", "entry_price", "exit_price", "pnl_pct"])

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
        if entry_price <= 0:
            continue

        pnl_pct = ((exit_price - entry_price) / entry_price) * 100.0
        results.append(
            {
                "source_table": str(row["source_table"]).strip().upper(),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl_pct": round(float(pnl_pct), 2),
            }
        )

    if not results:
        return pd.DataFrame(columns=["source_table", "trades", "entry_price", "exit_price", "pnl_pct"])

    results_df = pd.DataFrame(results)
    summary = (
        results_df.groupby("source_table", sort=True)
        .agg(trades=("pnl_pct", "size"), entry_price=("entry_price", "mean"), exit_price=("exit_price", "mean"), pnl_pct=("pnl_pct", "mean"))
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
            f"{str(row['source_table']).upper():<12} trades={int(row['trades']):>2}  P&L%={float(row['pnl_pct']):>8.2f}"
        )

    overall_pnl = round(float(results_df["pnl_pct"].mean()), 2)
    print("-" * 60)
    print(f"Overall average P&L%: {overall_pnl:.2f}")


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
        return False

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
        return response.ok
    except Exception:
        return False


def insert_signal_row(conn: sqlite3.Connection, row: dict[str, object]) -> int:
    cursor = conn.execute(
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
            notes,
            position_state,
            buy_signal_id,
            trigger_exit_rsi,
            action_timestamp,
            closed_by_signal_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
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
            row["position_state"],
            row["buy_signal_id"],
            row["trigger_exit_rsi"],
            row["action_timestamp"],
            row["closed_by_signal_id"],
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


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
) -> None:
    table_rules = heatmap_df[heatmap_df["source_table"] == source_table]
    if table_rules.empty:
        return

    ranked_rules = table_rules.sort_values(
        by=["exit_rsi", "entry_rsi"],
        ascending=[True, True],
    )

    # First attempt to close any open BUY buckets with matching exit conditions.
    for buy_id, buy_entry_rsi, buy_exit_rsi in get_open_buy_rows(conn, source_table):
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
                "signal_date": signal_date,
                "signal_timestamp": signal_timestamp,
                "notes": sell_reason,
                "position_state": SELL_CONFIRMED_STATE,
                "buy_signal_id": buy_id,
                "trigger_exit_rsi": buy_exit_rsi,
                "action_timestamp": signal_timestamp,
                "closed_by_signal_id": None,
            },
        )
        close_buy_bucket(conn, buy_id, sell_id, signal_timestamp)
        message = (
            f"SELL {source_table} | closed_buy_id={buy_id} | bucket=({buy_entry_rsi},{buy_exit_rsi}) | "
            f"RSI {current_rsi:.2f} | LTP {ltp:.2f}"
        )
        print(message)
        if send_to_telegram:
            send_telegram_message(message)
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

        insert_signal_row(
            conn,
            {
                "source_table": source_table,
                "signal_type": "BUY",
                "entry_rsi": entry_rsi,
                "exit_rsi": exit_rsi,
                "previous_rsi": current_rsi,
                "current_rsi": current_rsi,
                "ltp": ltp,
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
        message = (
            f"BUY {source_table} | bucket=({entry_rsi},{exit_rsi}) | RSI {current_rsi:.2f} | LTP {ltp:.2f}"
        )
        print(message)
        if send_to_telegram:
            send_telegram_message(message)
        return


def main() -> None:
    args = parse_args()
    interval_seconds = max(5, args.interval)
    requested_symbols = [symbol.strip().upper() for symbol in (args.symbols or []) if symbol.strip()]

    conn = sqlite3.connect(DB_NAME)
    try:
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
            ltps = get_live_ltps(symbols)
            if not ltps:
                print("Unable to fetch live prices. Retrying.")
                time.sleep(interval_seconds)
                continue

            now = datetime.now()
            signal_date = now.strftime("%Y-%m-%d")
            signal_timestamp = now.isoformat(timespec="seconds")

            for source_table, ltp in ltps.items():
                current_rsi, latest_close, _ = load_latest_rsi(conn, source_table)
                if current_rsi is None:
                    print(f"Skipping {source_table}: no latest RSI available.")
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
                )

            time.sleep(interval_seconds)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
