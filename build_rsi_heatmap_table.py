#!/usr/bin/env python
"""
Build and store RSI heatmap data for a selected equity table.

This script mirrors the core logic from rsi_dashboard.py:
1. Load trade_date, close, and rsi from a source table.
2. Simulate RSI entry/exit trades across a strategy grid.
3. Store each heatmap cell as one row in a dedicated SQLite table.

Usage:
    py build_rsi_heatmap_table.py <SOURCE_TABLE>

Optional:
    py build_rsi_heatmap_table.py <SOURCE_TABLE> <OUTPUT_TABLE>
    py build_rsi_heatmap_table.py <SOURCE_TABLE> <OUTPUT_TABLE> <DB_PATH>
    py build_rsi_heatmap_table.py <SOURCE_TABLE> --force
"""

import sys
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_DB_NAME = "quant_historic_data.db"
DEFAULT_OUTPUT_TABLE = "rsi_heatmap_data"
REQUIRED_SOURCE_COLUMNS = {"trade_date", "close", "rsi"}
SYSTEM_TABLES = {
    "stocks_rsi_cagrs",
    "market_data",
    "quant_engine_runs",
    "quant_engine_steps",
    "rsi_heatmap_data",
    "rsi_heatmap_data_for_trading",
    "rsi_live_signal_log",
    "rsi_live_signal_log_trading",
}

ENTRY_START = 20
ENTRY_END = 65
ENTRY_STEP = 5

EXIT_START_GAP = 5
EXIT_END = 80
EXIT_STEP = 6

MIN_TRADES = 4
USE_STOP_LOSS = True
STOP_LOSS_PCT = -0.12
MIN_AVG_RETURN_PCT = 2.5
DEFAULT_MAX_AVG_RETURN_PCT = None
TRADE_HEATMAP_DEFAULT_MIN_PROFIT = 0.5
TRADE_HEATMAP_DEFAULT_MAX_PROFIT = 2.0
MIN_WIN_RATE_PCT = 50.0
MAX_ACCEPTABLE_LOSS_PCT = -15.0
MAX_RULES_PER_TABLE = 6


def quote_identifier(name):
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Table name must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Table name contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


def normalize_identifier(name):
    cleaned = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
    cleaned = cleaned.strip("_")
    return cleaned or "rsi_heatmap_data"


def table_exists(conn, table_name):
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_columns(conn, table_name):
    return {
        row[1].lower()
        for row in conn.execute(f"PRAGMA table_info({quote_identifier(table_name)})")
    }


def validate_source_table(conn, table_name):
    if not table_exists(conn, table_name):
        raise RuntimeError(f"Source table '{table_name}' does not exist.")

    columns = get_table_columns(conn, table_name)
    missing = REQUIRED_SOURCE_COLUMNS - columns
    if missing:
        raise RuntimeError(
            f"Source table '{table_name}' is missing required columns: {sorted(missing)}"
        )


def get_equity_tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    equity_tables = []
    for (name,) in rows:
        if name in SYSTEM_TABLES:
            continue
        cols = get_table_columns(conn, name)
        if REQUIRED_SOURCE_COLUMNS.issubset(cols):
            equity_tables.append(name)
    return equity_tables


def create_output_table(conn, table_name):
    safe_suffix = normalize_identifier(table_name)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_identifier(table_name)} (
            source_table      TEXT NOT NULL,
            entry_rsi         INTEGER NOT NULL,
            exit_rsi          INTEGER NOT NULL,
            avg_return_pct    REAL NOT NULL,
            trades            INTEGER NOT NULL,
            win_rate_pct      REAL NOT NULL,
            min_return_pct    REAL NOT NULL,
            max_return_pct    REAL NOT NULL,
            first_trade_date  TEXT,
            last_trade_date   TEXT,
            generated_at      TEXT NOT NULL,
            PRIMARY KEY (source_table, entry_rsi, exit_rsi)
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{safe_suffix}_source_table
        ON {quote_identifier(table_name)} (source_table)
        """
    )
    conn.commit()


def create_trading_signal_log_table(conn):
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS rsi_live_signal_log_trading (
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
        CREATE INDEX IF NOT EXISTS idx_rsi_live_signal_log_trading_lookup
        ON rsi_live_signal_log_trading (source_table, entry_rsi, exit_rsi, signal_timestamp)
        """
    )
    conn.commit()


def heatmap_data_exists(conn, output_table, source_table):
    row = conn.execute(
        f"""
        SELECT 1
        FROM {quote_identifier(output_table)}
        WHERE source_table = ?
        LIMIT 1
        """,
        (source_table,),
    ).fetchone()
    return row is not None


def load_data(conn, table_name):
    df = pd.read_sql(
        f"""
        SELECT trade_date, close, rsi
        FROM {quote_identifier(table_name)}
        ORDER BY trade_date
        """,
        conn,
    )

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")

    for col in ("close", "rsi"):
        df[col] = pd.to_numeric(
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip(),
            errors="coerce",
        )

    df = df.dropna(subset=["trade_date", "close", "rsi"]).reset_index(drop=True)
    return df


def simulate(df, entry, exit_rsi, use_sl=USE_STOP_LOSS, sl=STOP_LOSS_PCT):
    closes = df["close"].values
    rsis = df["rsi"].values
    dates = df["trade_date"].values

    n = len(df)
    trade_returns = []
    trade_exit_dates = []

    i = 1
    while i < n:
        if rsis[i - 1] < entry <= rsis[i]:
            entry_price = closes[i]
            j = i + 1

            while j < n:
                ret = (closes[j] / entry_price - 1) * 100

                if rsis[j] >= exit_rsi or (use_sl and ret <= sl * 100):
                    trade_returns.append(ret)
                    trade_exit_dates.append(pd.Timestamp(dates[j]))
                    i = j
                    break

                j += 1
        i += 1

    return trade_returns, trade_exit_dates


def build_strategy_matrix(df, min_avg_return_pct=MIN_AVG_RETURN_PCT, max_avg_return_pct=DEFAULT_MAX_AVG_RETURN_PCT):
    results = []
    stats = {
        "tested_combinations": 0,
        "failed_min_trades": 0,
        "failed_min_avg_return": 0,
        "failed_max_avg_return": 0,
        "failed_min_win_rate": 0,
        "failed_max_loss": 0,
        "passed_all_filters": 0,
    }
    generated_at = datetime.now().isoformat(timespec="seconds")

    for entry in range(ENTRY_START, ENTRY_END + 1, ENTRY_STEP):
        for exit_rsi in range(entry + EXIT_START_GAP, EXIT_END + 1, EXIT_STEP):
            stats["tested_combinations"] += 1
            trades, exit_dates = simulate(df, entry, exit_rsi)

            if len(trades) < MIN_TRADES:
                stats["failed_min_trades"] += 1
                continue

            trade_array = np.array(trades, dtype=float)
            avg_return_pct = round(float(np.mean(trade_array)), 2)
            win_rate_pct = round(float((trade_array > 0).mean() * 100), 2)
            min_return_pct = round(float(np.min(trade_array)), 2)
            max_return_pct = round(float(np.max(trade_array)), 2)

            if avg_return_pct < min_avg_return_pct:
                stats["failed_min_avg_return"] += 1
                continue

            if max_avg_return_pct is not None and avg_return_pct > max_avg_return_pct:
                stats["failed_max_avg_return"] += 1
                continue

            if win_rate_pct < MIN_WIN_RATE_PCT:
                stats["failed_min_win_rate"] += 1
                continue

            if min_return_pct < MAX_ACCEPTABLE_LOSS_PCT:
                stats["failed_max_loss"] += 1
                continue

            results.append(
                {
                    "entry_rsi": entry,
                    "exit_rsi": exit_rsi,
                    "avg_return_pct": avg_return_pct,
                    "trades": int(len(trade_array)),
                    "win_rate_pct": win_rate_pct,
                    "min_return_pct": min_return_pct,
                    "max_return_pct": max_return_pct,
                    "first_trade_date": min(exit_dates).strftime("%Y-%m-%d"),
                    "last_trade_date": max(exit_dates).strftime("%Y-%m-%d"),
                    "generated_at": generated_at,
                }
            )
            stats["passed_all_filters"] += 1

    matrix_df = pd.DataFrame(results)
    if matrix_df.empty:
        return matrix_df, stats

    # Keep compact set of strongest rules per table to avoid noisy/overfit buckets.
    matrix_df = matrix_df.sort_values(
        by=["avg_return_pct", "win_rate_pct", "trades", "min_return_pct", "exit_rsi"],
        ascending=[False, False, False, False, True],
    ).head(MAX_RULES_PER_TABLE)

    return matrix_df.reset_index(drop=True), stats


def replace_heatmap_rows(conn, output_table, source_table, heatmap_df):
    conn.execute(
        f"DELETE FROM {quote_identifier(output_table)} WHERE source_table = ?",
        (source_table,),
    )

    if heatmap_df.empty:
        conn.commit()
        return 0

    heatmap_df = heatmap_df.copy()
    heatmap_df.insert(0, "source_table", source_table)

    heatmap_df.to_sql(output_table, conn, if_exists="append", index=False)
    conn.commit()
    return len(heatmap_df)


def main():
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: py build_rsi_heatmap_table.py <SOURCE_TABLE> [OUTPUT_TABLE] [DB_PATH] [--force]"
        )

    raw_args = sys.argv[1:]
    force_rebuild = "--force" in raw_args
    min_profit = MIN_AVG_RETURN_PCT
    max_profit = DEFAULT_MAX_AVG_RETURN_PCT
    args = []
    i = 0
    while i < len(raw_args):
        arg = raw_args[i]
        if arg == "--force":
            i += 1
            continue
        if arg.startswith("--min-profit="):
            min_profit = float(arg.split("=", 1)[1])
            i += 1
            continue
        if arg == "--min-profit":
            if i + 1 >= len(raw_args):
                raise SystemExit("Missing value for --min-profit")
            min_profit = float(raw_args[i + 1])
            i += 2
            continue
        if arg.startswith("--max-profit="):
            max_profit = float(arg.split("=", 1)[1])
            i += 1
            continue
        if arg == "--max-profit":
            if i + 1 >= len(raw_args):
                raise SystemExit("Missing value for --max-profit")
            max_profit = float(raw_args[i + 1])
            i += 2
            continue
        args.append(arg)
        i += 1

    if not args:
        raise SystemExit(
            "Usage: py build_rsi_heatmap_table.py <SOURCE_TABLE> [OUTPUT_TABLE] [DB_PATH] [--force] [--min-profit X] [--max-profit Y]"
        )

    source_table = None
    output_table = DEFAULT_OUTPUT_TABLE
    db_name = DEFAULT_DB_NAME

    if len(args) == 1 and args[0].strip() == "rsi_heatmap_data_for_trading":
        output_table = "rsi_heatmap_data_for_trading"
    else:
        if len(args) >= 1:
            source_table = args[0].strip()
        if len(args) >= 2:
            output_table = args[1].strip()
        if len(args) >= 3:
            db_name = args[2].strip()

    if output_table == "rsi_heatmap_data_for_trading":
        if not any(arg.startswith("--min-profit") for arg in raw_args):
            min_profit = TRADE_HEATMAP_DEFAULT_MIN_PROFIT
        if not any(arg.startswith("--max-profit") for arg in raw_args):
            max_profit = TRADE_HEATMAP_DEFAULT_MAX_PROFIT

    if max_profit is not None and max_profit < min_profit:
        raise SystemExit("--max-profit must be greater than or equal to --min-profit")

    db_path = Path(db_name)
    conn = sqlite3.connect(str(db_path))

    try:
        create_output_table(conn, output_table)
        create_trading_signal_log_table(conn)

        if source_table is None:
            if output_table == "rsi_heatmap_data_for_trading":
                source_tables = get_equity_tables(conn)
                if not source_tables:
                    print("No equity tables found in database.")
                    return
            else:
                raise SystemExit(
                    "Usage: py build_rsi_heatmap_table.py <SOURCE_TABLE> [OUTPUT_TABLE] [DB_PATH] [--force] [--min-profit X] [--max-profit Y]"
                )
        else:
            source_tables = [source_table]

        for source in source_tables:
            validate_source_table(conn, source)

            if heatmap_data_exists(conn, output_table, source) and not force_rebuild:
                print(
                    f"Heatmap data already exists for source table '{source}' "
                    f"in '{output_table}'. Skipping run. Use --force to rebuild."
                )
                continue

            df = load_data(conn, source)
            if df.empty:
                print(f"No usable rows found in source table '{source}'. Skipping.")
                continue

            heatmap_df, build_stats = build_strategy_matrix(
                df,
                min_avg_return_pct=min_profit,
                max_avg_return_pct=max_profit,
            )
            inserted = replace_heatmap_rows(conn, output_table, source, heatmap_df)

            print(f"Source table         : {source}")
            print(f"Output table         : {output_table}")
            print(f"Database path        : {db_path.resolve()}")
            print(f"Source rows          : {len(df)}")
            print(f"Heatmap rows         : {inserted}")
            print(f"Minimum profit bound : {min_profit:.2f}%")
            if max_profit is not None:
                print(f"Maximum profit bound : {max_profit:.2f}%")

            if not heatmap_df.empty:
                best = heatmap_df.sort_values(
                    by=["avg_return_pct", "trades"],
                    ascending=[False, False],
                ).iloc[0]
                print(
                    "Best strategy  : "
                    f"entry={int(best['entry_rsi'])}, "
                    f"exit={int(best['exit_rsi'])}, "
                    f"avg_return={best['avg_return_pct']:.2f}%, "
                    f"trades={int(best['trades'])}"
                )
            else:
                if build_stats["failed_min_trades"] == build_stats["tested_combinations"]:
                    reason = (
                        f"none met MIN_TRADES={MIN_TRADES}"
                    )
                elif build_stats["failed_min_avg_return"] > 0:
                    reason = (
                        f"none met MIN_AVG_RETURN_PCT={MIN_AVG_RETURN_PCT:.2f}% "
                        f"after passing MIN_TRADES={MIN_TRADES}"
                    )
                elif build_stats["failed_min_win_rate"] > 0:
                    reason = (
                        f"none met MIN_WIN_RATE_PCT={MIN_WIN_RATE_PCT:.2f}% "
                        f"after passing return/trade filters"
                    )
                elif build_stats["failed_max_loss"] > 0:
                    reason = (
                        f"none met MAX_ACCEPTABLE_LOSS_PCT={MAX_ACCEPTABLE_LOSS_PCT:.2f}% "
                        f"(worst loss filter)"
                    )
                else:
                    reason = "no strategy rows were produced"

                print(f"Best strategy  : {reason}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
