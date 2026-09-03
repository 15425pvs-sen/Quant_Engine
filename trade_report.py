#!/usr/bin/env python
from __future__ import annotations

import argparse
import sqlite3
from datetime import timedelta
from pathlib import Path

import pandas as pd

DB_NAME = Path(__file__).resolve().parent / "quant_historic_data.db"
SIGNAL_LOG_TABLE = "rsi_live_signal_log_trading"
ALL_SIGNAL_LOG_TABLE = "rsi_live_signal_log_trading_all_signals"
TRADE_LIMITS_FILE = Path(__file__).resolve().parent / "zerodha_trade_limits.json"


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Identifier must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Identifier contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


def _parse_signal_date(value: object) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def _is_same_calendar_day(left: object, right: object) -> bool:
    left_ts = _parse_signal_date(left)
    right_ts = _parse_signal_date(right)
    if left_ts is None or right_ts is None:
        return False
    return left_ts.date() == right_ts.date()


def _load_trade_limits() -> tuple[float, float]:
    default_per_trade_value = 15000.0
    default_daily_limit = 60000.0
    if not TRADE_LIMITS_FILE.exists():
        return default_per_trade_value, default_daily_limit

    try:
        import json

        with TRADE_LIMITS_FILE.open("r", encoding="utf-8") as handle:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print trade reports from the RSI signal log.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DB_NAME,
        help="Path to the Quant database file.",
    )
    return parser.parse_args()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    return conn.execute(query, (table_name,)).fetchone() is not None


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
        if reference_timestamp is not None:
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
            _is_same_calendar_day(buy_ts, sell_timestamp) for buy_ts in buy_rows["signal_timestamp"].tolist()
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
                "buy_cost",
                "sell_value",
                "buy_cost_total",
                "sell_cost_total",
                "gross_pnl_abs",
                "net_pnl_abs",
                "gross_pnl_pct",
                "net_pnl_pct",
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


def _print_grouped_pnl(title: str, df: pd.DataFrame, group_field: str) -> None:
    print(f"\n{title}")
    print("-" * 120)
    if df.empty:
        print("None")
        return

    grouped = (
        df.groupby(group_field, sort=True)
        .agg(
            trades=("sell_id", "count"),
            buy_cost=("buy_cost", "sum"),
            gross_pnl_abs=("gross_pnl_abs", "sum"),
            net_pnl_abs=("net_pnl_abs", "sum"),
        )
        .reset_index()
    )
    grouped["gross_pnl_pct"] = grouped.apply(
        lambda row: round((float(row["gross_pnl_abs"]) / float(row["buy_cost"])) * 100.0, 2) if float(row["buy_cost"]) else 0.0,
        axis=1,
    )
    grouped["net_pnl_pct"] = grouped.apply(
        lambda row: round((float(row["net_pnl_abs"]) / float(row["buy_cost"])) * 100.0, 2) if float(row["buy_cost"]) else 0.0,
        axis=1,
    )
    print(
        grouped[
            [group_field, "trades", "buy_cost", "gross_pnl_abs", "net_pnl_abs", "gross_pnl_pct", "net_pnl_pct"]
        ].to_string(index=False)
    )


def _print_monthly_pnl(detail_df: pd.DataFrame) -> None:
    print("\nMonth-wise P&L")
    print("-" * 120)
    if detail_df.empty:
        print("None")
        return

    monthly_df = detail_df.copy()
    monthly_df["year_num"] = monthly_df["signal_timestamp"].dt.year
    monthly_df["month_num"] = monthly_df["signal_timestamp"].dt.month
    monthly = (
        monthly_df.groupby(["year_num", "month_num"], sort=True)
        .agg(net_pnl_abs=("net_pnl_abs", "sum"))
        .reset_index()
    )
    multiple_years = detail_df["signal_timestamp"].dt.year.nunique(dropna=True) > 1
    month_names = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }
    for _, row in monthly.iterrows():
        year_num = int(row["year_num"])
        month_num = int(row["month_num"])
        month_name = month_names.get(month_num, f"Month {month_num}")
        if multiple_years:
            print(f"{month_name} {year_num} - PnL - {float(row['net_pnl_abs']):.2f}")
        else:
            print(f"{month_name} - PnL - {float(row['net_pnl_abs']):.2f}")


def print_trade_results(conn: sqlite3.Connection) -> None:
    detail_df = get_trade_result_detail(conn)
    if detail_df.empty:
        print("No completed BUY/SELL trades found in the signal log.")
        return

    summary_df = get_trade_results(conn)
    per_trade_value, daily_limit = _load_trade_limits()
    print("\nZerodha trade limits")
    print("-" * 120)
    print(f"PER_TRADE_VALUE: {per_trade_value:.2f}")
    print(f"DAILY_LIMIT: {daily_limit:.2f}")

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
    print(f"Net P&L till date: {overall_net_pnl:.2f}")

    detail_df = detail_df.copy()
    detail_df["signal_timestamp"] = pd.to_datetime(detail_df["signal_timestamp"], errors="coerce")
    detail_df = detail_df.dropna(subset=["signal_timestamp"])
    if not detail_df.empty:
        detail_df["week_start"] = detail_df["signal_timestamp"].dt.date.apply(
            lambda d: (d - timedelta(days=d.weekday())).isoformat()
        )
        _print_grouped_pnl("Week-wise P&L", detail_df, "week_start")
        _print_monthly_pnl(detail_df)
    else:
        print("\nWeek-wise P&L")
        print("-" * 120)
        print("None")
        print("\nMonth-wise P&L")
        print("-" * 120)
        print("None")

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


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        if not table_exists(conn, SIGNAL_LOG_TABLE):
            raise SystemExit(f"Signal log table '{SIGNAL_LOG_TABLE}' does not exist.")
        if not table_exists(conn, ALL_SIGNAL_LOG_TABLE):
            raise SystemExit(f"Signal log table '{ALL_SIGNAL_LOG_TABLE}' does not exist.")
        print_trade_results(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
