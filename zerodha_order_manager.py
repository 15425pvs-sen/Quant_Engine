#!/usr/bin/env python
"""Manual Zerodha order executor.

This script is designed to be called directly from live_rsi_tracking.py when a
BUY or SELL signal is generated.

It does NOT depend on any SQLite table. Instead it accepts a direct CLI request
and enforces:
- exact per-trade value cap
- total daily limit across all trades
- manual YES/NO confirmation before sending the order to Zerodha

Examples:
    python zerodha_order_manager.py --side BUY --symbol RELIANCE --ltp 2500
    python zerodha_order_manager.py --side SELL --symbol TCS --ltp 3500
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kiteconnect import KiteConnect

TOKEN_FILE = Path(__file__).resolve().parent / "zerodha_tokens.json"
USAGE_FILE = Path(__file__).resolve().parent / "zerodha_daily_usage.json"

# Set your fixed trading limits here.
PER_TRADE_VALUE = 100000.0  # Example: 15000.0
DAILY_LIMIT = 100000.0     # Example: 100000.0


def load_tokens() -> dict[str, Any]:
    if not TOKEN_FILE.exists():
        raise FileNotFoundError("Missing zerodha_tokens.json. Run zerodha_auth.py first.")

    with TOKEN_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not data.get("api_key") or not data.get("access_token"):
        raise ValueError("zerodha_tokens.json is incomplete. Please re-run zerodha_auth.py.")

    return data


def create_kite_client() -> KiteConnect:
    creds = load_tokens()
    kite = KiteConnect(api_key=creds["api_key"])
    kite.set_access_token(creds["access_token"])
    return kite


def get_today_usage() -> tuple[str, float]:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not USAGE_FILE.exists():
        return today, 0.0

    try:
        with USAGE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        return today, 0.0

    if not isinstance(data, dict):
        return today, 0.0

    usage_date = str(data.get("date", today))
    used_value = float(data.get("used_value", 0.0) or 0.0)
    return usage_date, used_value


def save_today_usage(date_str: str, used_value: float) -> None:
    payload = {
        "date": date_str,
        "used_value": round(float(used_value), 2),
    }
    with USAGE_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def get_available_cash(kite: KiteConnect) -> float:
    try:
        margins = kite.margins()
    except Exception as exc:
        raise RuntimeError(f"Unable to fetch available cash from Zerodha: {exc}") from exc

    equity = margins.get("equity", {}) if isinstance(margins, dict) else {}
    available = equity.get("available", {}) if isinstance(equity, dict) else {}

    cash = 0.0
    if isinstance(available, dict):
        cash = float(available.get("cash", 0.0) or 0.0)
        if cash <= 0:
            cash = float(available.get("adhoc_margin", 0.0) or 0.0)

    if cash <= 0:
        cash = float(margins.get("available_cash", 0.0) or 0.0)

    if cash <= 0:
        raise RuntimeError("No usable cash balance found in Zerodha margins response.")

    return cash


def get_net_quantity_for_symbol(kite: KiteConnect, tradingsymbol: str) -> int:
    try:
        positions = kite.positions()
    except Exception as exc:
        raise RuntimeError(f"Unable to fetch positions from Zerodha: {exc}") from exc

    net = positions.get("net", []) if isinstance(positions, dict) else []
    for item in net:
        if str(item.get("tradingsymbol", "")).upper() == str(tradingsymbol).upper():
            return int(float(item.get("quantity", 0) or 0))

    return 0


def estimate_trade_quantity(ltp: float, per_trade_value: float) -> tuple[int, float]:
    if ltp <= 0 or per_trade_value <= 0:
        return 0, 0.0

    qty = int(per_trade_value // ltp)
    if qty <= 0:
        return 0, 0.0

    order_value = qty * ltp
    return qty, float(order_value)


def confirm_prompt(side: str, symbol: str, ltp: float, quantity: int, order_value: float) -> bool:
    print("\nOrder confirmation required")
    print("-" * 80)
    print(f"Side: {side.upper()}")
    print(f"Symbol: {symbol.upper()}")
    print(f"LTP: ₹{ltp:,.2f}")
    print(f"Qty: {quantity}")
    print(f"Approx order value: ₹{order_value:,.2f}")
    print("-" * 80)

    while True:
        response = input('Type YES to confirm or NO to cancel: ').strip().upper()
        if response == "YES":
            return True
        if response == "NO":
            return False
        print("Invalid input. Please type YES or NO only.")


def validate_trade_amount(side: str, symbol: str, ltp: float) -> tuple[int, float, float]:
    if ltp <= 0:
        raise ValueError("LTP must be greater than zero.")
    if PER_TRADE_VALUE <= 0:
        raise ValueError("PER_TRADE_VALUE must be set to a value greater than zero.")
    if DAILY_LIMIT <= 0:
        raise ValueError("DAILY_LIMIT must be set to a value greater than zero.")

    qty, order_value = estimate_trade_quantity(ltp, PER_TRADE_VALUE)
    if qty <= 0:
        raise ValueError(
            f"Order quantity came out to zero for {symbol} at LTP ₹{ltp:,.2f} and per-trade-value ₹{PER_TRADE_VALUE:,.2f}."
        )

    usage_date, used_today = get_today_usage()
    if usage_date != datetime.now(timezone.utc).strftime("%Y-%m-%d"):
        used_today = 0.0

    remaining_today = max(0.0, DAILY_LIMIT - used_today)
    if order_value > remaining_today:
        raise ValueError(
            f"This trade would exceed today's remaining allocation. "
            f"Order value ₹{order_value:,.2f} > remaining ₹{remaining_today:,.2f}."
        )

    return qty, order_value, used_today


def place_order(kite: KiteConnect, side: str, symbol: str, quantity: int, ltp: float) -> dict[str, Any]:
    symbol = symbol.upper()
    tx_type = "BUY" if side.upper() == "BUY" else "SELL"

    if side.upper() == "SELL":
        net_qty = get_net_quantity_for_symbol(kite, symbol)
        if net_qty <= 0:
            raise RuntimeError(f"SELL signal for {symbol} rejected: no net quantity available to sell.")
        quantity = min(quantity, abs(net_qty))
        if quantity <= 0:
            raise RuntimeError(f"SELL signal for {symbol} rejected: calculated quantity is zero.")

    order_id = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=kite.EXCHANGE_NSE,
        tradingsymbol=symbol,
        transaction_type=getattr(kite, f"TRANSACTION_TYPE_{tx_type}"),
        quantity=quantity,
        product=kite.PRODUCT_CNC,
        order_type=kite.ORDER_TYPE_MARKET
    )

    return {
        "order_id": str(order_id),
        "status": "PENDING",
        "tradingsymbol": symbol,
        "exchange": "NSE",
        "quantity": int(quantity),
        "ltp": float(ltp),
    }

def update_daily_usage(order_value: float) -> None:
    date_str, used_today = get_today_usage()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if date_str != today:
        used_today = 0.0
    save_today_usage(today, used_today + float(order_value))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Place a Zerodha BUY or SELL order with manual confirmation.")
    parser.add_argument("--side", choices=["BUY", "SELL"], required=True, help="Side of the trade.")
    parser.add_argument("--symbol", required=True, help="Trading symbol, for example RELIANCE or TCS.")
    parser.add_argument("--ltp", type=float, required=True, help="Current LTP for the stock.")
    parser.add_argument("--dry-run", action="store_true", help="Print order details without placing a live order.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        qty, order_value, used_today = validate_trade_amount(
            args.side,
            args.symbol,
            args.ltp,
        )

        print(f"Daily used so far: ₹{used_today:,.2f}")
        print(f"Remaining today: ₹{max(0.0, DAILY_LIMIT - used_today):,.2f}")
        print(f"Calculated qty: {qty}")
        print(f"Proposed value: ₹{order_value:,.2f}")
        print(f"Configured PER_TRADE_VALUE: ₹{PER_TRADE_VALUE:,.2f}")
        print(f"Configured DAILY_LIMIT: ₹{DAILY_LIMIT:,.2f}")
        if args.dry_run:
            print("DRY RUN: no live order sent to Zerodha.")
            return

        approved = confirm_prompt(args.side, args.symbol, args.ltp, qty, order_value)
        if not approved:
            print("Order cancelled by user.")
            return

        kite = create_kite_client()
        order_result = place_order(kite, args.side, args.symbol, qty, args.ltp)
        update_daily_usage(order_value)

        print("\nOrder successfully placed")
        print("-" * 80)
        print(f"Order ID: {order_result['order_id']}")
        print(f"Side: {args.side.upper()}")
        print(f"Symbol: {args.symbol.upper()}")
        print(f"Qty: {order_result['quantity']}")
        print(f"LTP: ₹{order_result['ltp']:.2f}")
        print(f"Order value: ₹{order_value:,.2f}")
        print(f"Status: {order_result['status']}")

    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExecution interrupted by user.")
        sys.exit(0)
