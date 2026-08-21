"""
upstox_portfolio_view.py

Read-only portfolio viewer for Upstox.

Shows:
- Long-term holdings
- Short-term positions

Uses the existing Upstox authentication/session setup from
upstox_order_manager.py.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from upstox_order_manager import UpstoxOrderError, UpstoxOrderManager


def _pick(row: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _match_symbol(row: dict[str, Any], symbol: str | None) -> bool:
    if not symbol:
        return True
    symbol = symbol.strip().upper()
    row_symbol = str(
        _pick(
            row,
            "trading_symbol",
            "tradingsymbol",
            "symbol",
            "security_symbol",
            "display_name",
            default="",
        )
    ).strip().upper()
    return symbol in row_symbol or row_symbol in symbol


def _normalize_portfolio_row(row: dict[str, Any]) -> dict[str, Any]:
    quantity = _pick(row, "qty", "quantity", "net_quantity", "available_quantity")
    avg_price = _pick(row, "average_price", "avg_price", "buy_avg", "buy_average_price")
    ltp = _pick(row, "ltp", "last_price", "last_traded_price", "close_price")
    pnl = _pick(row, "pnl", "realised_pnl", "unrealised_pnl", "profit_loss")
    invested = _pick(row, "investment", "invested_value", "buy_value", "cost_value")
    current = _pick(row, "current_value", "market_value", "value")

    return {
        "symbol": _pick(row, "trading_symbol", "tradingsymbol", "symbol", "security_symbol", "display_name", default="-"),
        "exchange": _pick(row, "exchange", default="-"),
        "product": _pick(row, "product", "product_type", default="-"),
        "quantity": _as_int(quantity),
        "avg_price": _as_float(avg_price),
        "ltp": _as_float(ltp),
        "pnl": _as_float(pnl),
        "invested": _as_float(invested),
        "current_value": _as_float(current),
        "raw": row,
    }


def _print_section(title: str, rows: list[dict[str, Any]], symbol: str | None = None) -> None:
    filtered = [row for row in rows if _match_symbol(row, symbol)]
    print()
    print(title)
    print("-" * len(title))
    if not filtered:
        print("No rows found.")
        return

    normalized = [_normalize_portfolio_row(row) for row in filtered]
    header = f"{'Symbol':<18} {'Exch':<6} {'Prod':<5} {'Qty':>6} {'Avg':>12} {'LTP':>12} {'PnL':>12} {'Invested':>12} {'Current':>12}"
    print(header)
    print("-" * len(header))

    total_qty = 0
    total_pnl = 0.0
    total_invested = 0.0
    total_current = 0.0

    for row in normalized:
        qty = row["quantity"] or 0
        avg = row["avg_price"]
        ltp = row["ltp"]
        pnl = row["pnl"]
        invested = row["invested"]
        current = row["current_value"]

        total_qty += qty
        if pnl is not None:
            total_pnl += pnl
        if invested is not None:
            total_invested += invested
        if current is not None:
            total_current += current

        print(
            f"{str(row['symbol']):<18} "
            f"{str(row['exchange']):<6} "
            f"{str(row['product']):<5} "
            f"{qty:>6} "
            f"{(avg if avg is not None else 0.0):>12.2f} "
            f"{(ltp if ltp is not None else 0.0):>12.2f} "
            f"{(pnl if pnl is not None else 0.0):>12.2f} "
            f"{(invested if invested is not None else 0.0):>12.2f} "
            f"{(current if current is not None else 0.0):>12.2f}"
        )

    print("-" * len(header))
    print(
        f"Totals: qty={total_qty} | pnl={total_pnl:.2f} | invested={total_invested:.2f} | current={total_current:.2f}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="View Upstox holdings and short-term positions")
    parser.add_argument(
        "--symbol",
        help="Optional symbol filter, e.g. ICICIBANK",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw JSON in addition to the formatted view.",
    )
    args = parser.parse_args()

    try:
        manager = UpstoxOrderManager()
        holdings = manager.get_holdings()
        positions = manager.get_positions()
    except UpstoxOrderError as exc:
        print(f"Failed to fetch portfolio data: {exc}")
        return
    except Exception as exc:
        print(f"Unexpected error while fetching portfolio data: {exc}")
        return

    print("Upstox portfolio snapshot")
    print("=" * 26)
    print(f"Holdings rows: {len(holdings)}")
    print(f"Positions rows: {len(positions)}")

    _print_section("Long-term holdings", holdings, args.symbol)
    _print_section("Short-term positions", positions, args.symbol)

    if args.raw:
        print()
        print("Raw holdings JSON")
        print("-" * 18)
        print(json.dumps(holdings, indent=2, default=str))
        print()
        print("Raw positions JSON")
        print("-" * 19)
        print(json.dumps(positions, indent=2, default=str))


if __name__ == "__main__":
    main()
