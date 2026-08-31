import argparse
import json
from pathlib import Path

from kiteconnect import KiteConnect

TOKEN_FILE = Path(__file__).with_name("zerodha_tokens.json")


def load_credentials() -> dict:
    if not TOKEN_FILE.exists():
        raise FileNotFoundError(
            "No Zerodha token file found. Run zerodha_auth.py first and complete the login flow."
        )

    with TOKEN_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not data.get("api_key") or not data.get("access_token"):
        raise ValueError(
            "The token file is incomplete. Please re-run zerodha_auth.py to generate a valid session."
        )

    return data


def create_kite_client() -> KiteConnect:
    creds = load_credentials()
    kite = KiteConnect(api_key=creds["api_key"])
    kite.set_access_token(creds["access_token"])
    return kite


def _lookup_path(payload, *path):
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _extract_available_margin(payload) -> float | None:
    if isinstance(payload, bool):
        return None
    if isinstance(payload, (int, float)):
        return float(payload)
    if isinstance(payload, list):
        for item in payload:
            margin = _extract_available_margin(item)
            if margin is not None:
                return margin
        return None
    if not isinstance(payload, dict):
        return None

    for path in (
        ("data", "equity", "net"),
        ("data", "equity", "available", "live_balance"),
        ("data", "equity", "available", "cash"),
        ("data", "net"),
        ("data", "available", "live_balance"),
        ("data", "available", "cash"),
        ("equity", "net"),
        ("equity", "available", "live_balance"),
        ("equity", "available", "cash"),
        ("net",),
        ("available", "live_balance"),
        ("available", "cash"),
    ):
        value = _lookup_path(payload, *path)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                continue
    return None


def get_available_funds() -> float | None:
    kite = create_kite_client()
    for getter in (lambda: kite.margins(), lambda: kite.margins("equity")):
        try:
            payload = getter()
        except Exception:
            continue

        margin = _extract_available_margin(payload)
        if margin is not None:
            return margin
    return None


def get_portfolio_details() -> dict:
    kite = create_kite_client()

    profile = kite.profile()
    holdings = kite.holdings()
    positions = kite.positions()
    available_funds = get_available_funds()

    portfolio = {
        "profile": profile,
        "holdings": holdings,
        "positions": positions,
        "available_funds": available_funds,
    }
    return portfolio


def safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def summarize_holdings(holdings: list[dict]) -> tuple[float, float, int]:
    total_value = 0.0
    total_pnl = 0.0
    total_qty = 0

    for item in holdings:
        qty = safe_float(item.get("quantity", 0))
        avg_price = safe_float(item.get("average_price", 0))
        ltp = safe_float(item.get("last_price", 0))

        value = ltp * qty
        pnl = (ltp - avg_price) * qty

        total_value += value
        total_pnl += pnl
        total_qty += int(qty)

    return total_value, total_pnl, total_qty


def format_holdings(holdings: list[dict]) -> None:
    """This function is intentionally kept minimal to avoid printing each stock line."""
    if not holdings:
        print("No holdings found.")
        return

    total_value, total_pnl, total_qty = summarize_holdings(holdings)
    print(f"Total Holdings Count: {total_qty}")
    print(f"Total Portfolio Value: Rs. {total_value:,.2f}")
    print(f"Total P/L: Rs. {total_pnl:,.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Show Zerodha portfolio data and available funds."
    )
    parser.add_argument(
        "--available-funds",
        action="store_true",
        default=True,
        help="Display available Zerodha funds in the portfolio summary.",
    )
    parser.add_argument(
        "--no-available-funds",
        dest="available_funds",
        action="store_false",
        help="Hide available Zerodha funds in the portfolio summary.",
    )
    args = parser.parse_args()

    try:
        portfolio = get_portfolio_details()

        profile = portfolio.get("profile", {})
        holdings = portfolio.get("holdings", [])
        available_funds = portfolio.get("available_funds")
        total_value, total_pnl, _ = summarize_holdings(holdings)

        print("==== Zerodha Portfolio Summary ====")
        print(f"User: {profile.get('user_name') or profile.get('user_id') or 'N/A'}")
        print(f"Account: {profile.get('account') or 'N/A'}")
        if args.available_funds:
            if available_funds is None:
                print("Available Funds: unavailable")
            else:
                print(f"Available Funds: Rs. {available_funds:,.2f}")
        print(f"Total Portfolio Value: Rs. {total_value:,.2f}")
        print(f"Total P/L: Rs. {total_pnl:,.2f}")

        net_positions = portfolio["positions"].get("net", [])
        if net_positions:
            print("\nNet Positions:")
            print("-" * 110)
            for pos in net_positions:
                print(pos)

    except FileNotFoundError as exc:
        print(str(exc))
        print("\nSteps:")
        print("1. Update ZERODHA_API_KEY and ZERODHA_API_SECRET in your environment.")
        print("2. Run: python zerodha_auth.py")
        print("3. Login via the browser and complete the callback.")
        print("4. Run: python zerodha_kite.py")
    except ValueError as exc:
        print(str(exc))
    except Exception as exc:  # pragma: no cover
        print(f"Unexpected error: {exc}")


if __name__ == "__main__":
    main()
