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


def get_portfolio_details() -> dict:
    kite = create_kite_client()

    profile = kite.profile()
    holdings = kite.holdings()
    positions = kite.positions()

    portfolio = {
        "profile": profile,
        "holdings": holdings,
        "positions": positions,
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
    print(f"Total Portfolio Value: ₹{total_value:,.2f}")
    print(f"Total P/L: ₹{total_pnl:,.2f}")


def main() -> None:
    try:
        portfolio = get_portfolio_details()

        profile = portfolio.get("profile", {})
        holdings = portfolio.get("holdings", [])
        total_value, total_pnl, _ = summarize_holdings(holdings)

        print("==== Zerodha Portfolio Summary ====")
        print(f"User: {profile.get('user_name') or profile.get('user_id') or 'N/A'}")
        print(f"Account: {profile.get('account') or 'N/A'}")
        print(f"Total Portfolio Value: ₹{total_value:,.2f}")
        print(f"Total P/L: ₹{total_pnl:,.2f}")

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
