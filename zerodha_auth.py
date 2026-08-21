import json
import os
from pathlib import Path

from flask import Flask, request
from kiteconnect import KiteConnect

APP_PORT = 8000
REDIRECT_URL = "http://127.0.0.1:8000/callback"
TOKEN_FILE = Path(__file__).with_name("zerodha_tokens.json")

API_KEY = os.getenv("ZERODHA_API_KEY", "YOUR_API_KEY")
API_SECRET = os.getenv("ZERODHA_API_SECRET", "YOUR_API_SECRET")

app = Flask(__name__)

def save_tokens(token_data: dict) -> None:
    """Persist the Kite access token and related auth details locally."""
    # Kite may return Python datetime/date objects in the session payload.
    # JSON cannot serialize those directly, so convert them to string values.
    TOKEN_FILE.write_text(json.dumps(token_data, indent=2, default=str), encoding="utf-8")
    print(f"Token saved to: {TOKEN_FILE}")


@app.route("/")
def login_page():
    kc = KiteConnect(api_key=API_KEY)
    login_url = kc.login_url()
    return f"""
    <html>
      <body>
        <h2>Zerodha Kite Login</h2>
        <p>Open this URL in your browser:</p>
        <a href="{login_url}">{login_url}</a>
      </body>
    </html>
    """


@app.route("/callback")
def callback():
    request_token = request.args.get("request_token")
    if not request_token:
        return "Missing request_token in callback URL.", 400

    try:
        kc = KiteConnect(api_key=API_KEY)
        session_data = kc.generate_session(request_token, API_SECRET)

        token_payload = {
            "api_key": API_KEY,
            "api_secret": API_SECRET,
            "user_id": session_data.get("user_id"),
            "access_token": session_data.get("access_token"),
            "public_token": session_data.get("public_token"),
            "refresh_token": session_data.get("refresh_token"),
            "login_time": session_data.get("login_time"),
            "redirect_url": REDIRECT_URL,
        }

        save_tokens(token_payload)

        return """
        <html>
          <body>
            <h2>Authentication successful</h2>
            <p>Your Zerodha access token has been saved locally.</p>
            <p>You can now run zerodha_kite.py to fetch your portfolio.</p>
          </body>
        </html>
        """
    except Exception as exc:  # pragma: no cover - user-friendly callback failure path
        return f"Authentication failed: {exc}", 500


if __name__ == "__main__":
    print("===================================================")
    print("Zerodha Auth Flow")
    print(f"Redirect URL: {REDIRECT_URL}")
    print(f"Token file: {TOKEN_FILE}")
    print("Open http://127.0.0.1:8000/ in your browser to log in.")
    print("===================================================")
    app.run(host="127.0.0.1", port=APP_PORT, debug=True)
