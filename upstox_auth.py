"""
upstox_auth.py

Upstox OAuth 2.0 authentication helper.

First-time setup:
1. Create an Upstox API app and obtain API Key/API Secret.
2. Register the exact redirect URI configured below in the Upstox app.
3. Set environment variables:
       UPSTOX_API_KEY
       UPSTOX_API_SECRET
       UPSTOX_REDIRECT_URI

Example:
    Windows PowerShell:
        $env:UPSTOX_API_KEY="your_api_key"
        $env:UPSTOX_API_SECRET="your_api_secret"
        $env:UPSTOX_REDIRECT_URI="http://127.0.0.1:8765/callback"

    Linux/macOS:
        export UPSTOX_API_KEY="your_api_key"
        export UPSTOX_API_SECRET="your_api_secret"
        export UPSTOX_REDIRECT_URI="http://127.0.0.1:8765/callback"

Run:
    python upstox_auth.py

The script opens the Upstox authorization page in your browser, waits for
the OAuth callback, exchanges the one-time code for an access token, and
stores the token locally in .upstox_token.json.

Do NOT commit .upstox_token.json or your API secret to source control.
"""

from __future__ import annotations

import json
import os
import secrets
import threading
import time
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

import requests


AUTH_URL = "https://api.upstox.com/v2/login/authorization/dialog"
TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token"

TOKEN_FILE = Path(__file__).resolve().parent / ".upstox_token.json"
DEFAULT_TIMEOUT = 30


@dataclass
class UpstoxCredentials:
    api_key: str
    api_secret: str
    redirect_uri: str


def load_credentials() -> UpstoxCredentials:
    api_key = os.getenv("UPSTOX_API_KEY")
    api_secret = os.getenv("UPSTOX_API_SECRET")
    redirect_uri = os.getenv(
        "UPSTOX_REDIRECT_URI",
        "http://127.0.0.1:8765/callback",
    )

    missing = []
    if not api_key:
        missing.append("UPSTOX_API_KEY")
    if not api_secret:
        missing.append("UPSTOX_API_SECRET")

    if missing:
        raise RuntimeError(
            "Missing environment variable(s): "
            + ", ".join(missing)
            + ". See the setup instructions at the top of upstox_auth.py."
        )

    return UpstoxCredentials(api_key, api_secret, redirect_uri)


def save_token(token_data: dict) -> None:
    payload = dict(token_data)
    payload["saved_at"] = int(time.time())

    TOKEN_FILE.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    try:
        # Best effort on platforms that support POSIX permissions.
        TOKEN_FILE.chmod(0o600)
    except OSError:
        pass


def load_token() -> Optional[dict]:
    if not TOKEN_FILE.exists():
        return None

    try:
        data = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    return data if data.get("access_token") else None


def get_access_token() -> Optional[str]:
    token = load_token()
    return token.get("access_token") if token else None


def build_authorization_url(
    credentials: UpstoxCredentials,
    state: str,
) -> str:
    params = {
        "response_type": "code",
        "client_id": credentials.api_key,
        "redirect_uri": credentials.redirect_uri,
        "state": state,
    }
    return f"{AUTH_URL}?{urlencode(params)}"


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    callback_code: Optional[str] = None
    callback_state: Optional[str] = None
    callback_error: Optional[str] = None
    expected_state: Optional[str] = None
    received_event = threading.Event()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path != urlparse(
            os.environ.get(
                "UPSTOX_REDIRECT_URI",
                "http://127.0.0.1:8765/callback",
            )
        ).path:
            self.send_error(404, "Invalid callback path")
            return

        query = parse_qs(parsed.query)

        OAuthCallbackHandler.callback_code = query.get("code", [None])[0]
        OAuthCallbackHandler.callback_state = query.get("state", [None])[0]
        OAuthCallbackHandler.callback_error = query.get("error", [None])[0]

        if (
            OAuthCallbackHandler.expected_state
            and OAuthCallbackHandler.callback_state
            != OAuthCallbackHandler.expected_state
        ):
            OAuthCallbackHandler.callback_error = "Invalid OAuth state"

        success = (
            OAuthCallbackHandler.callback_code is not None
            and OAuthCallbackHandler.callback_error is None
        )

        body = (
            "<html><body>"
            "<h2>Upstox authentication successful.</h2>"
            "<p>You can close this browser tab and return to Python.</p>"
            "</body></html>"
            if success
            else
            "<html><body>"
            "<h2>Upstox authentication failed.</h2>"
            "<p>Return to the Python console for details.</p>"
            "</body></html>"
        )

        self.send_response(200 if success else 400)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

        OAuthCallbackHandler.received_event.set()

    def log_message(self, format: str, *args) -> None:
        # Keep the console output clean.
        return


def exchange_code_for_token(
    credentials: UpstoxCredentials,
    code: str,
) -> dict:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "code": code,
        "client_id": credentials.api_key,
        "client_secret": credentials.api_secret,
        "redirect_uri": credentials.redirect_uri,
        "grant_type": "authorization_code",
    }

    response = requests.post(
        TOKEN_URL,
        headers=headers,
        data=data,
        timeout=DEFAULT_TIMEOUT,
    )

    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_response": response.text}

    if not response.ok:
        raise RuntimeError(
            f"Upstox token request failed "
            f"(HTTP {response.status_code}): {payload}"
        )

    if not payload.get("access_token"):
        raise RuntimeError(
            f"Upstox did not return an access_token: {payload}"
        )

    return payload


def authenticate(timeout_seconds: int = 180) -> str:
    """
    Run the interactive OAuth flow and return a fresh access token.
    """
    credentials = load_credentials()

    parsed_redirect = urlparse(credentials.redirect_uri)
    if parsed_redirect.hostname not in {"127.0.0.1", "localhost"}:
        raise RuntimeError(
            "This local callback implementation expects a localhost redirect URI, "
            "for example http://127.0.0.1:8765/callback."
        )

    if parsed_redirect.port is None:
        raise RuntimeError(
            "UPSTOX_REDIRECT_URI must include a port, e.g. "
            "http://127.0.0.1:8765/callback"
        )

    state = secrets.token_urlsafe(32)

    OAuthCallbackHandler.callback_code = None
    OAuthCallbackHandler.callback_state = None
    OAuthCallbackHandler.callback_error = None
    OAuthCallbackHandler.expected_state = state
    OAuthCallbackHandler.received_event.clear()

    server = HTTPServer(
        (parsed_redirect.hostname, parsed_redirect.port),
        OAuthCallbackHandler,
    )

    server_thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )
    server_thread.start()

    authorization_url = build_authorization_url(credentials, state)

    print("\nOpening Upstox authorization page...")
    print("If the browser does not open automatically, use this URL:")
    print(authorization_url)
    print()

    webbrowser.open(authorization_url)

    try:
        if not OAuthCallbackHandler.received_event.wait(timeout_seconds):
            raise TimeoutError(
                f"Timed out after {timeout_seconds} seconds waiting for "
                "the Upstox OAuth callback."
            )

        if OAuthCallbackHandler.callback_error:
            raise RuntimeError(
                f"Upstox authentication failed: "
                f"{OAuthCallbackHandler.callback_error}"
            )

        code = OAuthCallbackHandler.callback_code
        if not code:
            raise RuntimeError(
                "Upstox callback did not contain an authorization code."
            )

        token_data = exchange_code_for_token(credentials, code)
        save_token(token_data)

        print("Upstox authentication successful.")
        print(f"Token saved to: {TOKEN_FILE}")
        return token_data["access_token"]

    finally:
        server.shutdown()
        server.server_close()


def get_valid_access_token(force_login: bool = False) -> str:
    """
    Return the locally stored token unless force_login=True.

    Upstox access tokens expire according to Upstox's token lifecycle,
    so call authenticate() again when the token is no longer accepted.
    """
    if not force_login:
        token = get_access_token()
        if token:
            return token

    return authenticate()


def main() -> None:
    authenticate()


if __name__ == "__main__":
    main()
