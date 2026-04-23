#!/usr/bin/env python3
"""One-time helper to get a new Dropbox refresh token with updated scopes.

Run this after adding sharing.write (or any other scope) to your Dropbox app:
    .\scripts\venv_python.cmd scripts/dropbox_reauth.py

You'll need your App Key and App Secret from:
    https://www.dropbox.com/developers/apps
"""

import json
import sys
import urllib.parse
import urllib.request


def exchange_code(app_key: str, app_secret: str, code: str) -> dict:
    payload = urllib.parse.urlencode({
        "code": code,
        "grant_type": "authorization_code",
        "client_id": app_key,
        "client_secret": app_secret,
    }).encode()
    req = urllib.request.Request(
        url="https://api.dropboxapi.com/oauth2/token",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())


def main():
    print("=== Dropbox Re-authorization ===\n")
    print("1. Make sure you have already enabled 'sharing.write' (and any other")
    print("   scopes you need) in your Dropbox app console Permissions tab.\n")

    app_key = input("Enter your Dropbox App Key: ").strip()
    app_secret = input("Enter your Dropbox App Secret: ").strip()

    if not app_key or not app_secret:
        print("App key and secret are required.")
        sys.exit(1)

    auth_url = (
        "https://www.dropbox.com/oauth2/authorize"
        f"?client_id={app_key}"
        "&response_type=code"
        "&token_access_type=offline"
    )

    print(f"\n2. Open this URL in your browser and click 'Allow':\n\n   {auth_url}\n")
    code = input("3. Paste the authorization code here: ").strip()

    if not code:
        print("No code entered.")
        sys.exit(1)

    print("\nExchanging code for tokens...")
    try:
        result = exchange_code(app_key, app_secret, code)
    except Exception as exc:
        print(f"Token exchange failed: {exc}")
        sys.exit(1)

    refresh_token = result.get("refresh_token")
    if not refresh_token:
        print(f"No refresh token in response: {result}")
        sys.exit(1)

    print("\n=== Success ===\n")
    print(f"New refresh token:\n  {refresh_token}\n")
    print("Update your config with:")
    print(f"  EDEC_DROPBOX_REFRESH_TOKEN={refresh_token}")
    print(f"  EDEC_DROPBOX_APP_KEY={app_key}")
    print(f"  EDEC_DROPBOX_APP_SECRET={app_secret}")
    print("\nThen restart the bot — the shared link will appear automatically.")


if __name__ == "__main__":
    main()
