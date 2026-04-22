"""Dashboard HTML assets for the built-in HA live API."""

from __future__ import annotations

from pathlib import Path


_DASHBOARD_HTML = Path(__file__).with_name("live_api_dashboard.html").read_text(encoding="utf-8")


def render_dashboard_html(app_version: str) -> str:
    return _DASHBOARD_HTML.replace("__APP_VERSION__", app_version)
