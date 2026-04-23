"""Helpers for the optional OpenAI weekly-tuning dependency."""

from __future__ import annotations

from types import ModuleType


_IMPORT_ERROR: ModuleNotFoundError | None = None

try:
    import openai as _openai
except ModuleNotFoundError as exc:
    _openai = None
    _IMPORT_ERROR = exc


def require_openai() -> ModuleType:
    """Return the OpenAI SDK module or raise a clear optional-dependency error."""

    if _openai is None:
        raise ModuleNotFoundError(
            "openai is required for weekly AI tuning commands. "
            "Install it with `python -m pip install openai`."
        ) from _IMPORT_ERROR
    return _openai
