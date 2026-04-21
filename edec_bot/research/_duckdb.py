"""Helpers for the optional DuckDB research dependency."""

from __future__ import annotations

from types import ModuleType


_IMPORT_ERROR: ModuleNotFoundError | None = None

try:
    import duckdb as _duckdb
except ModuleNotFoundError as exc:
    _duckdb = None
    _IMPORT_ERROR = exc


def require_duckdb() -> ModuleType:
    """Return the DuckDB module or raise a clear optional-dependency error."""

    if _duckdb is None:
        raise ModuleNotFoundError(
            "duckdb is required for research warehouse commands and artifact generation. "
            "Install it with `python -m pip install duckdb`."
        ) from _IMPORT_ERROR
    return _duckdb
