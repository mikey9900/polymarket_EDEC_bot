"""Small runtime-default helpers that are safe to unit test in isolation."""

import os


def default_strategy_mode() -> str:
    return os.getenv("EDEC_DEFAULT_MODE", "both")
