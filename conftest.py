collect_ignore = []

try:
    import duckdb  # noqa: F401
except ImportError:
    collect_ignore += [
        "tests/test_research_artifacts.py",
        "tests/test_research_sync.py",
    ]
