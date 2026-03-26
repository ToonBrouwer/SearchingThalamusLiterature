from pathlib import Path
from typing import Any, Dict

import yaml


# 🔥 Base directory = where this file (config.py) lives
BASE_DIR = Path(__file__).resolve().parent


def load_yaml(path: str | Path) -> Dict[str, Any]:
    path = Path(path)

    # If relative path → resolve from BASE_DIR
    if not path.is_absolute():
        path = BASE_DIR / path

    if not path.exists():
        raise FileNotFoundError(f"Could not find file: {path}")

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_settings(path: str | Path = "settings.yml") -> Dict[str, Any]:
    settings = load_yaml(path)

    if "data_root" not in settings:
        raise KeyError("`data_root` is missing from settings.yml")

    return settings


def load_queries(path: str | Path = "queries.yml") -> Dict[str, Any]:
    queries = load_yaml(path)

    if "queries" not in queries:
        raise KeyError("`queries` is missing from queries.yml")

    return queries["queries"]


def get_query(query_name: str, path: str | Path = "queries.yml") -> str:
    queries = load_queries(path)

    if query_name not in queries:
        available = ", ".join(sorted(queries.keys()))
        raise KeyError(
            f"Query '{query_name}' not found. Available queries: {available}"
        )

    query = queries[query_name].get("query")

    if not query:
        raise ValueError(f"Query '{query_name}' has no 'query' field.")

    return query