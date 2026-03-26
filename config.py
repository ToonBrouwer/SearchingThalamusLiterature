from pathlib import Path
from typing import Any, Dict

import yaml


def load_yaml(path: str | Path) -> Dict[str, Any]:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_settings(path: str | Path = "config/settings.yaml") -> Dict[str, Any]:
    settings = load_yaml(path)

    if "data_root" not in settings:
        raise KeyError("`data_root` is missing from config/settings.yaml")

    return settings


def load_queries(path: str | Path = "config/queries.yaml") -> Dict[str, Any]:
    queries = load_yaml(path)

    if "queries" not in queries:
        raise KeyError("`queries` is missing from config/queries.yaml")

    return queries["queries"]


def get_query(query_name: str, path: str | Path = "config/queries.yaml") -> str:
    queries = load_queries(path)

    if query_name not in queries:
        available = ", ".join(sorted(queries.keys()))
        raise KeyError(f"Query '{query_name}' not found. Available queries: {available}")

    query = queries[query_name].get("query")
    if not query:
        raise ValueError(f"Query '{query_name}' has no `query` field.")

    return query