from pathlib import Path
from typing import Dict


def build_data_dirs(data_root: str | Path) -> Dict[str, Path]:
    data_root = Path(data_root).expanduser().resolve()

    dirs = {
        "data_root": data_root,
        "raw": data_root / "raw",
        "interim": data_root / "interim",
        "processed": data_root / "processed",
        "logs": data_root / "logs",
        "pubmed_raw": data_root / "raw" / "pubmed",
    }

    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)

    return dirs