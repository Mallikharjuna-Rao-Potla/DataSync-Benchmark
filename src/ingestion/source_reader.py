"""Ingestion: read CSVs, fail loudly on unreadable files / bad schema, capture lineage."""
import hashlib
from pathlib import Path

import pandas as pd

from src.validation.schema_validator import validate_schema


class SourceError(Exception):
    """File missing, unreadable or malformed -> the whole batch fails."""


def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path, required_columns):
    path = Path(path)
    if not path.is_file():
        raise SourceError(f"Source file not found: {path}")
    if path.stat().st_size == 0:
        raise SourceError(f"Source file is empty (0 bytes): {path}")
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""],
                         on_bad_lines="error")
    except (pd.errors.ParserError, pd.errors.EmptyDataError, UnicodeDecodeError) as exc:
        raise SourceError(f"Malformed CSV {path.name}: {exc}") from exc
    validate_schema(df, required_columns, path.name)     # raises SchemaError
    df.attrs["source_file"] = path.name
    return df
