"""
Format definitions for OmniMorph data handling.
"""
from __future__ import annotations

from enum import Enum, auto
from pathlib import Path
from typing import Union


class Format(str, Enum):
    AVRO = auto()
    PARQUET = auto()
    CSV = auto()
    JSON = auto()

    @classmethod
    def _missing_(cls, value):
        # accept lowercase strings like "avro"
        val = str(value).strip().lower()
        try:
            return cls[val.upper()]
        except KeyError:
            raise ValueError(f"Unrecognized format: {value!r}") from None

    # ---------- helpers ----------

    @staticmethod
    def from_path(path: Union[str, Path]) -> "Format":
        """Infer a Format from a filename extension."""
        ext = Path(path).suffix.lower().lstrip(".")
        mapping = {
            "avro": Format.AVRO,
            "parquet": Format.PARQUET,
            "pq": Format.PARQUET,
            "csv": Format.CSV,
            "json": Format.JSON,
            "ndjson": Format.JSON,
            "jsonl": Format.JSON,
        }
        try:
            return mapping[ext]
        except KeyError:
            raise ValueError(
                f"Cannot infer format from extension {ext!r}. "
                "Specify src_fmt/dst_fmt explicitly."
            ) from None
