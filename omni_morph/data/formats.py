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
        """
        Infer a Format from a filename extension.
        
        Supports both local paths and cloud URLs (Azure ADLS Gen2).
        
        Args:
            path: A string or Path object pointing to a file. Can be a local path or cloud URL.
            
        Returns:
            Format enum value based on the file extension.
            
        Raises:
            ValueError: If the format cannot be inferred from the file extension.
        """
        path_str = str(path)
        
        # Handle Azure ADLS Gen2 URLs
        if path_str.startswith(('abfss://', 'abfs://')):
            # Extract the file name from the URL
            # Example: abfss://container@account.dfs.core.windows.net/path/to/file.csv
            # We need to extract 'file.csv'
            file_name = path_str.split('/')[-1]
            ext = Path(file_name).suffix.lower().lstrip(".")
        else:
            ext = Path(path_str).suffix.lower().lstrip(".")
            
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
