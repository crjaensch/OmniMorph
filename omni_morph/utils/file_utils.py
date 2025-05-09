"""
file_utils.py

Utilities for extracting schemas from data files.
Supports Parquet, Avro, JSON, and CSV formats.
"""

import json
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Optional, Union
import os
import datetime as _dt
from omni_morph.data.formats import Format
from omni_morph.data.filesystems import FileSystemHandler
from omni_morph.utils._csv_schema import infer_csv_schema
from omni_morph.data.exceptions import ExtractError


import pyarrow.parquet as pq
import fastavro
from genson import SchemaBuilder

def _infer_json_schema(filepath: str):
    """
    Infer JSON schema: handles both JSON and JSONL (first record) using GenSON.
    
    Supports both local paths and cloud URLs (Azure ADLS Gen2).
    """
    builder = SchemaBuilder()
    with FileSystemHandler.open_file(filepath, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except JSONDecodeError:
            f.seek(0)
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    break
            else:
                raise ValueError(f"No JSON records found in {filepath}")
    builder.add_object(data)
    return builder.to_schema()

def get_schema(filepath: str, fmt: Format = None):
    """
    Extract the schema from a data file.

    Args:
        filepath (str): Path to the data file.
        fmt (Format, optional): Override format inference. Supported: 'parquet', 'avro', 'json', 'csv'.

    Returns:
        The extracted schema. Type depends on format:
        - Parquet: dict representing schema
        - Avro: dict representing schema
        - JSON: dict representing JSON Schema
        - CSV: dict representing inferred schema

    Raises:
        ValueError: If format is unsupported or cannot be inferred.
    """
    # ---------- format ------------------------------------------------------
    resolved_fmt = fmt or Format.from_path(filepath)
    if isinstance(resolved_fmt, str):
        resolved_fmt = Format(resolved_fmt)

    if resolved_fmt == Format.PARQUET:
        schema = pq.read_schema(filepath)
        # Convert pyarrow.Schema to JSON-serializable dict
        return {"fields": [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in schema]}
    if resolved_fmt == Format.AVRO:
        with FileSystemHandler.open_file(filepath, 'rb') as fo:
            reader = fastavro.reader(fo)
            return reader.schema
    if resolved_fmt == Format.JSON:
        return _infer_json_schema(filepath)
    if resolved_fmt == Format.CSV:
        try:
            return infer_csv_schema(filepath)
        except Exception as e:
            raise ValueError(f"CSV schema inference failed: {str(e)}")

    raise ValueError(
        f"Unsupported format {resolved_fmt!r}. Supported formats: parquet, avro, json, csv."
    )

# ---------------------------------------------------------------------------
#  METADATA HELPER  ----------------------------------------------------------
# ---------------------------------------------------------------------------

def get_metadata(
    filepath: str,
    fmt: Optional[Format] = None,
    *,
    sample_bytes: int = 32_768,
    small_file_threshold: int = 100 * 1024 * 1024,
    detect_encoding: bool = True,
) -> dict[str, Any]:
    """
    Return metadata for the given file.

    Args:
        filepath (str): Path to the data file (local path or cloud URL).
        fmt (Format, optional): Override format inference. Supported: 'parquet', 'avro', 'json', 'csv'. Defaults to None.
        sample_bytes (int, optional): Number of bytes to sample for encoding detection. Defaults to 32_768.
        small_file_threshold (int, optional): Threshold in bytes for treating a file as small. Defaults to 100 * 1024 * 1024.
        detect_encoding (bool, optional): Whether to detect encoding for text formats. Defaults to True.

    Returns:
        dict[str, Any]: A dict with keys 'file_size', 'created', 'modified', 'encoding', 'num_records', 'format'.

    Raises:
        ExtractError: If filepath does not exist or is not a regular file.
        ValueError: If the file format is unsupported.
    """
    # Check if file exists
    if not FileSystemHandler.exists(filepath):
        raise ExtractError(f"{filepath!r} does not exist or is not a regular file.")

    # ---------- filesystem --------------------------------------------------
    # Get file info from the appropriate filesystem
    file_info = FileSystemHandler.get_file_info(filepath)
    file_size = file_info.get('size', 0)
    
    # Try to get timestamps (may not be available for all filesystems)
    created = file_info.get('created', None)
    modified = file_info.get('mtime', None)
    
    # Convert to datetime objects if available
    if created is not None:
        created = _dt.datetime.fromtimestamp(created, _dt.timezone.utc)
    else:
        # Default to current time if not available
        created = _dt.datetime.now(_dt.timezone.utc)
        
    if modified is not None:
        modified = _dt.datetime.fromtimestamp(modified, _dt.timezone.utc)
    else:
        # Default to current time if not available
        modified = _dt.datetime.now(_dt.timezone.utc)

    # ---------- format ------------------------------------------------------
    resolved_fmt = fmt or Format.from_path(filepath)
    if isinstance(resolved_fmt, str):
        resolved_fmt = Format(resolved_fmt)

    # ---------- encoding ----------------------------------------------------
    # CSV/JSON: detect encoding; Parquet/Avro: binary
    if resolved_fmt in {Format.CSV, Format.JSON} and detect_encoding:
        encoding = _guess_encoding(filepath, sample_bytes)
    elif resolved_fmt in {Format.PARQUET, Format.AVRO}:
        encoding = "binary"
    else:
        encoding = None

    # ---------- record count ------------------------------------------------
    if resolved_fmt == Format.PARQUET:
        num_records = pq.ParquetFile(filepath).metadata.num_rows
    elif resolved_fmt == Format.AVRO:
        num_records = _count_avro(filepath, small_file_threshold)
    elif resolved_fmt == Format.JSON:
        num_records = _count_lines(filepath, encoding or "utf-8", small_file_threshold)
    elif resolved_fmt == Format.CSV:
        num_records = _count_csv_rows(filepath, encoding or "utf-8", small_file_threshold)
    else:
        raise ExtractError(f"Unsupported format {resolved_fmt!r}")

    return {
        "file_size": file_size,
        "created": created,
        "modified": modified,
        "encoding": encoding,
        "num_records": num_records,
        "format": resolved_fmt.name.lower(),  # represent format as lowercase string
    }


# ---------------------------------------------------------------------------
#  Helpers for meta()  -------------------------------------------------------
# ---------------------------------------------------------------------------

def _guess_encoding(filepath: str, sample_bytes: int) -> str:
    """
    Best-effort encoding sniff (defaults to UTF-8).
    
    Supports both local paths and cloud URLs (Azure ADLS Gen2).
    """
    try:
        import chardet
    except ImportError:
        return "utf-8"

    with FileSystemHandler.open_file(filepath, "rb") as fh:
        raw = fh.read(sample_bytes)
    res = chardet.detect(raw)
    return res["encoding"] or "utf-8"


def _count_avro(path: str, limit: int) -> int:
    """
    Count records in an Avro data file without loading it fully into RAM.
    
    Supports both local paths and cloud URLs (Azure ADLS Gen2).
    """
    # Get file info to check size
    file_info = FileSystemHandler.get_file_info(path)
    size = file_info.get('size', 0)
    
    if size < limit:  # small file → load once
        with FileSystemHandler.open_file(path, "rb") as fo:
            return sum(1 for _ in fastavro.reader(fo))

    # streaming count, block by block
    cnt = 0
    with FileSystemHandler.open_file(path, "rb") as fo:
        for rec in fastavro.reader(fo):
            cnt += 1
    return cnt


def _count_lines(path: str, encoding: str, limit: int) -> int:
    """
    Fast line count for JSONL.
    
    Supports both local paths and cloud URLs (Azure ADLS Gen2).
    """
    # Get file info to check size
    file_info = FileSystemHandler.get_file_info(path)
    size = file_info.get('size', 0)
    
    count = 0
    with FileSystemHandler.open_file(path, "r", encoding=encoding) as fh:
        if size < limit:
            for line in fh:
                if line.strip():
                    count += 1
        else:
            # For large files, sample first N lines
            # to estimate average line length
            sample_size = 1000
            sample_lines = []
            for i, line in enumerate(fh):
                if i >= sample_size:
                    break
                if line.strip():
                    sample_lines.append(line)
            
            if not sample_lines:
                return 0  # Empty file or no valid lines
                
            # Calculate average line length from sample
            avg_line_length = sum(len(line) for line in sample_lines) / len(sample_lines)
            
            # Estimate total lines based on file size and average line length
            # This is an approximation and might not be 100% accurate
            estimated_lines = int(size / avg_line_length)
            
            # Add count from sample
            count = len(sample_lines)
            
            # If we've read less than 10% of the file, extrapolate
            if sample_size * avg_line_length < size * 0.1:
                return estimated_lines
            
            # Otherwise continue counting the rest
            for line in fh:
                if line.strip():
                    count += 1
    
    return count


def _count_csv_rows(path: str, encoding: str, limit: int) -> int:
    """
    Counts data rows (excludes header).
    
    Supports both local paths and cloud URLs (Azure ADLS Gen2).
    """
    # Just reuse line counter but subtract 1 for header
    total = _count_lines(path, encoding, limit)
    return max(0, total - 1)  # Ensure we don't return negative count
