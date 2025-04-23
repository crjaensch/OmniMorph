"""
extractor.py  ── unified head / tail extraction function for Parquet, Avro, JSONL and CSV

Public API
==========

    from omni_morph.data.extractor import head, tail, ExtractError

    tbl   = head("events.parquet", 100)               # pyarrow.Table
    frame = tail("logs.jsonl",  20, return_type="pandas")

---------------------------------------------------------------------------
"""

from __future__ import annotations
import os
from enum import Enum
from typing import Literal, Optional, Union
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from collections import deque

try:
    from fastavro import reader as avro_reader
except ImportError:  # fastavro is optional until Avro is actually used
    avro_reader = None

from .formats import Format

__all__ = [
    "head",
    "tail",
    "ExtractError",
]

# ---------------------------------------------------------------------------
# Exceptions & internal enums
# ---------------------------------------------------------------------------

class ExtractError(RuntimeError):
    """Raised when an extract operation cannot be completed."""


class _Operation(str, Enum):
    HEAD = "head"
    TAIL = "tail"

    @classmethod
    def coerce(cls, value: str | "_Operation") -> "_Operation":
        if isinstance(value, cls):
            return value
        val = str(value).lower()
        if val not in cls._value2member_map_:
            raise ValueError(f"operation must be 'head' or 'tail', not {value!r}")
        return cls(val)


# ---------------------------------------------------------------------------
# Public helpers – the simplified interface you requested
# ---------------------------------------------------------------------------

def head(
    path: str,
    n: int,
    fmt: Optional[Format] = None,
    *,
    return_type: Literal["arrow", "pandas"] = "arrow",
    small_file_threshold: int = 100 * 1024 * 1024,  # 100 MB
) -> pa.Table | pd.DataFrame:
    """
    Return the first n records from a file.
    
    This function reads the first n records from the specified file path and returns
    them as either a PyArrow Table or Pandas DataFrame. The format can be explicitly
    specified or inferred from the file extension.
    
    Args:
        path: A string pointing to the file to read.
        n: Number of records to return from the beginning of the file.
        fmt: Optional format specification. If None, the format is inferred
             from the file extension.
        return_type: The return type, either "arrow" for PyArrow Table or
                    "pandas" for Pandas DataFrame.
        small_file_threshold: Size threshold in bytes for determining if a file
                             is small enough to read entirely into memory.
    
    Returns:
        A PyArrow Table or Pandas DataFrame containing the first n records.
    
    Raises:
        ValueError: If n is not positive or if the format cannot be inferred.
        ExtractError: If an error occurs during extraction.
    """
    return _extract_records(
        path,
        n,
        _Operation.HEAD,
        fmt=fmt,
        return_type=return_type,
        small_file_threshold=small_file_threshold,
    )


def tail(
    path: str,
    n: int,
    fmt: Optional[Format] = None,
    *,
    return_type: Literal["arrow", "pandas"] = "arrow",
    small_file_threshold: int = 100 * 1024 * 1024,  # 100 MB
) -> pa.Table | pd.DataFrame:
    """
    Return the last n records from a file.
    
    This function reads the last n records from the specified file path and returns
    them as either a PyArrow Table or Pandas DataFrame. The format can be explicitly
    specified or inferred from the file extension.
    
    Args:
        path: A string pointing to the file to read.
        n: Number of records to return from the end of the file.
        fmt: Optional format specification. If None, the format is inferred
             from the file extension.
        return_type: The return type, either "arrow" for PyArrow Table or
                    "pandas" for Pandas DataFrame.
        small_file_threshold: Size threshold in bytes for determining if a file
                             is small enough to read entirely into memory.
    
    Returns:
        A PyArrow Table or Pandas DataFrame containing the last n records.
    
    Raises:
        ValueError: If n is not positive or if the format cannot be inferred.
        ExtractError: If an error occurs during extraction.
    """
    return _extract_records(
        path,
        n,
        _Operation.TAIL,
        fmt=fmt,
        return_type=return_type,
        small_file_threshold=small_file_threshold,
    )


# ---------------------------------------------------------------------------
# Core engine – kept exactly as before (only _Operation renamed)
# ---------------------------------------------------------------------------

def _extract_records(
    path: str,
    n: int,
    operation: str | _Operation,
    fmt: Optional[Format] = None,
    *,
    return_type: Literal["arrow", "pandas"] = "arrow",
    small_file_threshold: int = 100 * 1024 * 1024,  # 100 MB
) -> pa.Table | pd.DataFrame:
    """
    Internal work-horse that the `head`/`tail` helpers delegate to.

    Users normally shouldn’t need to call this directly.
    """
    if n <= 0:
        raise ValueError("n must be positive")

    op = _Operation.coerce(operation)
    resolved_fmt = fmt or Format.from_path(path)

    # Define format-specific thresholds
    parquet_large_threshold = 10 * 1024 * 1024 * 1024  # 10GB for Parquet
    avro_large_threshold = 1 * 1024 * 1024 * 1024      # 1GB for Avro

    try:
        if resolved_fmt is Format.PARQUET:
            table = _parquet_extract(path, n, op, parquet_large_threshold)
        elif resolved_fmt is Format.AVRO:
            table = _avro_extract(path, n, op, avro_large_threshold)
        elif resolved_fmt is Format.JSON:
            table = _jsonl_extract(path, n, op, small_file_threshold)
        elif resolved_fmt is Format.CSV:
            table = _csv_extract(path, n, op, small_file_threshold)
        else:
            raise ExtractError(f"Unsupported format {resolved_fmt!r}")
    except Exception as exc:
        raise ExtractError(
            f"{op.value}({n}) failed for {os.path.basename(path)!r}: {exc}"
        ) from exc

    return table if return_type == "arrow" else table.to_pandas()


# ---------------------------------------------------------------------------
# Per-format implementations (unchanged)
# ---------------------------------------------------------------------------

def _parquet_extract(path: str, n: int, op: _Operation, limit: int) -> pa.Table:
    pfile = pq.ParquetFile(path)
    file_size = os.path.getsize(path)
    
    # For extremely large files, use more aggressive row group selection
    aggressive_mode = file_size > limit
    
    if op is _Operation.HEAD:
        # For very large files, read row groups one at a time
        if aggressive_mode:
            rows_collected = 0
            tables = []
            for rg in range(pfile.num_row_groups):
                table = pfile.read_row_group(rg)
                rows_collected += len(table)
                tables.append(table)
                if rows_collected >= n:
                    break
            return pa.concat_tables(tables).slice(0, n) if tables else pa.table({})
        else:
            return pfile.read_row_groups(range(pfile.num_row_groups)).slice(0, n)
    
    # -- tail: collect enough row-groups from the end ------------------------
    if aggressive_mode:
        # For large files, be more selective about which row groups to read
        rows_needed = n
        groups = []
        total_rows = sum(pfile.metadata.row_group(rg).num_rows for rg in range(pfile.num_row_groups))
        
        # Estimate which row group to start with based on total rows and rows needed
        # This helps avoid scanning all row groups for very large files
        start_group = max(0, pfile.num_row_groups - (pfile.num_row_groups * n // total_rows) - 1)
        
        for rg in range(pfile.num_row_groups - 1, start_group - 1, -1):
            row_count = pfile.metadata.row_group(rg).num_rows
            rows_needed -= row_count
            groups.append(rg)
            if rows_needed <= 0:
                break
        
        groups.reverse()
        tables = [pfile.read_row_group(rg) for rg in groups]
        tbl = pa.concat_tables(tables) if tables else pa.table({})
        return tbl.slice(max(0, len(tbl) - n), n)
    else:
        # Implementation for smaller files
        rows_needed = n
        groups = []
        for rg in reversed(range(pfile.num_row_groups)):
            rows_needed -= pfile.metadata.row_group(rg).num_rows
            groups.append(rg)
            if rows_needed <= 0:
                break

        groups.reverse()
        tbl = pfile.read_row_groups(groups)
        return tbl.slice(len(tbl) - n, n)


def _avro_extract(path: str, n: int, op: _Operation, limit: int) -> pa.Table:
    if avro_reader is None:
        raise ImportError("fastavro is required for Avro support (`pip install fastavro`).")

    file_size = os.path.getsize(path)
    large_file = file_size > limit

    # For head operations, the current implementation is already efficient
    # as it only reads the first n records
    if op is _Operation.HEAD:
        with open(path, "rb") as fo:
            data = [record for _, record in zip(range(n), avro_reader(fo))]
        return pa.Table.from_pylist(data) if data else pa.table({})
    
    # For tail operations on large files, we need a more memory-efficient approach
    if large_file:
        return _avro_extract_tail_large_file(path, n)
    else:
        # Implementation for smaller files
        with open(path, "rb") as fo:
            buf: deque = deque(maxlen=n)
            for record in avro_reader(fo):
                buf.append(record)
            data = list(buf)
        return pa.Table.from_pylist(data) if data else pa.table({})


def _avro_extract_tail_large_file(path: str, n: int) -> pa.Table:
    """
    Memory-efficient extraction of the last n records from a large Avro file.
    
    This uses a two-pass approach:
    1. First pass: Count total records and estimate record size
    2. Second pass: Seek to an estimated position and read records
    """
    if avro_reader is None:
        raise ImportError("fastavro is required for Avro support (`pip install fastavro`).")
    
    # First pass: Sample the file to estimate record count and size
    file_size = os.path.getsize(path)
    
    # Sample at most 1000 records or 10MB to estimate average record size
    sample_size = min(10 * 1024 * 1024, file_size // 10)  # 10MB or 10% of file, whichever is smaller
    
    record_count = 0
    bytes_read = 0
    start_pos = 0
    
    # Read a sample from the beginning to estimate record size
    with open(path, "rb") as fo:
        reader = avro_reader(fo)
        # Get the reader's schema and sync marker for later use
        schema = reader.schema
        
        # Sample records to estimate average size
        for i, _ in enumerate(reader):
            record_count += 1
            if i >= 1000:  # Limit sample to 1000 records
                break
        
        # Get current position to estimate bytes read
        bytes_read = fo.tell()
    
    # If we couldn't read any records, return empty table
    if record_count == 0:
        return pa.table({})
    
    # Estimate average record size and total record count
    avg_record_size = bytes_read / record_count
    estimated_total_records = int(file_size / avg_record_size)
    
    # If we need more records than estimated total, just use the original approach
    if n >= estimated_total_records:
        with open(path, "rb") as fo:
            buf: deque = deque(maxlen=n)
            for record in avro_reader(fo):
                buf.append(record)
            data = list(buf)
        return pa.Table.from_pylist(data) if data else pa.table({})
    
    # Second pass: Seek to an estimated position and read records
    # We'll aim to start reading at a position where we expect to find the last n records
    # We'll add a safety margin to ensure we don't miss records
    safety_margin = min(n * 2, estimated_total_records // 4)  # Double the records or 25% of total, whichever is smaller
    records_to_skip = max(0, estimated_total_records - n - safety_margin)
    
    # Calculate approximate byte position to start reading
    # We can't seek directly in Avro, so we'll read and discard records
    start_pos = int(records_to_skip * avg_record_size)
    
    # Adjust start position to ensure we don't start beyond file size
    start_pos = min(start_pos, file_size - 1000)  # Ensure at least 1KB at the end
    
    # Read from the estimated position to the end
    buf: deque = deque(maxlen=n)
    with open(path, "rb") as fo:
        # We need to start from the beginning and skip records
        # This is unavoidable with Avro's sequential format
        reader = avro_reader(fo)
        
        # Skip records until we reach our target position
        skipped = 0
        for _ in reader:
            skipped += 1
            if skipped >= records_to_skip:
                break
        
        # Now collect the remaining records
        for record in reader:
            buf.append(record)
    
    data = list(buf)
    return pa.Table.from_pylist(data) if data else pa.table({})


def _jsonl_extract(path: str, n: int, op: _Operation, limit: int) -> pa.Table:
    import json

    if op is _Operation.HEAD:
        with open(path, "r", encoding="utf8") as fh:
            rows = [json.loads(line) for _, line in zip(range(n), fh)]
    else:
        rows = _tail_lines(path, n)
        rows = [json.loads(line) for line in rows]

    return pa.Table.from_pylist(rows) if rows else pa.table({})


def _csv_extract(path: str, n: int, op: _Operation, limit: int) -> pa.Table:
    if op is _Operation.HEAD:
        df = pd.read_csv(path, nrows=n)
    else:
        size = os.path.getsize(path)
        if size > limit:
            lines = _tail_lines(path, n + 1)         # preserve header
            header = _read_csv_header(path)
            from io import StringIO
            csv_chunk = "\n".join([header] + lines[-n:])
            df = pd.read_csv(StringIO(csv_chunk))
        else:
            df = pd.read_csv(path).tail(n)

    return pa.Table.from_pandas(df, preserve_index=False)


# ---------------------------------------------------------------------------
# Utility helpers (unchanged)
# ---------------------------------------------------------------------------

def _tail_lines(path: str, n: int, block_size: int = 8192) -> list[str]:
    end = os.path.getsize(path)
    lines: deque = deque()
    with open(path, "rb") as fh:
        while end > 0 and len(lines) < n:
            start = max(0, end - block_size)
            fh.seek(start)
            chunk = fh.read(end - start)
            lines.extendleft(chunk.decode("utf8").splitlines())
            end = start
    return list(reversed([ln for ln in lines if ln]))[-n:]


def _read_csv_header(path: str) -> str:
    with open(path, "r", encoding="utf8") as fh:
        return fh.readline().rstrip("\n")


# ---------------------------------------------------------------------------
# Quick CLI-like demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(head("example.parquet", 5).to_pandas())
    print(tail("example.csv", 10, return_type="pandas"))