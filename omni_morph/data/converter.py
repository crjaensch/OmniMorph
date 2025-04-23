"""
omni_morph.data.converter
==============

A tiny, zero-dependency (outside PyArrow) helper that converts data between
Avro, Parquet, CSV and JSON.  Requires Python 3.12+ and PyArrow ≥ 15.

Public API
----------
• read(path, fmt=None) → pa.Table        - read file into a PyArrow Table  
• write(table, path, fmt=None, **kw)     - write table to file, returns *table*  
• convert(src, dst, *, src_fmt=None, dst_fmt=None) → pa.Table

Example
-------

>>> from omni_morph.data import convert
>>> convert("events.avro", "events.parquet")

Copyright © 2025 Christian R. Jaensch.  MIT license.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Union

import pyarrow as pa

from . import _io as _io
from .formats import Format

__all__ = ["Format", "read", "write", "convert"]


# ============================== public helpers ============================== #


def read(path: Union[str, Path], fmt: Optional[Format] = None, **kwargs) -> pa.Table:
    """Read data from a file into a PyArrow Table.
    
    This function reads data from the specified file path and returns it as a
    PyArrow Table. The format can be explicitly specified or inferred from the
    file extension.
    
    Args:
        path: A string or Path object pointing to the file to read.
        fmt: Optional format specification. If None, the format is inferred
             from the file extension.
        **kwargs: Additional keyword arguments passed to the underlying
                 PyArrow reader.
    
    Returns:
        A PyArrow Table containing the data from the file.
    
    Raises:
        ValueError: If the format cannot be inferred from the file extension
                   and no format is explicitly provided.
        IOError: If the file cannot be read or does not exist.
    """
    resolved_fmt = fmt or Format.from_path(path)
    return _io._read_impl(Path(path), resolved_fmt, **kwargs)


def write(
    table: pa.Table,
    path: Union[str, Path],
    fmt: Optional[Format] = None,
    **kwargs,
) -> pa.Table:
    """Write PyArrow Table to a file in the specified format.
    
    This function writes a PyArrow Table to the specified file path using the
    given format. The format can be explicitly specified or inferred from the
    file extension. The function returns the original table to allow for method
    chaining.
    
    Args:
        table: The PyArrow Table to write.
        path: A string or Path object pointing to the destination file.
        fmt: Optional format specification. If None, the format is inferred
             from the file extension.
        **kwargs: Additional keyword arguments passed to the underlying
                 PyArrow writer.
    
    Returns:
        The original PyArrow Table (for method chaining).
    
    Raises:
        ValueError: If the format cannot be inferred from the file extension
                   and no format is explicitly provided.
        IOError: If the file cannot be written to the specified path.
    """
    resolved_fmt = fmt or Format.from_path(path)
    _io._write_impl(table, Path(path), resolved_fmt, **kwargs)
    return table


def convert(
    src: Union[str, Path],
    dst: Union[str, Path],
    *,
    src_fmt: Optional[Format] = None,
    dst_fmt: Optional[Format] = None,
    read_kwargs: Optional[dict[str, Any]] = None,
    write_kwargs: Optional[dict[str, Any]] = None,
) -> pa.Table:
    """Convert a data file from one format to another.
    
    This function reads data from a source file and writes it to a destination
    file, potentially changing the format. Both source and destination formats
    can be explicitly specified or inferred from file extensions.
    
    Args:
        src: A string or Path object pointing to the source file.
        dst: A string or Path object pointing to the destination file.
        src_fmt: Optional source format specification. If None, the format is
                inferred from the source file extension.
        dst_fmt: Optional destination format specification. If None, the format
                is inferred from the destination file extension.
        read_kwargs: Optional dictionary of keyword arguments passed to the
                    underlying PyArrow reader.
        write_kwargs: Optional dictionary of keyword arguments passed to the
                     underlying PyArrow writer.
    
    Returns:
        A PyArrow Table containing the data that was written to the destination
        file (useful for inspection).
    
    Raises:
        ValueError: If the formats cannot be inferred from file extensions
                   and no formats are explicitly provided.
        IOError: If the source file cannot be read or the destination file
                cannot be written.
    """
    table = read(src, src_fmt, **(read_kwargs or {}))
    return write(table, dst, dst_fmt, **(write_kwargs or {}))
