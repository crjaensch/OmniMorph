"""
omni_morph.data.converter
==============

A tiny, zero-dependency (outside PyArrow) helper that converts data between
Avro, Parquet, CSV and JSON.  Requires Python 3.12+ and PyArrow ≥ 15.

Public API
----------

• Format enum                            - canonical names & helpers  
• read(path, fmt=None) → pa.Table        - read file into a PyArrow Table  
• write(table, path, fmt=None, **kw)     - write table to file, returns *table*  
• convert(src, dst, *, src_fmt=None, dst_fmt=None) → pa.Table

Example
-------

>>> from omni_morph.data.converter import convert
>>> convert("events.avro", "events.parquet")

Copyright © 2025 Christian R. Jaensch.  MIT license.
"""
from __future__ import annotations

from enum import Enum, auto
from pathlib import Path
from typing import Any, Optional, Union

import pyarrow as pa

from . import _io as _io
from .formats import Format

__all__ = ["Format", "read", "write", "convert"]


# ============================== public helpers ============================== #


def read(path: Union[str, Path], fmt: Optional[Format] = None, **kwargs) -> pa.Table:
    """
    Read *path* into a **pyarrow.Table**.

    Parameters
    ----------
    path : str | Path
    fmt  : Format | None
        Omit to infer from filename.
    kwargs : dict
        Passed to the underlying pyarrow reader.
    """
    resolved_fmt = fmt or Format.from_path(path)
    return _io._read_impl(Path(path), resolved_fmt, **kwargs)


def write(
    table: pa.Table,
    path: Union[str, Path],
    fmt: Optional[Format] = None,
    **kwargs,
) -> pa.Table:
    """
    Write *table* to *path* in *fmt* and return the table (for chaining)."""
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
    """
    Convert *src* file into *dst* file.

    Returns the pyarrow.Table that was written (handy for inspection).

    Any extra keyword arguments can be supplied through *read_kwargs* /
    *write_kwargs* and are passed straight through to PyArrow.
    """
    table = read(src, src_fmt, **(read_kwargs or {}))
    return write(table, dst, dst_fmt, **(write_kwargs or {}))
