"""
Internal I/O helpers - kept separate so the public package namespace stays tidy.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as papq

# Try to import Avro support, but don't fail if it's not available
try:
    import pyarrow.avro as paavro
    HAS_AVRO = True
except ImportError:
    HAS_AVRO = False
    # Define a placeholder that will raise a more helpful error when used
    class AvroNotAvailable:
        @staticmethod
        def read_avro(*args, **kwargs):
            raise ImportError(
                "PyArrow Avro support is not available. "
                "Please install a version of PyArrow that includes Avro support."
            )
        
        @staticmethod
        def write_avro(*args, **kwargs):
            raise ImportError(
                "PyArrow Avro support is not available. "
                "Please install a version of PyArrow that includes Avro support."
            )
    
    paavro = AvroNotAvailable()

from .formats import Format

# --------------------------------------------------------------------------- #
# Readers
# --------------------------------------------------------------------------- #
def _read_impl(path: Path, fmt: Format, **kwargs) -> pa.Table:
    if fmt is Format.AVRO:
        return paavro.read_avro(path, **kwargs)
    elif fmt is Format.PARQUET:
        return papq.read_table(path, **kwargs)
    elif fmt is Format.CSV:
        return pacsv.read_csv(path, **kwargs)
    elif fmt is Format.JSON:
        return pajson.read_json(path, **kwargs)
    else:
        raise AssertionError("unreachable")


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #
def _write_impl(table: pa.Table, path: Path, fmt: Format, **kwargs) -> None:
    if fmt is Format.AVRO:
        paavro.write_avro(path, table, **kwargs)
    elif fmt is Format.PARQUET:
        papq.write_table(table, path, **kwargs)
    elif fmt is Format.CSV:
        pacsv.write_csv(table, path, **kwargs)
    elif fmt is Format.JSON:
        pajson.write_json(table, path, **kwargs)
    else:
        raise AssertionError("unreachable")
