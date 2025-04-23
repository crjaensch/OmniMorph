"""
Internal I/O helpers - kept separate so the public package namespace stays tidy.
"""
from __future__ import annotations

import io
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as papq

# Try to import fastavro for Avro support
try:
    import fastavro
    HAS_FASTAVRO = True
except ImportError:
    HAS_FASTAVRO = False
    # Define a placeholder that will raise a more helpful error when used
    class FastavroNotAvailable:
        @staticmethod
        def read_avro(*args, **kwargs):
            raise ImportError(
                "Fastavro is not available. "
                "Please install fastavro to support Avro format."
            )
        
        @staticmethod
        def write_avro(*args, **kwargs):
            raise ImportError(
                "Fastavro is not available. "
                "Please install fastavro to support Avro format."
            )
    
    fastavro = FastavroNotAvailable()

from .formats import Format

# --------------------------------------------------------------------------- #
# Readers
# --------------------------------------------------------------------------- #
def _read_impl(path: Path, fmt: Format, **kwargs) -> pa.Table:
    """Read a file into a PyArrow Table."""
    if fmt is Format.AVRO:
        if not HAS_FASTAVRO:
            raise ImportError("Fastavro is not available. Please install fastavro to support Avro format.")
        
        # Read Avro file using fastavro
        with open(path, 'rb') as fo:
            avro_reader = fastavro.reader(fo)
            records = [r for r in avro_reader]
        
        # Convert to PyArrow Table
        if records:
            return pa.Table.from_pylist(records)
        else:
            # Return empty table with schema from Avro file
            with open(path, 'rb') as fo:
                schema = fastavro.schema.load_schema(fo)
            pa_fields = []
            for field_name, field_type in schema.get('fields', {}).items():
                # Simple mapping of Avro types to PyArrow types
                # This is a simplified version and might need expansion
                if field_type == 'string':
                    pa_fields.append(pa.field(field_name, pa.string()))
                elif field_type in ('int', 'long'):
                    pa_fields.append(pa.field(field_name, pa.int64()))
                elif field_type in ('float', 'double'):
                    pa_fields.append(pa.field(field_name, pa.float64()))
                elif field_type == 'boolean':
                    pa_fields.append(pa.field(field_name, pa.bool_()))
                else:
                    pa_fields.append(pa.field(field_name, pa.string()))  # Default to string
            return pa.Table.from_arrays([], schema=pa.schema(pa_fields))
    
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
    """Write a PyArrow Table to a file."""
    if fmt is Format.AVRO:
        if not HAS_FASTAVRO:
            raise ImportError("Fastavro is not available. Please install fastavro to support Avro format.")
        
        # Convert PyArrow Table to list of Python dictionaries
        records = table.to_pylist()
        
        # Infer Avro schema from PyArrow schema
        avro_schema = {"type": "record", "name": "Root", "fields": []}
        for field in table.schema:
            field_type = _pyarrow_to_avro_type(field.type)
            avro_schema["fields"].append({
                "name": field.name,
                "type": field_type
            })
        
        # Write Avro file using fastavro
        with open(path, 'wb') as fo:
            fastavro.writer(fo, avro_schema, records)
    
    elif fmt is Format.PARQUET:
        papq.write_table(table, path, **kwargs)
    
    elif fmt is Format.CSV:
        pacsv.write_csv(table, path, **kwargs)
    
    elif fmt is Format.JSON:
        # PyArrow doesn't have a direct write_json function
        # Convert to list of records and write as JSON lines
        records = table.to_pylist()
        with open(path, 'w') as fo:
            import json
            for record in records:
                fo.write(json.dumps(record) + '\n')
    
    else:
        raise AssertionError("unreachable")


def _pyarrow_to_avro_type(pa_type: pa.DataType) -> str:
    """Convert PyArrow type to Avro type."""
    if pa.types.is_boolean(pa_type):
        return "boolean"
    elif pa.types.is_integer(pa_type):
        return "long"
    elif pa.types.is_floating(pa_type):
        return "double"
    elif pa.types.is_string(pa_type):
        return "string"
    elif pa.types.is_binary(pa_type):
        return "bytes"
    elif pa.types.is_list(pa_type):
        return {"type": "array", "items": _pyarrow_to_avro_type(pa_type.value_type)}
    elif pa.types.is_struct(pa_type):
        fields = []
        for field in pa_type:
            fields.append({
                "name": field.name,
                "type": _pyarrow_to_avro_type(field.type)
            })
        return {"type": "record", "name": "struct", "fields": fields}
    else:
        # Default to string for unsupported types
        return "string"
