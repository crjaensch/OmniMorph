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
def _generate_avro_schema(table: pa.Table, sample_records: list) -> tuple:
    """Generate an Avro schema based on a PyArrow table and sample records.
    
    Args:
        table: PyArrow table
        sample_records: Sample of records to analyze for type inference
        
    Returns:
        tuple: (avro_schema, string_columns) where avro_schema is the generated schema
               and string_columns is a set of column names that should be converted to strings
    """
    # Analyze sample to determine actual types used in each column
    column_types = {field.name: set() for field in table.schema}
    for record in sample_records:
        for key, value in record.items():
            if value is not None:
                column_types[key].add(type(value).__name__)
    
    # Create schema based on sample data analysis
    avro_schema = {"type": "record", "name": "Root", "fields": []}
    string_columns = set()
    
    for field in table.schema:
        field_name = field.name
        types_in_column = column_types[field_name]
        
        # Default to the PyArrow type mapping
        field_type = _pyarrow_to_avro_type(field.type, field_name)
        
        # If we have mixed types or string type is present, use string for safety
        if len(types_in_column) > 1 or 'str' in types_in_column:
            # Only override if not a complex type (struct or array)
            if not isinstance(field_type, dict):
                field_type = ["null", "string"]
                string_columns.add(field_name)
        
        avro_schema["fields"].append({
            "name": field_name,
            "type": field_type
        })
    
    return avro_schema, string_columns


def _write_impl(table: pa.Table, path: Path, fmt: Format, **kwargs) -> None:
    """Write a PyArrow Table to a file."""
    if fmt is Format.AVRO:
        if not HAS_FASTAVRO:
            raise ImportError("Fastavro is not available. Please install fastavro to support Avro format.")
        
        # Sample a small portion of the data to determine schema
        sample_size = min(100, table.num_rows)
        sample_indices = list(range(sample_size))
        sample_table = table.take(sample_indices) if sample_size > 0 else table
        sample_records = sample_table.to_pylist()
        
        # Generate the Avro schema from the sample data
        avro_schema, string_columns = _generate_avro_schema(table, sample_records)
        
        # Define a generator function to process records incrementally
        def record_generator():
            # Process records in chunks to save memory
            chunk_size = 1000  # Adjust based on memory constraints
            for i in range(0, table.num_rows, chunk_size):
                end_idx = min(i + chunk_size, table.num_rows)
                chunk_indices = list(range(i, end_idx))
                chunk = table.take(chunk_indices)
                records_chunk = chunk.to_pylist()
                
                for record in records_chunk:
                    # Process each record for compatibility with Avro
                    for key, value in list(record.items()):
                        # Handle datetime objects
                        if hasattr(value, 'isoformat'):
                            record[key] = value.isoformat()
                        # Convert values in string_columns to strings
                        elif key in string_columns and value is not None:
                            record[key] = str(value)
                    yield record
        
        # Write Avro file using fastavro with the generator
        with open(path, 'wb') as fo:
            fastavro.writer(fo, avro_schema, record_generator())
    
    elif fmt is Format.PARQUET:
        papq.write_table(table, path, **kwargs)
    
    elif fmt is Format.CSV:
        # Create WriteOptions object with the provided kwargs
        write_options = pacsv.WriteOptions(**kwargs)
        pacsv.write_csv(table, path, write_options=write_options)
    
    elif fmt is Format.JSON:
        # PyArrow doesn't have a direct write_json function
        # Convert to list of records and write as JSON lines
        records = table.to_pylist()
        with open(path, 'w') as fo:
            import json
            for record in records:
                fo.write(json.dumps(record, default=str) + '\n')
    
    else:
        raise AssertionError("unreachable")


def _pyarrow_to_avro_type(pa_type: pa.DataType, field_path="") -> str:
    """Convert PyArrow type to Avro type.
    
    Args:
        pa_type: PyArrow data type
        field_path: Path to the field, used to create unique names for nested structures
    """
    if pa.types.is_null(pa_type):
        return "null"
    elif pa.types.is_boolean(pa_type):
        return "boolean"
    elif pa.types.is_integer(pa_type):
        # Allow for null values in integer fields
        return ["null", "long"]
    elif pa.types.is_floating(pa_type):
        # Allow for null values in float fields
        return ["null", "double"]
    elif pa.types.is_string(pa_type):
        # Allow for null values in string fields
        return ["null", "string"]
    elif pa.types.is_binary(pa_type):
        return ["null", "bytes"]
    elif pa.types.is_timestamp(pa_type) or pa.types.is_date(pa_type) or pa.types.is_time(pa_type):
        # Handle datetime types as strings in Avro, allow nulls
        return ["null", "string"]
    elif pa.types.is_list(pa_type):
        item_type = _pyarrow_to_avro_type(pa_type.value_type, field_path + "_item")
        return {"type": "array", "items": item_type}
    elif pa.types.is_struct(pa_type):
        # Create a unique name for this struct based on its path
        struct_name = field_path.lstrip("_") if field_path else "record"
        
        fields = []
        for i, field in enumerate(pa_type):
            # Create a unique path for nested fields
            nested_path = f"{field_path}_{field.name}" if field_path else field.name
            fields.append({
                "name": field.name,
                "type": _pyarrow_to_avro_type(field.type, nested_path)
            })
        return {"type": "record", "name": struct_name, "fields": fields}
    else:
        # Default to nullable string for unsupported types
        return ["null", "string"]
