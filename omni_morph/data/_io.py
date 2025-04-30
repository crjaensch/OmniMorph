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

import pandas as pd

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
def _read_impl(path: Path, fmt: Format, schema: pa.Schema = None, **kwargs) -> pa.Table:
    """Read a file into a PyArrow Table."""
    if fmt is Format.AVRO:
        if not HAS_FASTAVRO:
            raise ImportError("Fastavro is not available. Please install fastavro to support Avro format.")
        
        # Refactored Avro reading for robust null handling
        return _read_avro(path, schema=schema, **kwargs)

    elif fmt is Format.PARQUET:
        # Check for empty file and use schema if provided
        if path.stat().st_size == 0 and schema is not None:
            # Create empty typed arrays based on schema
            empty_arrays = [pa.array([], type=field.type) for field in schema]
            return pa.Table.from_arrays(empty_arrays, schema=schema)
        return papq.read_table(path, **kwargs)
    
    elif fmt is Format.JSON:
        # Handle potential empty file issue for JSON
        if path.stat().st_size == 0:
            # Return empty table with schema if provided, else simplest empty table
            if schema:
                empty_arrays = [pa.array([], type=field.type) for field in schema]
                return pa.Table.from_arrays(empty_arrays, schema=schema)
            else:
                 return pa.Table.from_arrays([], names=[])
        # Assume JSON Lines format
        return pajson.read_json(path, **kwargs)
    
    elif fmt is Format.CSV:
        # Handle potential empty file issue for CSV
        if path.stat().st_size == 0:
             # Return empty table with schema if provided, else let read_csv handle (might error)
            if schema:
                 # Create empty typed arrays based on schema
                 empty_arrays = [pa.array([], type=field.type) for field in schema]
                 return pa.Table.from_arrays(empty_arrays, schema=schema)
             # else fall through to read_csv which might raise error on empty file depending on options
        return pacsv.read_csv(path, **kwargs)
    
    else:
        raise AssertionError("unreachable")


CHUNK_SIZE = 10000 # Default chunk size for reading large files
def _read_avro(path: Path, schema: pa.Schema = None, chunk_size: int = CHUNK_SIZE, **kwargs) -> pa.Table:
    """Read an Avro file into a PyArrow Table, processing in chunks for memory efficiency."""
    if not HAS_FASTAVRO:
        raise ImportError("Fastavro is not available. Please install fastavro to support Avro format.")

    table_chunks = []
    avro_pa_schema = schema
    records_chunk = []

    try:
        with open(path, 'rb') as fo:
            reader = fastavro.reader(fo)
            avro_schema_dict = reader.writer_schema

            # Convert Avro schema to PyArrow schema once if needed
            if not avro_pa_schema and avro_schema_dict:
                try:
                    avro_pa_schema = _avro_to_pyarrow_schema(avro_schema_dict)
                except Exception as e:
                    # Log warning, proceed without specific schema
                    print(f"Warning: Could not convert Avro schema to PyArrow schema: {e}")
                    avro_pa_schema = None # Fallback

            for i, record in enumerate(reader):
                records_chunk.append(record)
                if (i + 1) % chunk_size == 0:
                    if records_chunk:
                        df_chunk = pd.DataFrame.from_records(records_chunk)
                        try:
                            table_chunk = pa.Table.from_pandas(df_chunk, schema=avro_pa_schema, **kwargs)
                        except pa.ArrowTypeError as e:
                             print(f"Warning: Schema mismatch converting Pandas chunk to Arrow: {e}. Trying without explicit schema.")
                             table_chunk = pa.Table.from_pandas(df_chunk, **kwargs)
                        table_chunks.append(table_chunk)
                        records_chunk = [] # Reset chunk

            # Process the last partial chunk if any records remain
            if records_chunk:
                 df_chunk = pd.DataFrame.from_records(records_chunk)
                 try:
                     table_chunk = pa.Table.from_pandas(df_chunk, schema=avro_pa_schema, **kwargs)
                 except pa.ArrowTypeError as e:
                     print(f"Warning: Schema mismatch converting Pandas chunk to Arrow: {e}. Trying without explicit schema.")
                     table_chunk = pa.Table.from_pandas(df_chunk, **kwargs)
                 table_chunks.append(table_chunk)

    except StopIteration: # Handle case where fastavro.reader yields nothing for empty file
        # If the file was empty from the start, ensure schema is handled
        if not table_chunks:
            schema_to_use = avro_pa_schema or schema
            if schema_to_use:
                 empty_arrays = [pa.array([], type=field.type) for field in schema_to_use]
                 return pa.Table.from_arrays(empty_arrays, schema=schema_to_use)
            else: # No schema available
                 return pa.Table.from_arrays([], names=[]) # Simplest empty table

    if not table_chunks:
        # If file existed but contained no records after header/schema
        schema_to_use = avro_pa_schema or schema
        if schema_to_use:
            empty_arrays = [pa.array([], type=field.type) for field in schema_to_use]
            return pa.Table.from_arrays(empty_arrays, schema=schema_to_use)
        else: # No schema available
             return pa.Table.from_arrays([], names=[]) # Simplest empty table

    # Concatenate all table chunks into the final table
    return pa.concat_tables(table_chunks)


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #
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
    
    elif fmt is Format.JSON:
        # If table is empty, explicitly create an empty file
        if table.num_rows == 0:
            path.touch()
        elif table.num_rows > 0:
            # PyArrow doesn't have a direct write_json function for JSON Lines
            # Convert to list of records and write as JSON lines
            records = table.to_pylist()
            with open(path, 'w') as fo:
                import json
                for record in records:
                    # Use default=str for types not directly serializable (like datetime)
                    fo.write(json.dumps(record, default=str) + '\n')
    
    elif fmt is Format.CSV:
        # Ensure kwargs are passed correctly, maybe provide defaults? TBD
        # Create WriteOptions object with the provided kwargs
        # Default null representation is empty string "" which matches our read options
        write_options = kwargs.pop('write_options', pacsv.WriteOptions()) 
        # Any remaining kwargs might be invalid for write_csv, consider logging/error
        if kwargs: 
            # TODO: Log warning about unused kwargs for CSV write
            pass 
        pacsv.write_csv(table, path, write_options=write_options)

    else:
        raise AssertionError("unreachable")


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


def _pyarrow_to_avro_type(pa_type: pa.DataType, field_path="") -> str | list | dict:
    """Convert PyArrow type to Avro type, defaulting to nullable unions."""
    # Mapping from PyArrow types to Avro types
    if pa.types.is_null(pa_type):
        return "null"
    elif pa.types.is_boolean(pa_type):
        # Ensure boolean types are represented as nullable unions in Avro
        return ["null", "boolean"]
    elif pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int32(pa_type):
        avro_type = "int"
    elif pa.types.is_int64(pa_type):
        avro_type = "long"
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
        # Default to string for unknown types
        avro_type = "string"
        # Consider logging a warning here
        print(f"Warning: Unsupported PyArrow type {pa_type} at {field_path}. Converting to Avro string.")

    # Return nullable union for all types by default, unless the type itself is 'null'
    if avro_type == "null":
        return "null"
    else:
        # Wrap primitive strings, complex dicts, and logical dicts in a nullable union
        return ["null", avro_type]

def _avro_to_pyarrow_schema(avro_schema: Dict) -> pa.Schema:
    """Convert Avro schema (dict) to PyArrow schema."""
    fields = []
    if avro_schema.get('type') != 'record' or 'fields' not in avro_schema:
        raise ValueError("Avro schema must be a record type with fields")

    for field in avro_schema['fields']:
        fields.append(_avro_field_to_pyarrow(field))

    return pa.schema(fields)

def _avro_type_to_pyarrow(avro_type: Any) -> pa.DataType:
    """Convert a single Avro type definition to a PyArrow DataType."""
    # Handle union types (often ["null", type])
    if isinstance(avro_type, list):
        # Find the non-null type in the union
        non_null_types = [t for t in avro_type if t != 'null']
        if len(non_null_types) == 1:
            # Recursively convert the non-null type
            return _avro_type_to_pyarrow(non_null_types[0])
        else:
            # Handle unions with multiple non-null types (complex) - default to string for now
            # Or raise error if such unions are not supported
            print(f"Warning: Avro union with multiple non-null types {non_null_types} not fully supported. Defaulting to string.")
            return pa.string()

    # Handle logical types (dict)
    if isinstance(avro_type, dict) and 'logicalType' in avro_type:
        logical_type = avro_type.get('logicalType')
        base_type = avro_type.get('type')
        if logical_type == 'date' and base_type == 'int':
            return pa.date32()
        elif logical_type == 'timestamp-micros' and base_type == 'long':
            return pa.timestamp('us')
        elif logical_type == 'decimal' and base_type == 'bytes':
            precision = avro_type.get('precision')
            scale = avro_type.get('scale')
            if precision is not None and scale is not None:
                return pa.decimal128(precision, scale)
        # Add other logical types as needed (time, duration, uuid etc.)
        # Fallback to the base physical type if logical type is unknown/unhandled
        return _avro_type_to_pyarrow(base_type)

    # Handle complex types defined as dicts (record, array, map, enum)
    if isinstance(avro_type, dict):
      avro_type_kind = avro_type.get('type')
      if avro_type_kind == 'record':
           # Nested record conversion (simplified - may need named types handling)
          fields = [_avro_field_to_pyarrow(f) for f in avro_type.get('fields', [])]
          return pa.struct(fields)
      elif avro_type_kind == 'array':
          item_type = _avro_type_to_pyarrow(avro_type.get('items'))
          return pa.list_(item_type)
      elif avro_type_kind == 'map':
          value_type = _avro_type_to_pyarrow(avro_type.get('values'))
          return pa.map_(pa.string(), value_type) # Avro map keys are always strings
      elif avro_type_kind == 'enum':
          # Represent Avro enum as PyArrow dictionary with indices, or just string
          return pa.string() # Simplest representation
      # Fall through if it's a dict but not a recognized complex type or logical type
      
    # Handle primitive types (string)
    if avro_type == 'null':
        return pa.null()
    elif avro_type == 'boolean':
        return pa.bool_()
    elif avro_type == 'int':
        return pa.int32()
    elif avro_type == 'long':
        return pa.int64()
    elif avro_type == 'float':
        return pa.float32()
    elif avro_type == 'double':
        return pa.float64()
    elif avro_type == 'bytes':
        return pa.binary()
    elif avro_type == 'string':
        return pa.string()

    else:
        print(f"Warning: Unsupported Avro type '{avro_type}'. Defaulting to string.")
        return pa.string()

def _avro_field_to_pyarrow(avro_field: Dict) -> pa.Field:
    """Convert an Avro field definition to a PyArrow Field."""
    name = avro_field['name']
    pa_type = _avro_type_to_pyarrow(avro_field['type'])
    # Check if the field is nullable (common pattern: type is a list starting with 'null')
    is_nullable = isinstance(avro_field['type'], list) and avro_field['type'][0] == 'null'
    return pa.field(name, pa_type, nullable=is_nullable)
