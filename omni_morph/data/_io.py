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
    """Read a file into a PyArrow Table.
    
    Args:
        path: Path to the file to read
        fmt: Format of the file
        schema: Optional schema to use for reading
        **kwargs: Additional format-specific options
            - For Parquet: memory_map, use_dataset, columns, filters, use_threads
            - For CSV: block_size, column_types, include_columns
            - For JSON: block_size, column_types
    
    Returns:
        PyArrow Table containing the data from the file
    """
    # Cache file stats to avoid multiple syscalls
    file_empty = False
    try:
        file_stats = path.stat()
        file_empty = file_stats.st_size == 0
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    
    # Extract common parameters that might not be supported by all formats
    use_threads = kwargs.pop('use_threads', True)  # Default to True but don't pass to all formats
    use_dataset = kwargs.pop('use_dataset', False)
    columns = kwargs.pop('columns', None)
    filters = kwargs.pop('filters', None)
    
    if fmt is Format.AVRO:
        if not HAS_FASTAVRO:
            raise ImportError("Fastavro is not available. Please install fastavro to support Avro format.")
        
        # Refactored Avro reading for robust null handling
        return _read_avro(path, schema=schema, **kwargs)

    elif fmt is Format.PARQUET:
        # Handle empty file case
        if file_empty and schema is not None:
            # Create empty typed arrays based on schema
            empty_arrays = [pa.array([], type=field.type) for field in schema]
            return pa.Table.from_arrays(empty_arrays, schema=schema)
        
        # For Parquet, we can use all the parameters
        parquet_kwargs = kwargs.copy()
        
        # Add back the parameters that Parquet supports
        parquet_kwargs['memory_map'] = kwargs.get('memory_map', True)  # Enable memory mapping by default
        
        if use_dataset:
            # Use dataset API for better performance with column projection and predicate push-down
            import pyarrow.dataset as ds
            
            dataset = ds.dataset(path, format="parquet")
            scanner_kwargs = {}
            
            # Add supported parameters
            if columns is not None:
                scanner_kwargs['columns'] = columns
            if filters is not None:
                scanner_kwargs['filter'] = filters
            if use_threads is not None:
                scanner_kwargs['use_threads'] = use_threads
                
            # Add any remaining kwargs
            scanner_kwargs.update(parquet_kwargs)
            
            scanner = dataset.scanner(**scanner_kwargs)
            return scanner.to_table()
        else:
            # Use standard read_table with memory mapping
            if use_threads is not None:
                parquet_kwargs['use_threads'] = use_threads
            if columns is not None:
                parquet_kwargs['columns'] = columns
            return papq.read_table(path, **parquet_kwargs)
    
    elif fmt is Format.JSON:
        # Handle empty file case
        if file_empty:
            # Return empty table with schema if provided, else simplest empty table
            if schema:
                empty_arrays = [pa.array([], type=field.type) for field in schema]
                return pa.Table.from_arrays(empty_arrays, schema=schema)
            else:
                return pa.Table.from_arrays([], names=[])
        
        # Extract and set JSON-specific options
        block_size = kwargs.pop('block_size', None)  # Default is ~32MB
        
        # Create read options with block size if specified
        read_options = kwargs.get('read_options', None)
        if block_size is not None and read_options is None:
            read_options = pajson.ReadOptions(block_size=block_size)
            kwargs['read_options'] = read_options
        
        # JSON doesn't support use_threads directly
        # Assume JSON Lines format
        return pajson.read_json(path, **kwargs)
    
    elif fmt is Format.CSV:
        # Handle empty file case
        if file_empty:
            # Return empty table with schema if provided, else let read_csv handle (might error)
            if schema:
                # Create empty typed arrays based on schema
                empty_arrays = [pa.array([], type=field.type) for field in schema]
                return pa.Table.from_arrays(empty_arrays, schema=schema)
        
        # Extract and set CSV-specific options for better performance
        block_size = kwargs.pop('block_size', 64 * 1024 * 1024)  # 64MB default for manageable chunks
        column_types = kwargs.pop('column_types', None)  # For type inference optimization
        include_columns = kwargs.pop('include_columns', None)  # For column projection
        
        # Create read options with optimized settings
        read_options = kwargs.get('read_options', None)
        if read_options is None:
            read_options = pacsv.ReadOptions(
                block_size=block_size,
                use_threads=use_threads  # CSV reader supports use_threads
            )
            kwargs['read_options'] = read_options
        
        # Create convert options with column types if provided
        convert_options = kwargs.get('convert_options', None)
        if convert_options is None and (column_types is not None or include_columns is not None):
            convert_options = pacsv.ConvertOptions(
                column_types=column_types,
                include_columns=include_columns
            )
            kwargs['convert_options'] = convert_options
        
        return pacsv.read_csv(path, **kwargs)
    
    else:
        raise AssertionError("unreachable")


CHUNK_SIZE = 10000 # Default chunk size for reading large files
def _read_avro(path: Path, schema: pa.Schema = None, chunk_size: int = CHUNK_SIZE, **kwargs) -> pa.Table:
    """Read an Avro file into a PyArrow Table, processing in chunks for memory efficiency.
    
    This implementation uses direct conversion to PyArrow without Pandas intermediary.
    """
    if not HAS_FASTAVRO:
        raise ImportError("Fastavro is not available. Please install fastavro to support Avro format.")

    table_chunks = []
    avro_pa_schema = schema

    try:
        with open(path, 'rb') as fo:
            # Get schema information first
            reader = fastavro.reader(fo)
            avro_schema_dict = reader.writer_schema

            # Convert Avro schema to PyArrow schema once if needed
            if not avro_pa_schema and avro_schema_dict:
                try:
                    avro_pa_schema = _avro_to_pyarrow_schema(avro_schema_dict)
                except Exception as e:
                    # Log warning, proceed without specific schema
                    print(f"Warning: Could not convert Avro schema to PyArrow schema: {e}")
                    avro_pa_schema = None  # Fallback
            
            # Reset file for reading records
            fo.seek(0)
            
            # Read records in chunks for memory efficiency
            records_chunk = []
            reader = fastavro.reader(fo)
            
            for i, record in enumerate(reader):
                records_chunk.append(record)
                if (i + 1) % chunk_size == 0:
                    if records_chunk:
                        try:
                            # Convert directly from list of records to Arrow table without Pandas
                            table_chunk = pa.Table.from_pylist(records_chunk, schema=avro_pa_schema)
                            table_chunks.append(table_chunk)
                        except pa.ArrowTypeError as e:
                            print(f"Warning: Schema mismatch converting chunk to Arrow: {e}. Trying without explicit schema.")
                            table_chunk = pa.Table.from_pylist(records_chunk)
                            table_chunks.append(table_chunk)
                        records_chunk = []  # Reset chunk

            # Process the last partial chunk if any records remain
            if records_chunk:
                try:
                    table_chunk = pa.Table.from_pylist(records_chunk, schema=avro_pa_schema)
                    table_chunks.append(table_chunk)
                except pa.ArrowTypeError as e:
                    print(f"Warning: Schema mismatch converting chunk to Arrow: {e}. Trying without explicit schema.")
                    table_chunk = pa.Table.from_pylist(records_chunk)
                    table_chunks.append(table_chunk)

    except StopIteration:  # Handle case where fastavro.reader yields nothing for empty file
        # If the file was empty from the start, ensure schema is handled
        if not table_chunks:
            schema_to_use = avro_pa_schema or schema
            if schema_to_use:
                empty_arrays = [pa.array([], type=field.type) for field in schema_to_use]
                return pa.Table.from_arrays(empty_arrays, schema=schema_to_use)
            else:  # No schema available
                return pa.Table.from_arrays([], names=[])  # Simplest empty table

    if not table_chunks:
        # If file existed but contained no records after header/schema
        schema_to_use = avro_pa_schema or schema
        if schema_to_use:
            empty_arrays = [pa.array([], type=field.type) for field in schema_to_use]
            return pa.Table.from_arrays(empty_arrays, schema=schema_to_use)
        else:  # No schema available
            return pa.Table.from_arrays([], names=[])  # Simplest empty table

    # Concatenate all table chunks into the final table
    # This pushes the work into the C++ layer
    return pa.concat_tables(table_chunks, promote=True)


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #
def _write_impl(table: pa.Table, path: Path, fmt: Format, **kwargs) -> None:
    """Write a PyArrow Table to a file.
    
    Args:
        table: The PyArrow Table to write
        path: Path to write the file to
        fmt: Format of the file
        **kwargs: Additional format-specific options
            - For Parquet: compression, use_dictionary, write_statistics, use_threads
            - For CSV: include_header, batch_size, delimiter, quoting_style
            - For JSON: indent
    """
    # Extract common parameters that might not be supported by all formats
    use_threads = kwargs.pop('use_threads', True)  # Default to True but don't pass to all formats
    compression = kwargs.pop('compression', None)
    
    if fmt is Format.AVRO:
        if not HAS_FASTAVRO:
            raise ImportError("Fastavro is not available. Please install fastavro to support Avro format.")
        
        # Optimize batch size based on table characteristics
        # For wide tables (many columns), use smaller batches
        # For narrow tables, use larger batches
        optimal_batch_size = min(1 << 16, max(1000, 1000000 // max(1, len(table.schema))))
        
        # Pre-identify columns that need conversion
        datetime_columns = set()
        string_columns = set()
        for field in table.schema:
            if pa.types.is_timestamp(field.type) or pa.types.is_date(field.type) or pa.types.is_time(field.type):
                datetime_columns.add(field.name)
            elif pa.types.is_binary(field.type):
                string_columns.add(field.name)
        
        # Generate Avro schema directly from the table schema
        avro_schema_dict = {}
        for field in table.schema:
            field_type = _pyarrow_to_avro_type(field.type)
            if field.name not in avro_schema_dict:
                avro_schema_dict[field.name] = field_type
        
        # Create the full Avro schema
        avro_schema = {
            "type": "record",
            "name": "ArrowRecord",
            "fields": [
                {"name": name, "type": type_def}
                for name, type_def in avro_schema_dict.items()
            ]
        }
        
        # Parse the schema for validation and optimization
        parsed_schema = fastavro.parse_schema(avro_schema)
        
        # Define an optimized generator function
        def optimized_record_generator():
            for batch in table.to_batches(max_chunksize=optimal_batch_size):
                records = batch.to_pylist()
                
                # Process only the columns that need conversion
                for record in records:
                    # Process datetime columns
                    for col in datetime_columns:
                        if col in record and record[col] is not None and hasattr(record[col], 'isoformat'):
                            record[col] = record[col].isoformat()
                    
                    # Process string columns
                    for col in string_columns:
                        if col in record and record[col] is not None:
                            record[col] = str(record[col])
                    
                    yield record
                
                # Help garbage collection for large tables
                del records
        
        # Write Avro file using fastavro with the optimized generator
        with open(path, 'wb') as fo:
            fastavro.writer(fo, parsed_schema, optimized_record_generator())
    
    elif fmt is Format.PARQUET:
        # Optimize for data-lake workloads with better compression and performance
        parquet_kwargs = {
            'compression': compression or 'zstd',  # ~30% smaller + ~1.2× write
            'use_dictionary': kwargs.pop('use_dictionary', True),
            'write_statistics': kwargs.pop('write_statistics', True),
            'data_page_size': kwargs.pop('data_page_size', 512*1024),  # Sane page – row-group ratio
            'version': kwargs.pop('version', '2.6'),
        }
        
        # Note: use_threads is not directly supported by the Parquet writer in this version
        # Remove it if present to avoid errors
        kwargs.pop('use_threads', None)
        
        # Add any remaining kwargs
        parquet_kwargs.update(kwargs)
        
        papq.write_table(table, path, **parquet_kwargs)
    
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
        # Create WriteOptions with optimized defaults
        # Note: CSV writer doesn't support use_threads directly
        write_opts = pacsv.WriteOptions(
            include_header=kwargs.pop("include_header", True),
            batch_size=kwargs.pop("batch_size", 1 << 16),  # Optimize batch size for wide tables
            delimiter=kwargs.pop("delimiter", ","),  # Handle delimiter parameter
            quoting_style=kwargs.pop("quoting_style", "needed")  # Handle quoting_style parameter
        )
        # Pass remaining kwargs to write_csv
        pacsv.write_csv(table, path, write_options=write_opts, **kwargs)

    else:
        raise AssertionError("unreachable")


# Add memoization cache for schema conversion
_pyarrow_to_avro_type_cache = {}

def _pyarrow_to_avro_type(pa_type: pa.DataType, field_path="") -> str | list | dict:
    """Convert PyArrow type to Avro type, defaulting to nullable unions.
    
    Uses memoization to avoid redundant computations for nested schemas.
    
    Args:
        pa_type: PyArrow data type to convert
        field_path: Path to the field in the schema (for nested types)
        
    Returns:
        Avro type representation (string, list, or dict)
    """
    # Use cache key based on type and path
    cache_key = (str(pa_type), field_path)
    if cache_key in _pyarrow_to_avro_type_cache:
        return _pyarrow_to_avro_type_cache[cache_key]
    
    # Mapping from PyArrow types to Avro types
    if pa.types.is_null(pa_type):
        result = "null"
    elif pa.types.is_boolean(pa_type):
        # Ensure boolean types are represented as nullable unions in Avro
        result = ["null", "boolean"]
    elif pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int32(pa_type):
        avro_type = "int"
        result = ["null", avro_type]
    elif pa.types.is_int64(pa_type):
        avro_type = "long"
        result = ["null", avro_type]
    elif pa.types.is_floating(pa_type):
        # Allow for null values in float fields
        result = ["null", "double"]
    elif pa.types.is_string(pa_type):
        # Allow for null values in string fields
        result = ["null", "string"]
    elif pa.types.is_binary(pa_type):
        result = ["null", "bytes"]
    elif pa.types.is_timestamp(pa_type) or pa.types.is_date(pa_type) or pa.types.is_time(pa_type):
        # Handle datetime types as strings in Avro, allow nulls
        result = ["null", "string"]
    elif pa.types.is_list(pa_type):
        item_type = _pyarrow_to_avro_type(pa_type.value_type, field_path + "_item")
        result = {"type": "array", "items": item_type}
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
        result = {"type": "record", "name": struct_name, "fields": fields}
    else:
        # Default to string for unknown types
        avro_type = "string"
        # Consider logging a warning here
        print(f"Warning: Unsupported PyArrow type {pa_type} at {field_path}. Converting to Avro string.")
        result = ["null", avro_type]
    
    # Cache the result
    _pyarrow_to_avro_type_cache[cache_key] = result
    return result


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
