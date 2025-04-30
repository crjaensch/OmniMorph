# -*- coding: utf-8 -*-
"""Unit tests for the _io module."""

import pytest
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as papq
import pandas as pd
from pathlib import Path
import json

# Try importing fastavro for tests, mock if unavailable
try:
    import fastavro
    HAS_FASTAVRO_TEST = True
except ImportError:
    HAS_FASTAVRO_TEST = False
    fastavro = None # Define it as None if not available

# Import functions and constants from the module under test
from omni_morph.data._io import (
    _read_impl,
    _write_impl,
    HAS_FASTAVRO as MODULE_HAS_FASTAVRO,
    _generate_avro_schema,
    _pyarrow_to_avro_type
)
from omni_morph.data.formats import Format

# --- Fixtures --- #

@pytest.fixture(scope="module")
def temp_dir_module(tmp_path_factory):
    """Create a temporary directory for the module scope."""
    d = tmp_path_factory.mktemp("test_io_module")
    yield d
    # tmp_path_factory handles cleanup

@pytest.fixture
def sample_df():
    """Provides a sample Pandas DataFrame for testing."""
    return pd.DataFrame({
        'col_int': pd.Series([1, 2, None, 4], dtype=pd.Int64Dtype()),
        'col_float': [1.1, 2.2, 3.3, None],
        'col_str': ['a', None, 'c', 'd'],
        'col_bool': pd.Series([True, False, True, None], dtype=pd.BooleanDtype()),
        # TODO: Add datetime column later
    })

@pytest.fixture
def sample_table(sample_df):
    """Provides a sample PyArrow Table derived from the DataFrame."""
    return pa.Table.from_pandas(sample_df, preserve_index=False)

@pytest.fixture
def empty_table():
    """Provides an empty PyArrow Table with a defined schema."""
    schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('value', pa.string())
    ])
    # Provide empty arrays for each column in the schema
    empty_arrays = [pa.array([], type=field.type) for field in schema]
    return pa.Table.from_arrays(empty_arrays, schema=schema)


# --- Helper Functions --- #

def assert_table_equal(table1: pa.Table, table2: pa.Table):
    """Asserts that two PyArrow Tables are equal, handling potential type nuances."""
    # Sort columns by name for consistent comparison
    table1_sorted = table1.select(sorted(table1.column_names))
    table2_sorted = table2.select(sorted(table2.column_names))
    assert table1_sorted.equals(table2_sorted)

def assert_table_equal_pandas(table1: pa.Table, table2: pa.Table):
    """Asserts two tables are equal using Pandas conversion for robustness."""
    df1 = table1.to_pandas(types_mapper=pd.ArrowDtype)
    df2 = table2.to_pandas(types_mapper=pd.ArrowDtype)
    pd.testing.assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_dtype=True)


# --- Test _read_impl --- #

def test_read_csv(temp_dir_module, sample_table):
    """Test _read_impl for a CSV file."""
    file_path = temp_dir_module / "test_read.csv"
    # Use standard pyarrow writer to create test file
    pacsv.write_csv(sample_table, file_path)

    # Test the function under test
    # Define ConvertOptions to handle nulls
    convert_options = pacsv.ConvertOptions(null_values=["", "NULL", "N/A"])
    read_options = pacsv.ReadOptions(use_threads=False) # Autogenerate column names off by default

    read_back_table = _read_impl(
        file_path, 
        Format.CSV, 
        read_options=read_options, 
        convert_options=convert_options
    )

    # Verify the data, checking for nulls explicitly
    assert read_back_table.schema.equals(sample_table.schema)

def test_read_json(temp_dir_module, sample_table):
    """Test _read_impl for a JSON Lines file."""
    file_path = temp_dir_module / "test_read.jsonl"
    # _write_impl produces JSON lines, use it for setup here
    _write_impl(sample_table, file_path, Format.JSON)

    # Test the function under test
    read_back_table = _read_impl(file_path, Format.JSON)

    # JSON read might infer int as float etc. Use pandas comparison.
    assert_table_equal_pandas(sample_table, read_back_table)

def test_read_parquet(temp_dir_module, sample_table):
    """Test _read_impl for a Parquet file."""
    file_path = temp_dir_module / "test_read.parquet"
    # Use standard pyarrow writer
    papq.write_table(sample_table, file_path)

    # Test the function under test
    read_back_table = _read_impl(file_path, Format.PARQUET)
    # Parquet preserves schema well
    assert_table_equal(sample_table, read_back_table)

@pytest.mark.skipif(not HAS_FASTAVRO_TEST, reason="Test requires fastavro to be installed")
def test_read_avro(temp_dir_module, sample_table):
    """Test _read_impl for an Avro file (requires fastavro)."""
    file_path = temp_dir_module / "test_read.avro"
    # Use _write_impl for Avro setup to test the module's writer/reader pair
    try:
        _write_impl(sample_table, file_path, Format.AVRO)
    except ImportError:
        pytest.fail("ImportError raised during write, but fastavro should be available")

    # Test the function under test
    read_back_table = _read_impl(file_path, Format.AVRO)

    # Avro roundtrip might change types slightly (e.g., int precision)
    assert_table_equal_pandas(sample_table, read_back_table)

@pytest.mark.skipif(MODULE_HAS_FASTAVRO, reason="Test requires fastavro to be NOT installed")
def test_read_avro_no_fastavro(temp_dir_module):
    """Test _read_impl for Avro when fastavro is not available."""
    file_path = temp_dir_module / "dummy_no_avro.avro"
    file_path.touch()
    with pytest.raises(ImportError, match="Fastavro is not available"):
        _read_impl(file_path, Format.AVRO)

def test_read_empty_csv(temp_dir_module, empty_table):
    """Test _read_impl for an empty CSV file."""
    file_path = temp_dir_module / "empty.csv"
    pacsv.write_csv(empty_table, file_path)

    read_back_table = _read_impl(file_path, Format.CSV)
    assert read_back_table.num_rows == 0
    assert sorted(read_back_table.schema.names) == sorted(empty_table.schema.names)

# --- Test _write_impl --- #

def test_write_csv(temp_dir_module, sample_table):
    """Test _write_impl for a CSV file."""
    file_path = temp_dir_module / "test_write.csv"
    # Test the function under test
    _write_impl(sample_table, file_path, Format.CSV)

    # Read back using standard pyarrow reader for verification
    # Configure options for correct reading: use header, handle nulls
    convert_options = pacsv.ConvertOptions(
        # Include standard null representations
        null_values=["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null"],
        # Allow strings listed in null_values to become null
        strings_can_be_null=True
        # Remove column_types to avoid overriding null interpretation
        # column_types=sample_table.schema 
        )
    read_options = pacsv.ReadOptions(use_threads=False, autogenerate_column_names=False)
    read_back_table = pacsv.read_csv(
        file_path,
        read_options=read_options,
        convert_options=convert_options
    )

    # Ensure the data is identical after round trip
    assert read_back_table.equals(sample_table)

def test_write_json(temp_dir_module, sample_table):
    """Test _write_impl for a JSON Lines file."""
    file_path = temp_dir_module / "test_write.jsonl"
    # Test the function under test
    _write_impl(sample_table, file_path, Format.JSON)

    # Read back using standard pyarrow reader for verification
    read_back_table = pajson.read_json(file_path)
    assert_table_equal_pandas(sample_table, read_back_table)

def test_write_parquet(temp_dir_module, sample_table):
    """Test _write_impl for a Parquet file."""
    file_path = temp_dir_module / "test_write.parquet"
    # Test the function under test
    _write_impl(sample_table, file_path, Format.PARQUET)

    # Read back using standard pyarrow reader for verification
    read_back_table = papq.read_table(file_path)
    assert_table_equal(sample_table, read_back_table)

@pytest.mark.skipif(not HAS_FASTAVRO_TEST, reason="Test requires fastavro to be installed")
def test_write_avro(temp_dir_module, sample_table):
    """Test _write_impl for an Avro file (requires fastavro)."""
    file_path = temp_dir_module / "test_write.avro"
    # Test the function under test
    try:
        _write_impl(sample_table, file_path, Format.AVRO)
    except ImportError:
        pytest.fail("ImportError raised during write, but fastavro should be available")

    # Read back using _read_impl for verification (tests roundtrip)
    read_back_table = _read_impl(file_path, Format.AVRO)
    assert_table_equal_pandas(sample_table, read_back_table)

@pytest.mark.skipif(MODULE_HAS_FASTAVRO, reason="Test requires fastavro to be NOT installed")
def test_write_avro_no_fastavro(temp_dir_module, sample_table):
    """Test _write_impl for Avro when fastavro is not available."""
    file_path = temp_dir_module / "dummy_no_avro_write.avro"
    with pytest.raises(ImportError, match="Fastavro is not available"):
        _write_impl(sample_table, file_path, Format.AVRO)

def test_write_empty_table(temp_dir_module, empty_table):
    """Test _write_impl for an empty table to various formats."""
    formats_to_test = [Format.CSV, Format.JSON, Format.PARQUET]
    if HAS_FASTAVRO_TEST:
        formats_to_test.append(Format.AVRO)

    for fmt in formats_to_test:
        file_path = temp_dir_module / f"empty_write.{fmt.value.lower()}"
        _write_impl(empty_table, file_path, fmt)
    
        # Pass the original schema to ensure columns are preserved when reading empty file
        read_back_table = _read_impl(file_path, fmt, schema=empty_table.schema)
        assert read_back_table.num_rows == 0
        assert sorted(read_back_table.schema.names) == sorted(empty_table.schema.names)
        
        # Check types match too, BUT only for formats that embed schema in empty files
        if fmt in [Format.PARQUET, Format.AVRO]:
            assert read_back_table.schema.equals(empty_table.schema, check_metadata=False)
        # For CSV/JSON, empty files might not preserve type info upon read, so skip strict type check


# --- Test Helpers (Optional Direct Tests) --- #

def test_pyarrow_to_avro_type_basic():
    """Test basic type mappings from PyArrow to Avro."""
    assert _pyarrow_to_avro_type(pa.int64()) == ["null", "long"]
    assert _pyarrow_to_avro_type(pa.string()) == ["null", "string"]
    assert _pyarrow_to_avro_type(pa.float64()) == ["null", "double"]
    assert _pyarrow_to_avro_type(pa.bool_()) == ["null", "boolean"]
    # TODO: Add more types (date, timestamp, list, struct etc.)

@pytest.mark.skipif(not HAS_FASTAVRO_TEST, reason="Test requires fastavro to be installed")
def test_generate_avro_schema_mixed_types():
    """Test Avro schema generation infers string for mixed types."""
    # Explicitly use object dtype to avoid pandas inferring int
    mixed_df = pd.DataFrame({'col_mix': pd.Series([1, 2, '3', None], dtype=object)})
    
    # Convert to PyArrow table without explicit schema first
    try:
        mixed_table_inferred = pa.Table.from_pandas(mixed_df, safe=False) # safe=False might be needed for object dtype
    except pa.ArrowInvalid as e:
        # If initial conversion fails (e.g., ArrowInvalid due to mix), try converting column to string first in Pandas
        if "Could not convert" in str(e):
            mixed_df['col_mix'] = mixed_df['col_mix'].astype(str).replace('<NA>', pd.NA)
            mixed_table_inferred = pa.Table.from_pandas(mixed_df, safe=False)
        else:
            raise e
            
    # Find the index of the column to cast
    col_index = mixed_table_inferred.schema.get_field_index('col_mix')
    # Cast the column to string type
    mixed_table = mixed_table_inferred.set_column(col_index, 'col_mix', mixed_table_inferred.column('col_mix').cast(pa.string()))

    # Generate Avro schema using the function under test (with the string column)
    sample_records = mixed_table.to_pylist()
    avro_schema = _generate_avro_schema(mixed_table, sample_records)
    schema, string_columns = avro_schema

    assert 'col_mix' in string_columns
    col_mix_field = next(f for f in schema['fields'] if f['name'] == 'col_mix')
    assert col_mix_field['type'] == ["null", "string"]
