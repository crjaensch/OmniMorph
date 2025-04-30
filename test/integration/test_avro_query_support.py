#!/usr/bin/env python

"""
Integration tests for Avro query support in OmniMorph.

These tests focus specifically on the ability to query Avro files
using the query_engine module with both the DuckDB Avro extension
and the fastavro fallback mechanism.
"""

import subprocess
import tempfile
from pathlib import Path

import pytest
import duckdb

from omni_morph.data.query_engine import _ensure_avro_extension

# Path to the test data directory
TEST_DATA_DIR = Path(__file__).parent.parent / "data"

# Sample Avro files for testing
AVRO_TEST_FILE = TEST_DATA_DIR / "avro" / "test.avro"
AVRO_TEST_DEFLATE = TEST_DATA_DIR / "avro" / "test-deflate.avro"
AVRO_TEST_SNAPPY = TEST_DATA_DIR / "avro" / "test-snappy.avro"
AVRO_USERDATA_FILE = TEST_DATA_DIR / "sample-data" / "avro" / "userdata1.avro"

# Command to run the CLI
CLI_CMD = ["poetry", "run", "omo-cli"]


def run_cli(args, expected_exit_code=0, check=True):
    """Run the CLI with the given arguments and return the result."""
    cmd = CLI_CMD + args
    try:
        result = subprocess.run(
            cmd,
            check=check,
            capture_output=True,
            text=True,
        )
        if check and expected_exit_code != 0:
            pytest.fail(f"Command {cmd} succeeded but was expected to fail")
        return result
    except subprocess.CalledProcessError as e:
        if expected_exit_code == 0:
            pytest.fail(f"Command {cmd} failed with exit code {e.returncode}\nStdout: {e.stdout}\nStderr: {e.stderr}")
        assert e.returncode == expected_exit_code
        return e


def test_avro_extension_availability():
    """Test if the DuckDB Avro extension is available."""
    # This test doesn't fail if the extension isn't available,
    # it just reports the status for informational purposes
    conn = duckdb.connect(database=":memory:", config={"allow_unsigned_extensions": "true"})
    extension_loaded = _ensure_avro_extension(conn)
    
    # Just log the status - the test should pass regardless
    if extension_loaded:
        print("\nDuckDB Avro extension is available and loaded.")
    else:
        print("\nDuckDB Avro extension is not available. Using fastavro fallback.")


def test_avro_query_basic():
    """Test basic querying of Avro files."""
    # Simple query on the test.avro file
    result = run_cli(["query", str(AVRO_TEST_FILE), "SELECT * FROM test LIMIT 5"])
    assert result.returncode == 0
    
    # Verify the output contains expected data
    output = result.stdout
    assert "|" in output, "Output should contain markdown table formatting"
    assert "age" in output, "Output should contain age column"
    assert "is_human" in output, "Output should contain is_human column"
    assert "quote" in output, "Output should contain quote column"


def test_avro_query_filter():
    """Test querying Avro files with filters."""
    # Query with a WHERE clause
    result = run_cli(["query", str(AVRO_TEST_FILE), "SELECT age, is_human, quote FROM test WHERE age > 20"])
    assert result.returncode == 0
    
    # Verify the output contains expected data
    output = result.stdout
    assert "|" in output, "Output should contain markdown table formatting"
    assert "age" in output, "Output should contain age column"
    
    # Check that all ages in the result are > 20
    # This is a simple check that just ensures no age values <= 20 appear in the output
    for line in output.split('\n'):
        if '|' in line and 'age' not in line:  # Skip header and non-data lines
            parts = line.split('|')
            if len(parts) > 1 and parts[1].strip().isdigit():
                age = int(parts[1].strip())
                assert age > 20, f"Found age {age} <= 20 in filtered results"


def test_avro_query_userdata():
    """Test querying larger Avro files (userdata)."""
    # Query with aggregation
    result = run_cli(["query", str(AVRO_USERDATA_FILE), 
                     "SELECT gender, COUNT(*) as count, AVG(salary) as avg_salary FROM userdata1 GROUP BY gender"])
    assert result.returncode == 0
    
    # Verify the output contains expected columns
    output = result.stdout
    assert "gender" in output, "Output should contain gender column"
    assert "count" in output, "Output should contain count column"
    assert "avg_salary" in output, "Output should contain avg_salary column"


def test_avro_query_compression():
    """Test querying Avro files with different compression types."""
    # Test with deflate compression
    if AVRO_TEST_DEFLATE.exists():
        # Use the stem of the file as the table name
        table_name = AVRO_TEST_DEFLATE.stem
        result = run_cli(["query", str(AVRO_TEST_DEFLATE), f"SELECT * FROM \"{table_name}\" LIMIT 5"])
        assert result.returncode == 0
        assert "|" in result.stdout, "Output should contain markdown table formatting"
    
    # Test with snappy compression
    if AVRO_TEST_SNAPPY.exists():
        # Use the stem of the file as the table name
        table_name = AVRO_TEST_SNAPPY.stem
        result = run_cli(["query", str(AVRO_TEST_SNAPPY), f"SELECT * FROM \"{table_name}\" LIMIT 5"])
        assert result.returncode == 0
        assert "|" in result.stdout, "Output should contain markdown table formatting"


def test_avro_query_invalid():
    """Test querying Avro files with invalid queries."""
    # Test with an invalid SQL query
    result = run_cli(["query", str(AVRO_TEST_FILE), "SELECT * FROM nonexistent_table"])
    assert result.returncode == 0  # Command succeeds but shows validation error
    assert "SQL validation failed" in result.stdout
    assert "nonexistent_table" in result.stdout
    
    # Test with a non-existent file
    result = run_cli(["query", "nonexistent.avro", "SELECT * FROM nonexistent"], 
                     expected_exit_code=1, check=False)
    assert result.returncode == 1
    assert "Error" in result.stderr
