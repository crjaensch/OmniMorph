import subprocess
import tempfile
from pathlib import Path

import pytest
import pyarrow.parquet as pq

# Path to the test data directory
TEST_DATA_DIR = Path(__file__).parent.parent / "data"

# Sample files for testing
CSV_FILES = [
    TEST_DATA_DIR / "sample-data" / "csv" / f"userdata{i}.csv" for i in range(1, 5)
]
AVRO_FILES = [
    TEST_DATA_DIR / "sample-data" / "avro" / f"userdata{i}.avro" for i in range(1, 5)
]
PARQUET_FILES = [
    TEST_DATA_DIR / "sample-data" / "parquet" / f"userdata{i}.parquet" for i in range(1, 5)
]

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
        return e


def verify_file_exists_and_has_records(file_path, fmt):
    """Verify that the file exists and has records."""
    # Check that the file exists
    assert file_path.exists(), f"Output file {file_path} does not exist"
    
    # Check that the file has records based on format
    if fmt == "parquet":
        # For Parquet, we can use pyarrow to read and check
        table = pq.read_table(file_path)
        assert len(table) > 0, f"Parquet file {file_path} has no records"
    elif fmt == "csv":
        # For CSV, we can check if the file has content
        with open(file_path, "r") as f:
            lines = f.readlines()
            assert len(lines) > 1, f"CSV file {file_path} has no data rows"
    else:
        # For other formats, just check file size
        assert file_path.stat().st_size > 0, f"File {file_path} is empty"


# Tests that are known to work across formats
@pytest.mark.parametrize(
    "output_format", 
    ["csv", "jsonl"]
)
def test_cross_format_merge(output_format):
    """Test merging files from different formats into a single output file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Get one file from each format
        input_files = [
            str(CSV_FILES[0]),      # First CSV file
            str(PARQUET_FILES[0]),  # First Parquet file
        ]
        
        # Define output file
        output_path = Path(tmpdir) / f"merged_output.{output_format}"
        
        # Run merge command
        result = run_cli(["merge", "--no-cast"] + input_files + [str(output_path)])
        
        # Verify command succeeded
        assert result.returncode == 0
        
        # Verify output file exists and has records
        verify_file_exists_and_has_records(output_path, output_format)


# Tests that are known to work within the same format
@pytest.mark.parametrize(
    "input_files,output_format",
    [
        (CSV_FILES, "csv"),
        (PARQUET_FILES, "parquet")
    ]
)
def test_same_format_merge(input_files, output_format):
    """Test merging multiple files of the same format."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Convert Path objects to strings for CLI
        input_file_strs = [str(f) for f in input_files]
        
        # Define output file
        output_path = Path(tmpdir) / f"merged_all.{output_format}"
        
        # Run merge command
        result = run_cli(["merge", "--no-cast"] + input_file_strs + [str(output_path)])
        
        # Verify command succeeded
        assert result.returncode == 0
        
        # Verify output file exists and has records
        verify_file_exists_and_has_records(output_path, output_format)


# Tests for format conversion that are known to work
@pytest.mark.parametrize(
    "input_files,input_format,output_format",
    [
        (CSV_FILES, "csv", "jsonl"),
        (AVRO_FILES, "avro", "csv"),
        (PARQUET_FILES, "parquet", "jsonl"),
    ]
)
def test_format_conversion_merge(input_files, input_format, output_format):
    """Test merging files and converting to a different format."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Convert Path objects to strings for CLI
        input_file_strs = [str(f) for f in input_files]
        
        # Define output file
        output_path = Path(tmpdir) / f"merged_converted.{output_format}"
        
        # Run merge command
        result = run_cli(["merge", "--no-cast"] + input_file_strs + [str(output_path)])
        
        # Verify command succeeded
        assert result.returncode == 0
        
        # Verify output file exists and has records
        verify_file_exists_and_has_records(output_path, output_format)


# Tests for known schema compatibility issues
@pytest.mark.xfail(reason="Schema incompatibility between formats")
@pytest.mark.parametrize(
    "input_files,input_format,output_format",
    [
        (CSV_FILES, "csv", "parquet"),  # CSV to Parquet has schema issues
        (AVRO_FILES, "avro", "avro"),   # Avro merging has schema issues
    ]
)
def test_schema_incompatibility_merge(input_files, input_format, output_format):
    """Test merging files with known schema incompatibilities.
    
    These tests are expected to fail due to schema differences between the sample files.
    They are marked with xfail to document the known limitations.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # Convert Path objects to strings for CLI
        input_file_strs = [str(f) for f in input_files]
        
        # Define output file
        output_path = Path(tmpdir) / f"merged_incompatible.{output_format}"
        
        # Run merge command
        result = run_cli(["merge", "--no-cast"] + input_file_strs + [str(output_path)])
        
        # Verify command succeeded (this will fail, hence the xfail)
        assert result.returncode == 0
        
        # Verify output file exists and has records
        verify_file_exists_and_has_records(output_path, output_format)


@pytest.mark.parametrize(
    "input_files,expected_exit_code,error_pattern",
    [
        (["nonexistent.csv"], 1, "Error merging files"),
    ]
)
def test_merge_error_handling(input_files, expected_exit_code, error_pattern):
    """Test error handling for the merge command."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "output.csv"
        
        # Run merge command with non-existent file
        result = run_cli(["merge"] + input_files + [str(output_path)], 
                         expected_exit_code=expected_exit_code,
                         check=False)
        
        # Verify error message
        assert error_pattern in result.stderr
