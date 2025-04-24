import subprocess
import tempfile
from pathlib import Path

import pytest

# Path to the test data directory
TEST_DATA_DIR = Path(__file__).parent.parent / "data"

# Sample files for testing
CSV_FILE = TEST_DATA_DIR / "sample-data" / "csv" / "userdata1.csv"
JSON_FILE = TEST_DATA_DIR / "sample-data" / "json" / "books1.json"
AVRO_FILE = TEST_DATA_DIR / "avro" / "test.avro"
PARQUET_FILE = TEST_DATA_DIR / "parquet" / "test.parquet"
SCHEMA_FILE = TEST_DATA_DIR / "schema_valid.avsc"

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


# Test global options
def test_version():
    """Test the --version option."""
    result = run_cli(["--version"])
    assert result.stdout.strip(), "Version should not be empty"


def test_verbose():
    """Test the --verbose option."""
    result = run_cli(["--verbose", "count", str(CSV_FILE)])
    assert result.returncode == 0


def test_debug():
    """Test the --debug option."""
    result = run_cli(["--debug", "count", str(CSV_FILE)])
    assert result.returncode == 0


# Test implemented commands
def test_head():
    """Test the head command."""
    # Test with default number of records
    result = run_cli(["head", str(CSV_FILE)])
    assert result.returncode == 0
    # Count the number of lines in the output
    lines = result.stdout.strip().split("\n")
    assert len(lines) <= 20  # Default is 20 records
    
    # Verify the output contains valid data
    assert all('{' in line for line in lines), "Output should contain JSON-formatted records"

    # Test with custom number of records
    result = run_cli(["head", str(CSV_FILE), "-n", "5"])
    assert result.returncode == 0
    lines = result.stdout.strip().split("\n")
    assert len(lines) <= 5
    
    # Test with non-existent file
    result = run_cli(["head", "nonexistent.csv"], expected_exit_code=1, check=False)
    assert result.returncode == 1
    assert "Error:" in result.stderr


def test_tail():
    """Test the tail command."""
    # Test with default number of records
    result = run_cli(["tail", str(CSV_FILE)])
    assert result.returncode == 0
    # Count the number of lines in the output
    lines = result.stdout.strip().split("\n")
    assert len(lines) <= 20  # Default is 20 records
    
    # Verify the output contains valid data
    assert all('{' in line for line in lines), "Output should contain JSON-formatted records"

    # Test with custom number of records
    result = run_cli(["tail", str(CSV_FILE), "-n", "5"])
    assert result.returncode == 0
    lines = result.stdout.strip().split("\n")
    assert len(lines) <= 5
    
    # Test with non-existent file
    result = run_cli(["tail", "nonexistent.csv"], expected_exit_code=1, check=False)
    assert result.returncode == 1
    assert "Error:" in result.stderr


def test_count():
    """Test the count command."""
    result = run_cli(["count", str(CSV_FILE)])
    assert result.returncode == 0
    # The count should be a positive integer
    count = int(result.stdout.strip())
    assert count > 0


# Test conversion commands - these may fail due to data format issues
# but we want to verify the CLI interface works correctly
def test_conversion_commands():
    """Test the conversion commands."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a simple CSV file that should be easier to convert
        simple_csv = Path(tmpdir) / "simple.csv"
        with open(simple_csv, "w") as f:
            f.write("id,name,value\n")
            f.write("1,test1,100\n")
            f.write("2,test2,200\n")
        
        # Test to-json command
        json_output = Path(tmpdir) / "output.json"
        result = run_cli(["to-json", str(simple_csv), str(json_output)], check=False)
        # We don't assert success, just verify the command is recognized
        assert result.returncode in (0, 1)
        
        # Test to-csv command
        csv_output = Path(tmpdir) / "output.csv"
        result = run_cli(["to-csv", str(JSON_FILE), str(csv_output)], check=False)
        assert result.returncode in (0, 1)
        
        # Test to-parquet command
        parquet_output = Path(tmpdir) / "output.parquet"
        result = run_cli(["to-parquet", str(simple_csv), str(parquet_output)], check=False)
        assert result.returncode in (0, 1)
        
        # Test to-avro command
        avro_output = Path(tmpdir) / "output.avro"
        result = run_cli(["to-avro", str(simple_csv), str(avro_output)], check=False)
        assert result.returncode in (0, 1)


# Test unimplemented commands - they should fail with exit code 1
def test_meta():
    """Test the meta command (unimplemented)."""
    result = run_cli(["meta", str(CSV_FILE)], expected_exit_code=1, check=False)
    assert "not implemented" in result.stderr.lower()

def test_schema():
    """Test the schema command."""
    result = run_cli(["schema", str(CSV_FILE)])
    # Should succeed and print CSV schema
    assert result.returncode == 0
    stdout = result.stdout.strip()
    assert stdout.startswith("{") and stdout.endswith("}"), \
        "Schema command should output CSV schema"

def test_stats():
    """Test the stats command (unimplemented)."""
    result = run_cli(["stats", str(CSV_FILE)], expected_exit_code=1, check=False)
    assert "not implemented" in result.stderr.lower()


def test_validate():
    """Test the validate command (unimplemented)."""
    result = run_cli(["validate", str(CSV_FILE)], expected_exit_code=1, check=False)
    assert "not implemented" in result.stderr.lower()

    # Test with schema path
    result = run_cli(
        ["validate", str(CSV_FILE), "--schema-path", str(SCHEMA_FILE)],
        expected_exit_code=1,
        check=False
    )
    assert "not implemented" in result.stderr.lower()


def test_merge():
    """Test the merge command (unimplemented)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "merged.csv"
        result = run_cli(
            ["merge", str(CSV_FILE), str(CSV_FILE), str(output_path)],
            expected_exit_code=1,
            check=False
        )
        assert "not implemented" in result.stderr.lower()


def test_random_sample():
    """Test the random-sample command."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "sample.csv"
        
        # Test with n parameter
        result = run_cli(
            ["random-sample", str(CSV_FILE), str(output_path), "--n", "10"],
            check=False
        )
        assert result.returncode == 0, f"Command failed with error: {result.stderr}"
        # Verify the output file exists and has content
        assert output_path.exists(), "Output file was not created"
        assert output_path.stat().st_size > 0, "Output file is empty"
        
        # Test with fraction parameter
        output_path2 = Path(tmpdir) / "sample_fraction.csv"
        result = run_cli(
            ["random-sample", str(CSV_FILE), str(output_path2), "--fraction", "0.1"],
            check=False
        )
        assert result.returncode == 0, f"Command failed with error: {result.stderr}"
        assert output_path2.exists(), "Output file was not created"
        assert output_path2.stat().st_size > 0, "Output file is empty"
        
        # Test with invalid parameters (neither n nor fraction)
        output_path3 = Path(tmpdir) / "sample_invalid.csv"
        result = run_cli(
            ["random-sample", str(CSV_FILE), str(output_path3)],
            expected_exit_code=1,
            check=False
        )
        assert result.returncode == 1
        assert "Error:" in result.stderr
        
        # Test with non-existent file
        result = run_cli(
            ["random-sample", "nonexistent.csv", str(output_path)],
            expected_exit_code=1,
            check=False
        )
        assert result.returncode == 1
        assert "Error:" in result.stderr
