import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from InquirerPy.base.control import Choice

# Import the wizard module for direct testing
from omni_morph.omo_wizard import build_command, COMMANDS

# Path to the test data directory
TEST_DATA_DIR = Path(__file__).parent.parent / "data"

# Sample files for testing
CSV_FILE = TEST_DATA_DIR / "sample-data" / "csv" / "userdata1.csv"
JSON_FILE = TEST_DATA_DIR / "sample-data" / "json" / "books1.json"
AVRO_FILE = TEST_DATA_DIR / "sample-data" / "avro" / "userdata1.avro"
PARQUET_FILE = TEST_DATA_DIR / "sample-data" / "parquet" / "userdata1.parquet"
SCHEMA_FILE = TEST_DATA_DIR / "schema_valid.avsc"

# Command to run the wizard
WIZARD_CMD = ["poetry", "run", "omo-wizard"]


# Mock the InquirerPy prompts
def mock_inquirer_responses(monkeypatch, responses):
    """Mock InquirerPy prompts to return predefined responses."""
    # Create a mock execute function that returns values from the responses list
    mock_execute = MagicMock(side_effect=responses)
    
    # Create a mock for each InquirerPy prompt type
    mock_text = MagicMock()
    mock_text.execute = mock_execute
    
    mock_filepath = MagicMock()
    mock_filepath.execute = mock_execute
    
    mock_number = MagicMock()
    mock_number.execute = mock_execute
    
    mock_confirm = MagicMock()
    mock_confirm.execute = mock_execute
    
    mock_select = MagicMock()
    mock_select.execute = mock_execute
    
    # Patch the InquirerPy prompts
    monkeypatch.setattr("omni_morph.omo_wizard.inquirer.text", lambda **kwargs: mock_text)
    monkeypatch.setattr("omni_morph.omo_wizard.inquirer.filepath", lambda **kwargs: mock_filepath)
    monkeypatch.setattr("omni_morph.omo_wizard.inquirer.number", lambda **kwargs: mock_number)
    monkeypatch.setattr("omni_morph.omo_wizard.inquirer.confirm", lambda **kwargs: mock_confirm)
    monkeypatch.setattr("omni_morph.omo_wizard.inquirer.select", lambda **kwargs: mock_select)


# Test build_command function directly
def test_build_command_head():
    """Test that build_command correctly builds a head command."""
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(CSV_FILE)), \
         patch("omni_morph.omo_wizard.ask_int", return_value=5):
        
        command_str = build_command("head")
        
        assert "omo-cli head" in command_str
        assert "--number 5" in command_str
        assert str(CSV_FILE) in command_str


def test_build_command_tail():
    """Test that build_command correctly builds a tail command."""
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(CSV_FILE)), \
         patch("omni_morph.omo_wizard.ask_int", return_value=5):
        
        command_str = build_command("tail")
        
        assert "omo-cli tail" in command_str
        assert "--number 5" in command_str
        assert str(CSV_FILE) in command_str


def test_build_command_stats():
    """Test that build_command correctly builds a stats command."""
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(CSV_FILE)), \
         patch("omni_morph.omo_wizard.ask_text", return_value=""), \
         patch("omni_morph.omo_wizard.ask_int", return_value=2048), \
         patch("omni_morph.omo_wizard.ask_flag", return_value=True):
        
        command_str = build_command("stats")
        
        assert "omo-cli stats" in command_str
        assert "--markdown" in command_str
        assert "--sample-size 2048" in command_str
        assert str(CSV_FILE) in command_str


def test_build_command_to_json():
    """Test that build_command correctly builds a to-json command."""
    output_file = "output.json"
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(CSV_FILE)), \
         patch("omni_morph.omo_wizard.ask_output_path", return_value=output_file), \
         patch("omni_morph.omo_wizard.ask_flag", return_value=True):
        
        command_str = build_command("to-json")
        
        assert "omo-cli to-json" in command_str
        assert "--pretty" in command_str
        assert str(CSV_FILE) in command_str
        assert output_file in command_str


def test_build_command_random_sample():
    """Test that build_command correctly builds a random-sample command."""
    output_file = "sample.csv"
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(CSV_FILE)), \
         patch("omni_morph.omo_wizard.ask_output_path", return_value=output_file), \
         patch("omni_morph.omo_wizard.ask_int", return_value=100), \
         patch("omni_morph.omo_wizard.ask_text", side_effect=["0.1", "42"]), \
         patch("omni_morph.omo_wizard.ask_flag", return_value=False):
        
        command_str = build_command("random-sample")
        
        assert "omo-cli random-sample" in command_str
        assert "--n 100" in command_str
        assert "--fraction 0.1" in command_str
        assert "--seed" in command_str
        assert str(CSV_FILE) in command_str
        assert output_file in command_str


def test_build_command_query():
    """Test that build_command correctly builds a query command."""
    sql_query = "SELECT * FROM userdata1 LIMIT 10"
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(CSV_FILE)), \
         patch("omni_morph.omo_wizard.ask_text", side_effect=["json", sql_query]), \
         patch("omni_morph.omo_wizard.ask_flag", return_value=True):
        
        command_str = build_command("query")
        
        assert "omo-cli query" in command_str
        assert "--format json" in command_str
        assert str(CSV_FILE) in command_str
        assert sql_query in command_str


# Test all commands in COMMANDS registry
@pytest.mark.parametrize("cmd_name", COMMANDS.keys())
def test_all_commands_have_required_args(cmd_name):
    """Test that all commands have the required arguments."""
    cmd_spec = COMMANDS[cmd_name]
    assert "args" in cmd_spec, f"Command {cmd_name} is missing 'args' key"
    assert isinstance(cmd_spec["args"], list), f"Command {cmd_name} 'args' should be a list"
    
    # Check that all args have name and kind
    for arg in cmd_spec["args"]:
        assert "name" in arg, f"Argument in {cmd_name} is missing 'name' key"
        assert "kind" in arg, f"Argument {arg['name']} in {cmd_name} is missing 'kind' key"
        
        # Check that positional args are correctly marked
        if arg.get("positional", False):
            assert arg["kind"] in ["path", "output_path", "text", "sql", "paths"], \
                f"Positional argument {arg['name']} in {cmd_name} has invalid kind {arg['kind']}"


# Test command building with mocked user input
@pytest.mark.parametrize("file_path", [CSV_FILE, JSON_FILE, AVRO_FILE, PARQUET_FILE])
def test_head_command_with_different_files(file_path):
    """Test building the head command with different file types."""
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(file_path)), \
         patch("omni_morph.omo_wizard.ask_int", return_value=5):
        
        command_str = build_command("head")
        
        assert "omo-cli head" in command_str
        assert "--number 5" in command_str
        assert str(file_path) in command_str


@pytest.mark.parametrize("file_path", [CSV_FILE])
def test_stats_command_with_options(file_path):
    """Test building the stats command with different options."""
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(file_path)), \
         patch("omni_morph.omo_wizard.ask_text", return_value=""), \
         patch("omni_morph.omo_wizard.ask_int", return_value=2048), \
         patch("omni_morph.omo_wizard.ask_flag", return_value=True):
        
        command_str = build_command("stats")
        
        assert "omo-cli stats" in command_str
        assert "--markdown" in command_str
        assert "--sample-size 2048" in command_str
        assert str(file_path) in command_str


def test_to_json_command_with_options():
    """Test building the to-json command with different options."""
    output_file = "output.json"
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(CSV_FILE)), \
         patch("omni_morph.omo_wizard.ask_output_path", return_value=output_file), \
         patch("omni_morph.omo_wizard.ask_flag", return_value=True):
        
        command_str = build_command("to-json")
        
        assert "omo-cli to-json" in command_str
        assert "--pretty" in command_str
        assert str(CSV_FILE) in command_str
        assert output_file in command_str


def test_random_sample_command_with_options():
    """Test building the random-sample command with different options."""
    output_file = "sample.csv"
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(CSV_FILE)), \
         patch("omni_morph.omo_wizard.ask_output_path", return_value=output_file), \
         patch("omni_morph.omo_wizard.ask_int", return_value=100), \
         patch("omni_morph.omo_wizard.ask_text", side_effect=["0.1", "42"]), \
         patch("omni_morph.omo_wizard.ask_flag", return_value=False):
        
        command_str = build_command("random-sample")
        
        assert "omo-cli random-sample" in command_str
        assert "--n 100" in command_str
        assert "--fraction 0.1" in command_str
        assert "--seed" in command_str
        assert str(CSV_FILE) in command_str
        assert output_file in command_str


def test_query_command_with_options():
    """Test building the query command with different options."""
    sql_query = "SELECT * FROM userdata1 LIMIT 10"
    with patch("omni_morph.omo_wizard.ask_path", return_value=str(CSV_FILE)), \
         patch("omni_morph.omo_wizard.ask_text", side_effect=["json", sql_query]), \
         patch("omni_morph.omo_wizard.ask_flag", return_value=True):
        
        command_str = build_command("query")
        
        assert "omo-cli query" in command_str
        assert "--format json" in command_str
        assert str(CSV_FILE) in command_str
        assert sql_query in command_str
