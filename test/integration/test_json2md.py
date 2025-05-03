import pytest
from pathlib import Path

from omni_morph.utils.file_utils import get_schema
from omni_morph.utils.json2md import schema_to_markdown, stats_to_markdown

# Test data files for three schema formats
TEST_CASES = [
    ("parquet", "userdata1.parquet"),
    ("avro", "userdata1.avro"),
    ("csv", "userdata1.csv"),
]

@ pytest.mark.parametrize("fmt, filename", TEST_CASES)
def test_schema_to_markdown_basic(fmt, filename):
    # Locate sample file
    data_dir = Path(__file__).parent.parent / "data" / "sample-data" / fmt
    file_path = data_dir / filename
    # Extract schema
    schema = get_schema(str(file_path))
    # Convert to markdown
    md = schema_to_markdown(schema)
    
    # Basic checks for structure and content
    # The actual output starts with a newline, so we check for the title in the content
    assert "Data Schema Overview" in md, "Missing title"
    
    # Check for the presence of required columns in the output
    assert "Field Name" in md, "Missing Field Name column"
    assert "Data Type" in md, "Missing Data Type column"
    assert "Nullable" in md, "Missing Nullable column"
    assert "Description" in md, "Missing Description column"
    
    # Ensure table has proper structure with data rows
    lines = [l for l in md.splitlines() if l.startswith("|")]
    assert len(lines) >= 3, f"Insufficient table rows for {fmt}: {lines}"
    
    # Check for expected field names based on the test data
    assert "registration_dttm" in md, "Missing expected field 'registration_dttm'"
    assert "id" in md, "Missing expected field 'id'"
    
    # Check for proper formatting of nullable indicators
    assert "✅" in md or "❌" in md, "Missing nullable indicators"

@ pytest.mark.parametrize("fmt, filename", TEST_CASES)
def test_stats_to_markdown(fmt, filename):
    """Test the stats_to_markdown function with mock data."""
    # Create mock statistics data
    mock_stats = {
        "numeric_col": {
            "type": "numeric",
            "count": 100,
            "min": 1.0,
            "max": 100.0,
            "mean": 50.5,
            "median": 50.0
        },
        "categorical_col": {
            "type": "categorical",
            "distinct": 5,
            "top5": [
                {"value": "A", "count": 30},
                {"value": "B", "count": 25},
                {"value": "C", "count": 20},
                {"value": "D", "count": 15},
                {"value": "E", "count": 10}
            ]
        }
    }
    
    # Convert to markdown
    md = stats_to_markdown(mock_stats)
    
    # Basic checks for structure and content
    assert "# Numeric columns" in md, "Missing numeric columns section"
    assert "# Categorical columns" in md, "Missing categorical columns section"
    
    # Check for the presence of required columns in the numeric table
    assert "column" in md, "Missing column name in numeric table"
    assert "non-null count" in md, "Missing non-null count in numeric table"
    assert "min" in md, "Missing min in numeric table"
    assert "max" in md, "Missing max in numeric table"
    assert "mean" in md, "Missing mean in numeric table"
    assert "median" in md, "Missing median in numeric table"
    
    # Check for the presence of required columns in the categorical table
    assert "distinct" in md, "Missing distinct count in categorical table"
    assert "top-5 categories" in md, "Missing top-5 categories in categorical table"
    
    # Check for the presence of the mock data values
    assert "numeric_col" in md, "Missing numeric column name"
    assert "categorical_col" in md, "Missing categorical column name"
    assert "100" in md, "Missing count value"
    assert "A · 30" in md or "A · 30" in md, "Missing top category value"
