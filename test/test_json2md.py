import pytest
from pathlib import Path

from omni_morph.utils.file_utils import get_schema
from omni_morph.utils.json2md import schema_to_markdown

# Test data files for three schema formats
TEST_CASES = [
    ("parquet", "userdata1.parquet"),
    ("avro", "userdata1.avro"),
    ("csv", "userdata1.csv"),
]

@ pytest.mark.parametrize("fmt, filename", TEST_CASES)
def test_schema_to_markdown_basic(fmt, filename):
    # Locate sample file
    data_dir = Path(__file__).parent / "data" / "sample-data" / fmt
    file_path = data_dir / filename
    # Extract schema
    schema = get_schema(str(file_path))
    # Convert to markdown
    md = schema_to_markdown(schema)
    # Basic checks
    assert md.startswith("# 📦 Data Schema Overview"), "Missing title"
    assert "| Field Name | Data Type | Nullable | Description |" in md, "Missing table header"
    # Ensure at least one field row exists (header + at least one row)
    lines = [l for l in md.splitlines() if l.startswith("|")]
    # two header lines + at least one data row
    assert len(lines) >= 3, f"Insufficient table rows for {fmt}: {lines}"
