# OmniMorph
Transform, inspect, and merge data files with a single command-line Swiss Army knife for data engineers

## Installation

```bash
# Install using Poetry (recommended)
poetry install

# Or using pip
pip install omni_morph
```

## Usage

```bash
omni-morph-cli --help
```

```text
Usage: omni-morph-cli [OPTIONS] COMMAND [ARGS]...

  Transform, inspect, and merge data files with a single command-line tool

Options:
  -v, --verbose                   Enable info logging
  -d, --debug                     Enable debug logging
  --version                       Show version and exit
  --install-completion [bash|zsh|fish|powershell|pwsh]
                                  Install completion for the specified shell.
  --show-completion [bash|zsh|fish|powershell|pwsh]
                                  Show completion for the specified shell, to
                                  copy it or customize the installation.
  --help                          Show this message and exit.

Commands:
  count          Count the number of records in a file.
  head           Print the first N records from a file.
  merge          Merge multiple files of the same or different formats...
  meta           Print the metadata of a file.
  random-sample  Randomly sample records from a file.
  schema         Print the schema for a file.
  stats          Print statistics about a file.
  tail           Print the last N records from a file.
  to-avro        Convert one file to Avro format.
  to-csv         Convert one file to CSV format.
  to-json        Convert one file to JSON format.
  to-parquet     Convert one file to Parquet format.
  validate       Validate a file against a schema.
```

### Examples

```bash
# View the schema of a CSV file
omni-morph-cli schema data.csv

# Count records in a Parquet file
omni-morph-cli count data.parquet

# Convert from one format to another
omni-morph-cli to-json data.csv output.json
```

## Python example usage:
```python
from omni_morph.data import convert, Format, head, tail

# one-liner convenience
convert("my_file.avro", "my_file.parquet")          # Avro  → Parquet
convert("data.parquet", "out/data.csv")             # Parquet → CSV
convert("records.json", "records_converted.avro")   # JSON   → Avro

# Extract first/last records from files
first_records = head("data.csv", 10)                # Get first 10 records as PyArrow Table
last_records = tail("data.parquet", 5, return_type="pandas")  # Get last 5 records as Pandas DataFrame

# or the enum-based flavour (handy for in-memory tables)
from pathlib import Path
import pyarrow as pa

table = pa.table({"x": [1, 2, 3]})
from omni_morph.data import write
write(table, Path("my_table.avro"), Format.AVRO)

# Extract schema from a file
from omni_morph.utils.file_utils import get_schema

# Automatically detect format from file extension
schema = get_schema("data.csv")

# Or specify format explicitly
schema = get_schema("data.file", fmt=Format.CSV)
```

## Supported File Formats

- CSV (.csv)
- JSON (.json)
- Avro (.avro)
- Parquet (.parquet)

## Schema Inference

OmniMorph provides robust schema inference for all supported file formats:

- CSV: Custom inference engine that samples rows to determine column types
- JSON: Uses jsonschema-extractor to generate JSON Schema
- Avro: Extracts embedded schema
- Parquet: Extracts embedded schema