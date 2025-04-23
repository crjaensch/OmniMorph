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
omni-morph-cli -h
```

```bash
usage: omni-morph-cli [-h] {count, head, tail, meta, merge, random-sample, schema, stats, validate, to-json, to-csv, to-avro, to-parquet} ...

positional arguments:
  {count, head, tail, meta, merge, random-sample, schema, stats, validate, to-json, to-csv, to-avro, to-parquet} ...

commands
    count               Count the number of records in a file
    head                Print the first N records from a file
    tail                Print the last N records from a file
    meta                Print a file's metadata
    merge               Merge multiple files of the same or different formats into one (-t target-format)
    random-sample       Randomly sample records from a file
    schema              Print the schema for a file
    stats               Print statistics about a file
    validate            Validate a file
    to-json             Convert one file to JSON format
    to-csv              Convert one file to CSV format
    to-avro             Convert one file to Avro format
    to-parquet          Convert one file to Parquet format
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