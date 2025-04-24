# OmniMorph
Transform, inspect, and merge data files with a single command-line Swiss Army knife for data engineers

<p align="left">
  <img src="assets/omnimorph-logo.png" alt="OmniMorph Logo" width="500"/>
</p>

## Installation (WIP)

```bash
# Install using Poetry (recommended)
poetry install

# Or using pip
pip install omni_morph
```

## Usage

```bash
poetry run omo-cli --help
```

```text
Usage: omo-cli [OPTIONS] COMMAND [ARGS]...

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
  head           Print the first N records from a file.
  merge          Merge multiple files of the same or different formats...
  meta           Print the metadata of a file 
                 (JSON response with keys: file_size, created, modified, encoding, num_records, format).
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
omo-cli schema data.csv

# Get statistics about columns in a file
omo-cli stats data.csv

# Analyze specific columns only
omo-cli stats data.parquet --columns col1 col2 col3

# Force a specific format
omo-cli stats data.txt --format csv

# Adjust the sample size for t-digest median approximation
omo-cli stats large_data.parquet --sample-size 5000

# Convert from one format to another
omo-cli to-json data.csv output.json

# Randomly sample records from a file
omo-cli random-sample data.csv --n 50 --seed 42
omo-cli random-sample data.parquet --fraction 0.1

# View file metadata
omo-cli meta data.csv
```

## Python example usage:
```python
from omni_morph.data import convert, Format, head, tail, sample, get_stats

# one-liner convenience
convert("my_file.avro", "my_file.parquet")          # Avro  → Parquet
convert("data.parquet", "out/data.csv")             # Parquet → CSV
convert("records.json", "records_converted.avro")   # JSON   → Avro

# Extract first/last records from files
first_records = head("data.csv", 10)                # Get first 10 records as PyArrow Table
last_records = tail("data.parquet", 5, return_type="pandas")  # Get last 5 records as Pandas DataFrame

# Random sampling of records
sample_n = sample("data.csv", n=50, seed=42)        # Sample exactly 50 records
sample_frac = sample("data.parquet", fraction=0.1)  # Sample approximately 10% of records

# Get column statistics from files
stats = get_stats("data.csv")                       # Get stats for all columns
stats = get_stats("data.parquet", columns=["col1", "col2"])  # Get stats for specific columns
stats = get_stats("data.txt", fmt=Format.CSV)       # Force a specific format
stats = get_stats("large_data.parquet", sample_size=5000)  # Adjust t-digest sample size

# Numeric columns include: min, max, mean, median
print(f"Mean of col1: {stats['col1']['mean']}")
print(f"Median of col1: {stats['col1']['median']}")

# Categorical columns include: distinct count, top values
print(f"Distinct values in col2: {stats['col2']['distinct_count']}")
print(f"Most common value: {stats['col2']['top_values'][0][0]}")

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

## Large File Processing

OmniMorph is optimized for processing very large data files:

- **Parquet Files**: Memory-efficient processing for files >10GB using intelligent row group handling
- **Avro Files**: Two-pass sampling approach for files >1GB that maintains constant memory usage
- **Statistics Computation**: The `get_stats()` function processes files in chunks, maintaining constant memory usage even for very large files

These optimizations apply to the `tail()`, `sample()`, and `get_stats()` functions (and their CLI counterparts), making OmniMorph suitable for working with datasets in the 10-100GB range without running out of memory.

## Schema Inference

OmniMorph provides robust schema inference for all supported file formats:

- CSV: Custom inference engine that samples rows to determine column types
- JSON: Uses GenSON to generate JSON Schema
- Avro: Extracts embedded schema
- Parquet: Extracts embedded schema