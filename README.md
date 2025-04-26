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
  query          Run SQL queries against data files using DuckDB.
  random-sample  Randomly sample records from a file.
  schema         Print the schema for a file.
  stats          Print statistics about a file.
  tail           Print the last N records from a file.
  to-avro        Convert one file to Avro format.
  to-csv         Convert one file to CSV format.
  to-json        Convert one file to JSON format.
  to-parquet     Convert one file to Parquet format.
```

### Examples

```bash
# View the schema of a CSV file
omo-cli schema data.csv

# Get statistics about columns in a file
omo-cli stats data.csv

# Get statistics in markdown format for better readability
omo-cli stats data.csv --markdown

# Analyze specific columns only
omo-cli stats data.parquet --columns col1,col2,col3

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

# Merge multiple files into a single output file
omo-cli merge file1.csv file2.csv merged_output.csv

# Merge files of different formats (CSV and JSON) into a Parquet file
omo-cli merge data1.csv data2.json combined_data.parquet

# Run SQL queries against data files
omo-cli query data.csv "SELECT * FROM data LIMIT 10"
omo-cli query sales.parquet "SELECT category, SUM(amount) as total FROM sales GROUP BY category ORDER BY total DESC"

# Force a specific format for SQL queries
omo-cli query data.txt --format csv "SELECT * FROM data WHERE id > 100"
```

## Statistical Analysis Output Example

Here's an example of the statistics output in markdown format for the sample file `test/data/sample-data/parquet/userdata1.parquet`:

# Numeric columns

| column | non-null count | min | max | mean | median |
| ------ | ------------- | --- | --- | ---- | ------ |
| id | 1000 | 1 | 1000 | 500.50 | 500.50 |
| salary | 932 | 12380.49 | 286592.99 | 149005.36 | 147274.51 |

# Categorical columns

| column | distinct | top-5 categories (value · count) |
| ------ | -------- | ------------------------------- |
| registration_dttm | 995 | 2016-02-03 00:33:25 · 2 ; 2016-02-03 17:07:31 · 2 ; 2016-02-03 00:36:46 · 2 ; 2016-02-03 10:35:23 · 2 ; 2016-02-03 10:07:00 · 2 |
| first_name | 198 | __NULL__ · 16 ; Samuel · 11 ; Peter · 11 ; Mark · 11 ; Stephen · 10 |
| last_name | 247 | Barnes · 10 ; Willis · 9 ; Shaw · 9 ; Patterson · 9 ; Lane · 8 |
| email | 985 | __NULL__ · 16 ; ajordan0@com.com · 1 ; afreeman1@is.gd · 1 ; emorgan2@altervista.org · 1 ; driley3@gmpg.org · 1 |
| gender | 3 | Female · 482 ; Male · 451 ; __NULL__ · 67 |
| ip_address | 1000 | 1.197.201.2 · 1 ; 218.111.175.34 · 1 ; 7.161.136.94 · 1 ; 140.35.109.83 · 1 ; 169.113.235.40 · 1 |
| cc | 710 | __NULL__ · 291 ; 6759521864920116 · 1 ; 6767119071901597 · 1 ; 3576031598965625 · 1 ; 5602256255204850 · 1 |
| country | 120 | China · 189 ; Indonesia · 97 ; Russia · 62 ; Philippines · 45 ; Portugal · 38 |
| birthdate | 788 | __NULL__ · 197 ; 1/28/1997 · 2 ; 7/21/1986 · 2 ; 4/10/1965 · 2 ; 11/18/1958 · 2 |
| title | 182 | __NULL__ · 197 ; Electrical Engineer · 15 ; Structural Analysis Engineer · 13 ; Senior Cost Accountant · 12 ; Senior Sales Associate · 12 |

This human-readable format makes it easy to quickly understand the characteristics of your data.

## SQL Queries with DuckDB

OmniMorph includes a powerful SQL query engine powered by DuckDB that allows you to run SQL queries directly against data files without needing to set up a database.

### Features

- Run SQL queries against CSV, JSON, Avro, and Parquet files
- Results displayed as nicely formatted markdown tables
- Automatic schema inference from data files
- AI-powered query suggestions when SQL validation fails

### Example

```bash
# Basic query with limit
omo-cli query userdata.csv "SELECT id, first_name, last_name FROM userdata LIMIT 5"

# Aggregation query with grouping
omo-cli query sales.parquet "SELECT category, COUNT(*) as count, SUM(amount) as total FROM sales GROUP BY category"
```

### AI-Powered SQL Assistance

When you run an invalid SQL query, OmniMorph doesn't just show an error - it uses AI to suggest a corrected query based on the error message and the schema of your data file:

```
❌ SQL validation failed:
Catalog Error: Table with name sales_data does not exist!

💡 Suggested fix:

Try using the correct table name. The file's stem is used as the table name:

SELECT * FROM sales LIMIT 10
```

This feature requires an OpenAI API key set in the `OPENAI_API_KEY` environment variable.

## Python example usage:
```python
from omni_morph.data import convert, Format, head, tail, sample, get_stats
from omni_morph.data.merging import merge_files
from omni_morph.utils.file_utils import get_schema, get_metadata
from pathlib import Path
import pyarrow as pa

# ===== File Format Conversion =====

# Convert between different file formats
convert("my_file.avro", "my_file.parquet")          # Avro  → Parquet
convert("data.parquet", "out/data.csv")             # Parquet → CSV
convert("records.json", "records_converted.avro")   # JSON   → Avro

# Write PyArrow tables directly
table = pa.table({"x": [1, 2, 3]})
write(table, Path("my_table.avro"), Format.AVRO)

# ===== Data Inspection =====

# Extract first/last records from files
first_records = head("data.csv", 10)                # Get first 10 records as PyArrow Table
last_records = tail("data.parquet", 5, return_type="pandas")  # Get last 5 records as Pandas DataFrame

# Random sampling of records
sample_n = sample("data.csv", n=50, seed=42)        # Sample exactly 50 records
sample_frac = sample("data.parquet", fraction=0.1)  # Sample approximately 10% of records

# ===== Schema and Metadata =====

# Extract schema from a file
schema = get_schema("data.csv")                     # Auto-detect format from extension
schema = get_schema("data.file", fmt=Format.CSV)    # Explicitly specify format

# Get file metadata
metadata = get_metadata("data.parquet")             # File size, record count, etc.

# ===== Statistical Analysis =====

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

# ===== File Merging =====

# Merge multiple files into a single output file
merge_files(
    sources=["file1.csv", "file2.csv", "file3.csv"],
    output_path="merged_output.csv",
    progress=True  # Show progress during merge
)

# Merge files with different schemas (with automatic casting)
merge_files(
    sources=["data1.parquet", "data2.avro", "data3.json"],
    output_path="merged_data.parquet",
    output_fmt=Format.PARQUET,  # Explicitly specify output format
    allow_cast=True,  # Enable schema reconciliation
    chunksize=100_000  # Process in chunks of 100K rows for memory efficiency
)

# ===== SQL Queries =====

# Execute SQL queries against data files
from omni_morph.data.query_engine import query, validate_sql

# Run a query and get results as a PyArrow table
result_table = query(
    "SELECT country, COUNT(*) as count FROM userdata GROUP BY country ORDER BY count DESC",
    "data/userdata.csv",
    return_type="arrow"
)

# Run a query and get results as a pandas DataFrame
result_df = query(
    "SELECT AVG(salary) as avg_salary, gender FROM employees GROUP BY gender",
    "data/employees.parquet",
    return_type="pandas"
)

# Validate SQL syntax without executing the query
error = validate_sql(
    "SELECT * FROM data WHERE id = ?",
    "data.csv"
)
if error:
    print(f"SQL validation error: {error}")
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