# OmniMorph
Transform, inspect, and merge data files with a single command-line Swiss Army knife for data engineers

<p align="left">
  <img src="assets/omnimorph-logo.png" alt="OmniMorph Logo" width="500"/>
</p>

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Command-Line Interface](#command-line-interface)
  - [Available Commands](#available-commands)
  - [Command Examples](#command-examples)
- [Interactive Wizard](#interactive-wizard)
  - [When to Use Wizard vs CLI](#when-to-use-wizard-vs-cli)
  - [Example Wizard Session](#example-wizard-session)
- [Features](#features)
  - [Performance Optimizations](#performance-optimizations)
  - [Statistical Analysis](#statistical-analysis)
  - [SQL Queries with DuckDB](#sql-queries-with-duckdb)
  - [AI-Powered SQL Assistance](#ai-powered-sql-assistance)
  - [Large File Processing](#large-file-processing)
  - [Schema Inference](#schema-inference)
- [Python API](#python-api)
  - [File Format Conversion](#file-format-conversion)
  - [Data Inspection](#data-inspection)
  - [Schema and Metadata](#schema-and-metadata)
  - [Statistical Analysis (API)](#statistical-analysis-api)
  - [File Merging](#file-merging)
  - [SQL Queries (API)](#sql-queries-api)
- [Supported File Formats](#supported-file-formats)
- [Alpha Features](#alpha-features)
  - [Azure ADLS Gen2 Support](#azure-adls-gen2-support)

## Installation

There are two main ways to install and use OmniMorph:

### Option 1: Using Poetry (Development Mode)

```bash
# Install dependencies with Poetry
poetry install

# Run commands with the poetry run prefix
poetry run omo-cli --help
poetry run omo-wizard
```

### Option 2: Build and Install as a Python Package

```bash
# Optional: Create and activate a virtual environment first
python3 -m venv .omnimorph
source .omnimorph/bin/activate  # On Windows: .omnimorph\Scripts\activate

# Install dependencies and build the wheel file
poetry install
poetry build  # Creates .whl file in the 'dist' directory

# Install the wheel file
pip install dist/omni_morph-*.whl

# Run commands directly (no prefix needed)
omo-cli --help
omo-wizard
```

With Option 2, all commands can be used directly without the `poetry run` prefix.

## Quick Start

```bash
# View command help
poetry run omo-cli --help

# View the schema of a CSV file
poetry run omo-cli schema data.csv

# Convert from one format to another
poetry run omo-cli to-json data.csv output.json

# Run SQL queries against data files
poetry run omo-cli query data.csv "SELECT * FROM data LIMIT 10"

# Launch the interactive wizard
poetry run omo-wizard
```

## Command-Line Interface

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
  tail           Print the last N records from a file.
  meta           Print the metadata of a file (file_size, created, modified, encoding, num_records, format).
  schema         Print the schema for a file.
  stats          Print statistics about a file.
  query          Run SQL queries against data files using DuckDB.
  random-sample  Randomly sample records from a file.
  to-avro        Convert one file to Avro format.
  to-csv         Convert one file to CSV format.
  to-json        Convert one file to JSON format.
  to-parquet     Convert one file to Parquet format.
  merge          Merge multiple files of the same or different formats...
```

### Command Examples

```bash
# View the schema of a CSV file
poetry run omo-cli schema data.csv

# Get statistics about columns in a file
poetry run omo-cli stats data.csv

# Get statistics in markdown format for better readability
poetry run omo-cli stats data.csv --markdown

# Analyze specific columns only
poetry run omo-cli stats data.parquet --columns col1,col2,col3

# Force a specific format
poetry run omo-cli stats data.txt --format csv

# Adjust the sample size for t-digest median approximation
poetry run omo-cli stats large_data.parquet --sample-size 5000

# Use DuckDB's fast statistics generation (only compatible with --format)
poetry run omo-cli stats --fast data.csv

# Convert from one format to another
poetry run omo-cli to-json data.csv output.json

# Randomly sample records from a file
poetry run omo-cli random-sample data.csv --n 50 --seed 42
poetry run omo-cli random-sample data.parquet --fraction 0.1

# View file metadata
poetry run omo-cli meta data.csv

# Merge multiple files into a single output file
poetry run omo-cli merge file1.csv file2.csv merged_output.csv

# Merge files of different formats (CSV and JSON) into a Parquet file
poetry run omo-cli merge data1.csv data2.json combined_data.parquet

# Run SQL queries against data files
poetry run omo-cli query data.csv "SELECT * FROM data LIMIT 10"
poetry run omo-cli query sales.parquet "SELECT category, SUM(amount) as total FROM sales GROUP BY category ORDER BY total DESC"

# Force a specific format for SQL queries
poetry run omo-cli query data.txt --format csv "SELECT * FROM data WHERE id > 100"
```

## Interactive Wizard

In addition to the command-line interface, OmniMorph offers an interactive wizard that guides you through building and executing commands step by step.

```bash
poetry run omo-wizard
```

The wizard provides:

- **Interactive command selection**: Choose from all available commands with arrow-key navigation
- **File memory feature**: Remember a data file path across multiple commands with `remember file` and `forget file`
- **Guided parameter input**: The wizard prompts for each parameter with helpful descriptions
- **File path selection**: Navigate your file system to select input and output files
- **Command preview**: See the full command before execution
- **Confirmation step**: Review and confirm before running each command

### When to Use Wizard vs CLI

**Use the wizard when:**
- You're new to OmniMorph and learning the available commands
- You need help remembering command options and parameters
- You prefer interactive selection of files and options
- You want to explore OmniMorph's capabilities without memorizing syntax
- You're working with the same data file across multiple operations

**Use the CLI when:**
- You need to automate tasks in scripts
- You're familiar with the commands and want faster execution
- You need to process multiple files in batch operations
- You're integrating OmniMorph into other workflows

### Example Wizard Session

```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃                             OmniMorph Wizard 🤖                              ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
? Choose a command |
> remember file
head
tail
meta
schema
stats
query
random-sample
to-avro
to-csv
to-json
to-parquet
merge
QUIT

Arrow keys to move • Enter to select • CTRL-C to cancel
```

### File Memory Feature

The wizard now includes a file memory feature that allows you to remember a data file path across multiple commands:

1. **Remember a file**: Select the `remember file` command to store a file path for subsequent operations
2. **Use the remembered file**: When executing commands that require a file path, you'll be prompted to use the remembered file
3. **Forget a file**: When you're done working with a file, select `forget file` to clear the remembered path

This feature is especially useful when performing multiple operations on the same data file, such as checking the schema, running statistics, and executing queries in sequence.

## Features

### Performance Optimizations

OmniMorph is optimized for high-performance data processing:

- **Memory-efficient processing**: Processes data in chunks to handle files of any size with minimal memory footprint
- **Multi-threaded operations**: Leverages multiple CPU cores for faster processing when available
- **Memory mapping**: Uses memory mapping for Parquet files to avoid unnecessary data copies
- **Column projection**: Reads only the columns you need, significantly reducing I/O and memory usage
- **Predicate push-down**: Filters data at the storage layer for Parquet files, keeping undesired data on disk
- **Optimized compression**: Uses zstd compression for Parquet files, offering 20-40% smaller files with faster reads
- **Schema memoization**: Caches schema conversions to avoid redundant computations for nested schemas

### Statistical Analysis

OmniMorph provides comprehensive statistical analysis of your data files. Here's an example of the statistics output in markdown format for a sample file:

#### Numeric columns

| column | non-null count | min | max | mean | median |
| ------ | ------------- | --- | --- | ---- | ------ |
| id | 1000 | 1 | 1000 | 500.50 | 500.50 |
| salary | 932 | 12380.49 | 286592.99 | 149005.36 | 147274.51 |

#### Categorical columns

| column | distinct | top-5 categories (value · count) |
| ------ | -------- | ------------------------------- |
| registration_dttm | 995 | 2016-02-03 00:33:25 · 2 ; 2016-02-03 17:07:31 · 2 ; 2016-02-03 00:36:46 · 2 ; 2016-02-03 10:35:23 · 2 ; 2016-02-03 10:07:00 · 2 |
| first_name | 198 | __NULL__ · 16 ; Samuel · 11 ; Peter · 11 ; Mark · 11 ; Stephen · 10 |
| last_name | 247 | Barnes · 10 ; Willis · 9 ; Shaw · 9 ; Patterson · 9 ; Lane · 8 |
| gender | 3 | Female · 482 ; Male · 451 ; __NULL__ · 67 |

This human-readable format makes it easy to quickly understand the characteristics of your data.

For large datasets, you can use the `--fast` option which leverages DuckDB's built-in summarization capabilities:

```bash
poetry run omo-cli stats --fast large_data.parquet
```

The fast option provides similar statistics but processes data much more efficiently. Note that when using `--fast`, only the `--format` option is compatible; other options like `--columns` and `--sample-size` cannot be used.

### SQL Queries with DuckDB

OmniMorph includes a powerful SQL query engine powered by DuckDB that allows you to run SQL queries directly against data files without needing to set up a database.

Features:
- Run SQL queries against CSV, JSON, Avro, and Parquet files
- Results displayed as nicely formatted markdown tables
- Automatic schema inference from data files

Example:
```bash
# Basic query with limit
poetry run omo-cli query userdata.csv "SELECT id, first_name, last_name FROM userdata LIMIT 5"

# Aggregation query with grouping
poetry run omo-cli query sales.parquet "SELECT category, COUNT(*) as count, SUM(amount) as total FROM sales GROUP BY category"
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

You can directly run the suggested SQL query without having to copy and paste it, as shown in the example session below:

```
? Choose a command query
Format detected from file extension, skipping format prompt
? Use remembered file: ../test_omni-morph/yt_2025_01.parquet? Yes
? sql_query (SQL query) Output average and maximum fare amount as well as tip amount grouped by payment method and sort by decreasing fare amount
? Proceed? Yes
─ Executing omo-cli query ../test_omni-morph/yt_2025_01.parquet 'Output average and maximum fare amount as well as tip amount grouped by …

Command Output:
────────────────────────────────────────────────────────────────────────────────
❌ SQL validation failed:
Parser Error: syntax error at or near "Output"

💡 Suggested fix:
The error occurs because the text you wrote is a natural language description, not a valid SQL statement. You need to write a proper SQL query instead.

Here is a corrected SQL query that outputs the average and maximum fare amount and tip amount grouped by payment method, sorted by decreasing average fare amount:

SELECT
  payment_type,
  AVG(fare_amount) AS avg_fare_amount,
  MAX(fare_amount) AS max_fare_amount,
  AVG(tip_amount) AS avg_tip_amount,
  MAX(tip_amount) AS max_tip_amount
FROM yt_2025_01
GROUP BY payment_type
ORDER BY avg_fare_amount DESC;

Fix: Replace the natural language description with a valid SQL `SELECT` statement.
────────────────────────────────────────────────────────────────────────────────
AI suggested a SQL query fix.
? Would you like to run the suggested SQL query? Yes
Will run: omo-cli query ../test_omni-morph/yt_2025_01.parquet 'SELECT
  payment_type,
  AVG(fare_amount) AS avg_fare_amount,
  MAX(fare_amount) AS max_fare_amount,
  AVG(tip_amount) AS avg_tip_amount,
  MAX(tip_amount) AS max_tip_amount
FROM yt_2025_01
GROUP BY payment_type
ORDER BY avg_fare_amount DESC'
─ Executing omo-cli query ../test_omni-morph/yt_2025_01.parquet 'SELECT payment_type, AVG(fare_amount) AS avg_fare_amount, MAX(fare_amount) AS max_fare_amount, …
Command Output:
────────────────────────────────────────────────────────────────────────────────

|   payment_type |   avg_fare_amount |   max_fare_amount |   avg_tip_amount |   max_tip_amount |
|---------------:|------------------:|------------------:|-----------------:|-----------------:|
|              1 |          18.0085  |            936.8  |       4.10607    |           400    |
|              2 |          16.6031  |           2450.9  |       0.00271363 |            20.42 |
|              0 |          14.5875  |            503.59 |       0.452353   |            40    |
|              4 |          11.434   |         863372    |       0.0439846  |           123.34 |
|              3 |           4.49818 |            900    |       0.0168658  |            20    |
|              5 |           0       |              0    |       0          |             0    |
```

This feature requires an OpenAI API key set in the `OPENAI_API_KEY` environment variable.

### Large File Processing

OmniMorph is optimized for processing very large data files:

- **Parquet Files**: Memory-efficient processing for files >10GB using intelligent row group handling
- **Avro Files**: Two-pass sampling approach for files >1GB that maintains constant memory usage
- **Statistics Computation**: The `get_stats()` function processes files in chunks, maintaining constant memory usage even for very large files

These optimizations apply to the `tail()`, `sample()`, and `get_stats()` functions (and their CLI counterparts), making OmniMorph suitable for working with datasets in the 10-100GB range without running out of memory.

### Schema Inference

OmniMorph provides robust schema inference for all supported file formats:

- **CSV**: Custom inference engine that samples rows to determine column types
- **JSON**: Uses GenSON to generate JSON Schema
- **Avro**: Extracts embedded schema
- **Parquet**: Extracts embedded schema

## Python API

OmniMorph can also be used as a Python library in your own projects.

### File Format Conversion

```python
import pyarrow as pa
from omni_morph.data import convert, read, write, Format

# Basic conversion between formats
convert("data.csv", "data.parquet")                 # CSV → Parquet
convert("data.parquet", "out/data.csv")             # Parquet → CSV
convert("records.json", "records_converted.avro")   # JSON → Avro


# Explicit format specification
convert("data.txt", "data.json", src_fmt=Format.CSV, dst_fmt=Format.JSON)   # CSV → JSON

# Performance optimizations
# Read only specific columns (reduces I/O and memory usage)
table = read("large_data.parquet", columns=["id", "name", "value"])

# Use predicate push-down for Parquet files (filters data at storage layer)
import pyarrow.dataset as ds
filter_expr = ds.field("date") > "2025-01-01"
table = read("large_data.parquet", filters=filter_expr, use_dataset=True)

# Enable memory mapping and multi-threading
table = read("large_data.parquet", use_threads=True, memory_map=True)

# Write with optimized compression
write(table, "optimized_data.parquet", compression="zstd")

# Complete conversion with all performance options
convert(
    "large_data.csv", 
    "filtered_data.parquet",
    columns=["id", "date", "value"], 
    use_threads=True,
    use_dataset=True,
    compression="zstd"
)
```

### Data Inspection

```python
from omni_morph.data import head, tail, sample

# Extract first/last records from files
first_records = head("data.csv", 10)                # Get first 10 records as PyArrow Table
last_records = tail("data.parquet", 5, return_type="pandas")  # Get last 5 records as Pandas DataFrame

# Random sampling of records
sample_n = sample("data.csv", n=50, seed=42)        # Sample exactly 50 records
sample_frac = sample("data.parquet", fraction=0.1)  # Sample approximately 10% of records
```

### Schema and Metadata

```python
from omni_morph.utils.file_utils import get_schema, get_metadata
from omni_morph.data import Format

# Extract schema from a file
schema = get_schema("data.csv")                     # Auto-detect format from extension
schema = get_schema("data.file", fmt=Format.CSV)    # Explicitly specify format

# Get file metadata
metadata = get_metadata("data.parquet")             # File size, record count, etc.
```

### Statistical Analysis (API)

```python
from omni_morph.data import get_stats, Format

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
```

### File Merging

```python
from omni_morph.data.merging import merge_files
from omni_morph.data import Format

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
```

### SQL Queries (API)

```python
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

- **CSV** (.csv)
- **JSON** (.json)
- **Avro** (.avro)
- **Parquet** (.parquet)

## Alpha Features

These features are in early development and may not be fully stable. Use with caution in production environments.

### Azure ADLS Gen2 Support

OmniMorph now includes alpha-level support for Azure Data Lake Storage Gen2. This allows you to read and write files directly from/to Azure ADLS Gen2 storage.

#### Authentication Methods

OmniMorph supports multiple authentication methods for Azure ADLS Gen2:

1. **Connection String**
2. **Account Key**
3. **Service Principal**

#### CLI Usage

```bash
# Using connection string
poetry run omo-cli --azure-connection-string "DefaultEndpointsProtocol=https;..." head abfss://container@account.dfs.core.windows.net/path/to/file.csv

# Using account key
poetry run omo-cli --azure-account-name "myaccount" --azure-account-key "mykey" to-parquet abfss://container@account.dfs.core.windows.net/path/to/file.csv output.parquet

# Using service principal
poetry run omo-cli --azure-tenant-id "tenant-id" --azure-client-id "client-id" --azure-client-secret "client-secret" query abfss://container@account.dfs.core.windows.net/path/to/file.csv "SELECT * FROM file LIMIT 10"
```

#### Environment Variables

You can also set Azure credentials using environment variables:

```bash
# Set Azure credentials as environment variables
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
# or
export AZURE_STORAGE_ACCOUNT_NAME="myaccount"
export AZURE_STORAGE_ACCOUNT_KEY="mykey"
# or
export AZURE_TENANT_ID="tenant-id"
export AZURE_CLIENT_ID="client-id"
export AZURE_CLIENT_SECRET="client-secret"

# Then run commands without specifying credentials
poetry run omo-cli head abfss://container@account.dfs.core.windows.net/path/to/file.csv
```

#### Python API Usage

```python
from omni_morph.data.converter import read, write, convert
from omni_morph.data.filesystems import FileSystemHandler

# Set Azure credentials
FileSystemHandler.set_azure_credentials({
    "azure_connection_string": "DefaultEndpointsProtocol=https;..."
})

# Read from Azure ADLS Gen2
table = read("abfss://container@account.dfs.core.windows.net/path/to/file.csv")

# Write to Azure ADLS Gen2
write(table, "abfss://container@account.dfs.core.windows.net/path/to/output.parquet")

# Convert between formats
convert("abfss://container@account.dfs.core.windows.net/path/to/file.csv", 
       "abfss://container@account.dfs.core.windows.net/path/to/output.parquet")
```

> **Note:** This feature is currently in alpha. Some advanced operations like random sampling on very large files might not be optimized for cloud storage yet.