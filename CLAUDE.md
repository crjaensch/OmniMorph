# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OmniMorph is a command-line data transformation tool and Python library for data engineers. It provides file format conversion, data inspection, statistical analysis, and SQL querying capabilities for CSV, JSON, Avro, Parquet, and Excel (XLSX) files.

## Architecture

The project is organized into several key modules:

- **`omni_morph/data/`**: Core data processing modules
  - `converter.py`: File format conversion (read/write/convert functions)
  - `extractor.py`: Data extraction (head/tail/sample operations)
  - `statistics.py`: Statistical analysis with t-digest for large files
  - `query_engine.py`: SQL query execution via DuckDB with AI-powered error suggestions
  - `merging.py`: Multi-file merging with schema reconciliation
  - `filesystems.py`: File system abstractions including Azure ADLS Gen2 support
  - `formats.py`: Format detection and enum definitions
  - `_io.py`: Low-level I/O operations with performance optimizations

- **`omni_morph/utils/`**: Utility functions
  - `file_utils.py`: Schema extraction and metadata utilities
  - `json2md.py`: JSON to markdown conversion for readable statistics output

- **CLI Entry Points**:
  - `omo_cli.py`: Main CLI application using Typer
  - `omo_wizard.py`: Interactive wizard with InquirerPy for guided operations

## Development Commands

### Installation and Environment Setup
```bash
# Install dependencies
poetry install

# Build wheel package
poetry build

# Run commands in development mode
poetry run omo-cli --help
poetry run omo-wizard
```

### Testing
```bash
# Run all tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov omni_morph --cov-report xml

# Run specific test files
poetry run pytest test/unit/test_io.py
poetry run pytest test/integration/test_cli.py
```

### Code Quality
```bash
# Run linting
poetry run flake8 omni_morph/

# Run type checking
poetry run mypy omni_morph/

# Run import sorting
poetry run isort omni_morph/
```

## Key Technical Details

### Performance Optimizations
- **Memory-efficient processing**: Uses chunked processing for large files
- **Column projection**: Reads only needed columns to reduce I/O
- **Predicate push-down**: Filters applied at storage layer for Parquet
- **Memory mapping**: Enabled for Parquet files to avoid data copies
- **Multi-threading**: Leverages PyArrow's parallel processing capabilities

### File Format Support
- **CSV**: Custom schema inference with sampling
- **JSON**: GenSON-based schema generation
- **Avro**: Native schema extraction from embedded metadata
- **Parquet**: Schema from embedded metadata with advanced filtering
- **Excel (XLSX)**: Read/write via pandas & openpyxl with Arrow conversion

### Statistical Analysis
- Uses t-digest algorithm for approximate quantiles on large datasets
- Provides both detailed and fast (DuckDB-based) statistics modes
- Handles both numeric and categorical columns with top-k frequency analysis

### SQL Query Engine
- Powered by DuckDB for in-memory analytics
- Includes AI-powered error suggestions via OpenAI API
- Supports automatic table name inference from file stems
- Validates SQL syntax before execution

### Cloud Storage Integration
- Alpha-level Azure ADLS Gen2 support via `adlfs` and `fsspec`
- Multiple authentication methods (connection string, account key, service principal)
- Transparent file system abstraction through `FileSystemHandler`

## Testing Structure

- **Unit tests** (`test/unit/`): Core functionality testing
- **Integration tests** (`test/integration/`): End-to-end CLI and wizard testing
- **Test data** (`test/data/`): Sample files in various formats with compression variants

## Common Development Patterns

### Adding New File Formats
1. Update `Format` enum in `formats.py`
2. Add read/write logic in `converter.py`
3. Update format detection in `_io.py`
4. Add schema inference in `file_utils.py`

### Performance Considerations
- Always consider memory usage for large file operations
- Use PyArrow's lazy evaluation and filtering capabilities
- Implement chunked processing for operations that don't fit in memory
- Test with large files (>1GB) to verify memory efficiency

### Error Handling
- Use custom exceptions from `exceptions.py`
- Provide helpful error messages with context
- For SQL errors, integrate with AI suggestion system when possible

## Environment Variables

- `OPENAI_API_KEY`: Required for AI-powered SQL error suggestions
- `AZURE_STORAGE_CONNECTION_STRING`: Azure ADLS Gen2 connection string
- `AZURE_STORAGE_ACCOUNT_NAME/KEY`: Azure storage account credentials
- `AZURE_TENANT_ID/CLIENT_ID/CLIENT_SECRET`: Azure service principal credentials