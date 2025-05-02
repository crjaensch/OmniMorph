import logging
from pathlib import Path
import importlib.metadata
import json
from omni_morph.data.converter import read, convert, Format, write
from omni_morph.data.extractor import head as extract_head, tail as extract_tail, sample as extract_sample
from omni_morph.data.statistics import get_stats
from omni_morph.data.merging import merge_files
from omni_morph.data.query_engine import query as run_query, validate_sql, ai_suggest
from omni_morph.utils.file_utils import get_schema
import typer
import pyarrow as pa

DEFAULT_RECORDS = 20

app = typer.Typer(help="Transform, inspect, and merge data files with a single command-line tool")

@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable info logging"),
    debug: bool = typer.Option(False, "--debug", "-d", help="Enable debug logging"),
    version: bool = typer.Option(False, "--version", help="Show version and exit"),
):
    if version:
        version_str = importlib.metadata.version("omni_morph")
        typer.echo(version_str)
        raise typer.Exit()
    level = logging.WARNING
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    logging.basicConfig(level=level)

@app.command()
def head(file_path: Path = typer.Argument(..., help="Path to the input file"),
         n: int = typer.Option(DEFAULT_RECORDS, "-n", "--number", help="Number of records to display")):
    """
    Print the first N records from a file.
    """
    try:
        table = extract_head(str(file_path), n)
        for row in table.to_pylist():
            typer.echo(row)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def tail(file_path: Path = typer.Argument(..., help="Path to the input file"),
         n: int = typer.Option(DEFAULT_RECORDS, "-n", "--number", help="Number of records to display")):
    """
    Print the last N records from a file.
    """
    try:
        table = extract_tail(str(file_path), n)
        for row in table.to_pylist():
            typer.echo(row)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def meta(file_path: Path = typer.Argument(..., help="Path to the input file")):
    """
    Print the metadata of a file.
    """
    try:
        from omni_morph.utils.file_utils import get_metadata
        metadata = get_metadata(str(file_path))
        typer.echo(json.dumps(metadata, default=str, indent=2))
    except Exception as e:
        typer.echo(f"Error extracting metadata: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def schema(file_path: Path = typer.Argument(..., help="Path to the input file"),
         markdown: bool = typer.Option(False, "--markdown", help="Output in markdown format instead of JSON")):
    """
    Print the schema for a file.
    """
    try:
        # Import here to avoid import errors for other commands
        schema = get_schema(str(file_path))
        
        # Output the results based on format preference
        if markdown:
            from omni_morph.utils.json2md import schema_to_markdown
            typer.echo(schema_to_markdown(schema))
        else:
            typer.echo(json.dumps(schema, indent=2))
    except Exception as e:
        typer.echo(f"Error extracting schema: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def stats(
    file_path: Path = typer.Argument(..., help="Path to the input file"),
    columns: str = typer.Option(None, "--columns", "-c", help="Specific columns to analyze (comma-separated)"),
    format: str = typer.Option(None, "--format", "-f", help="Force specific format (avro, parquet, csv, json)"),
    sample_size: int = typer.Option(2048, "--sample-size", help="Number of samples for t-digest reservoir per column"),
    markdown: bool = typer.Option(False, "--markdown", help="Output in markdown format instead of JSON"),
    fast: bool = typer.Option(False, "--fast", help="Use DuckDB's Summarize for faster statistics generation"),
):
    """
    Print statistics about a file's columns.
    
    For numeric columns: min, max, mean, and approximate median.
    For categorical columns: distinct count and top 5 values.
    """
    try:
        # Validate option combinations
        if fast and (columns or sample_size != 2048 or markdown):
            typer.echo("Error: When using --fast, the options --columns, --sample-size, and --markdown cannot be used.", err=True)
            typer.echo("Use either 'stats --fast [--format FORMAT] FILE' or 'stats [--markdown] [--columns COLS] [--sample-size N] [--format FORMAT] FILE'", err=True)
            raise typer.Exit(code=1)
            
        # If fast option is enabled, use DuckDB's Summarize
        if fast:
            # Extract the table name from the file path (stem without extension)
            table_name = Path(file_path).stem
            
            # Build the SQL query
            sql_query = f"SUMMARIZE {table_name};"
            
            # Use the query engine to execute the query
            from omni_morph.data.query_engine import query
            import tempfile
            import os
            
            # Force format if provided
            fmt = Format(format) if format else None
            
            # Execute the query and get the results as a pandas DataFrame
            result_df = query(sql_query, file_path, fmt=fmt, return_type="pandas")
            
            # Save to a temporary markdown file for processing
            with tempfile.NamedTemporaryFile(suffix='.md', delete=False) as temp_file:
                temp_path = temp_file.name
                # Convert DataFrame to markdown and save
                with open(temp_path, 'w') as f:
                    f.write(result_df.to_markdown(index=False))
            
            try:
                # Convert the summary to our format
                from omni_morph.utils.convert_summary import convert_summary
                result_md = convert_summary(temp_path)
                
                # Output the results
                typer.echo(result_md)
            finally:
                # Clean up the temporary file
                if os.path.exists(temp_path):
                    os.remove(temp_path)
        else:
            # Convert format string to Format enum if provided
            fmt = Format(format) if format else None
            
            # Parse comma-separated columns into list
            if columns:
                columns = [col.strip() for col in columns.split(",")]
            else:
                columns = None
            
            # Get statistics for the file
            stats_result = get_stats(
                path=file_path,
                fmt=fmt,
                columns=columns,
                sample_size=sample_size
            )
            
            # Output the results based on format preference
            if markdown:
                from omni_morph.utils.json2md import stats_to_markdown
                typer.echo(stats_to_markdown(stats_result))
            else:
                # Output the results as JSON
                typer.echo(json.dumps(stats_result, indent=2, default=str))
    except Exception as e:
        typer.echo(f"Error computing statistics: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def merge(files: list[Path] = typer.Argument(..., help="Files to merge"),
          output_path: Path = typer.Argument(..., help="Output file path"),
          allow_cast: bool = typer.Option(True, "--allow-cast/--no-cast", help="Allow automatic casting between compatible types"),
          progress: bool = typer.Option(False, "--progress", "-p", help="Show progress during merge")):
    """
    Merge multiple files of the same or different formats into one.
    """
    try:
        # Convert Path objects to strings
        source_files = [str(file) for file in files]
        
        # Determine the output format from the file extension
        output_fmt = Format.from_path(output_path)
        
        # Merge the files
        merge_files(
            sources=source_files,
            output_path=str(output_path),
            output_fmt=output_fmt,
            allow_cast=allow_cast,
            progress=progress
        )
        
        typer.echo(f"Files merged successfully to {output_path}")
    except Exception as e:
        typer.echo(f"Error merging files: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def to_json(file_path: Path = typer.Argument(..., help="Path to the input file"),
            output_path: Path = typer.Argument(..., help="Path to the output JSON file"),
            pretty: bool = typer.Option(False, "--pretty", help="Pretty print JSON")):
    """
    Convert one file to JSON format.
    """
    convert(file_path, output_path, dst_fmt=Format.JSON, 
            write_kwargs={"pretty": pretty} if pretty else None)

@app.command()
def to_csv(file_path: Path = typer.Argument(..., help="Path to the input file"),
           output_path: Path = typer.Argument(..., help="Path to the output CSV file"),
           has_header: bool = typer.Option(True, "--has-header", help="CSV has header"),
           delimiter: str = typer.Option(",", help="Delimiter"),
           quote: str = typer.Option('"', help="Quote char")):
    """
    Convert one file to CSV format.
    """
    # Only include parameters supported by PyArrow's WriteOptions
    write_kwargs = {
        "include_header": has_header,
        "delimiter": delimiter,
        "quoting_style": "needed" if quote == '"' else "all"
    }
    convert(file_path, output_path, dst_fmt=Format.CSV, write_kwargs=write_kwargs)

@app.command()
def to_avro(file_path: Path = typer.Argument(..., help="Path to the input file"),
            output_path: Path = typer.Argument(..., help="Path to the output Avro file"),
            compression: str = typer.Option("uncompressed", "--compression", help="Compression method")):
    """
    Convert one file to Avro format.
    """
    write_kwargs = {"compression": compression} if compression != "uncompressed" else None
    convert(file_path, output_path, dst_fmt=Format.AVRO, write_kwargs=write_kwargs)

@app.command()
def to_parquet(file_path: Path = typer.Argument(..., help="Path to the input file"),
               output_path: Path = typer.Argument(..., help="Path to the output Parquet file"),
               compression: str = typer.Option("uncompressed", "--compression", help="Compression method")):
    """
    Convert one file to Parquet format.
    """
    write_kwargs = {"compression": compression} if compression != "uncompressed" else None
    convert(file_path, output_path, dst_fmt=Format.PARQUET, write_kwargs=write_kwargs)

@app.command()
def random_sample(file_path: Path = typer.Argument(..., help="Path to the input file"),
                   output_path: Path = typer.Argument(..., help="Path to the output file"),
                   n: int = typer.Option(None, "--n", help="Number of records"),
                   fraction: float = typer.Option(None, "--fraction", help="Fraction of records"),
                   seed: int = typer.Option(None, "--seed", help="Random seed for reproducibility")):
    """
    Randomly sample records from a file.
    """
    try:
        # Extract the sample as a PyArrow Table
        table = extract_sample(str(file_path), n=n, fraction=fraction, seed=seed)
        
        # Determine the output format from the file extension
        output_fmt = Format.from_path(output_path)
        
        # Check for complex types when writing to CSV
        if output_fmt == Format.CSV:
            has_complex_types = any(
                pa.types.is_list(field.type) or 
                pa.types.is_struct(field.type) or 
                pa.types.is_map(field.type)
                for field in table.schema
            )
            
            if has_complex_types:
                typer.echo("Error: Data contains complex types (lists, maps, or nested structures) "
                           "that cannot be represented in CSV format.")
                typer.echo("Please use JSONL, Parquet, or Avro format for output files with complex data types.")
                raise typer.Exit(code=1)
        
        # Write the table directly to the output file
        write(table, output_path, fmt=output_fmt)
        
        typer.echo(f"Sampled data written to {output_path}")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def query(
    file_path: Path = typer.Argument(..., help="Path to the input data file"),
    sql: str = typer.Argument(..., help="SQL query to execute against the data file"),
    format: str = typer.Option(None, "--format", "-f", help="Force specific format (avro, parquet, csv, json)"),
):
    """
    Run SQL queries against a data file using DuckDB.
    
    The file is exposed as a view with a name matching its filename (without extension).
    For example, 'sales_2025.parquet' becomes a table named 'sales_2025'.
    """
    try:
        # Convert format string to Format enum if provided
        fmt = Format(format) if format else None
        
        # Validate SQL before execution
        err = validate_sql(sql, file_path, fmt=fmt)
        
        if err is None:
            # SQL is valid - execute and stream result
            run_query(sql, file_path, fmt=fmt, return_type="stdout")
        else:
            # SQL is invalid - fetch schema & ask AI for suggestions
            schema_txt = json.dumps(get_schema(str(file_path), fmt), indent=2)
            typer.echo(f"\n❌ SQL validation failed:\n{err}\n")
            
            try:
                suggestion = ai_suggest(sql, err, schema_txt)
                typer.echo("💡 Suggested fix:\n")
                typer.echo(suggestion)
            except Exception as ex:
                # AI fallback - show schema so user can fix manually
                typer.echo(f"⚠️ Could not get AI suggestion: {ex}")
                typer.echo(f"\nSchema:\n{schema_txt}")
    except Exception as e:
        typer.echo(f"Error executing query: {e}", err=True)
        raise typer.Exit(code=1)
