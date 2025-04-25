import logging
from pathlib import Path
import importlib.metadata
import json
from omni_morph.data.converter import read, convert, Format, write
from omni_morph.data.extractor import head as extract_head, tail as extract_tail, sample as extract_sample
from omni_morph.data.statistics import get_stats
import typer

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
def schema(file_path: Path = typer.Argument(..., help="Path to the input file")):
    """
    Print the schema for a file.
    """
    try:
        # Import here to avoid import errors for other commands
        from omni_morph.utils.file_utils import get_schema
        schema = get_schema(str(file_path))
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
):
    """
    Print statistics about a file's columns.
    
    For numeric columns: min, max, mean, and approximate median.
    For categorical columns: distinct count and top 5 values.
    """
    try:
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
        
        # Output the results as JSON
        typer.echo(json.dumps(stats_result, indent=2, default=str))
    except Exception as e:
        typer.echo(f"Error computing statistics: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def validate(file_path: Path = typer.Argument(..., help="Path to the input file"),
             schema_path: Path = typer.Option(None, "--schema-path", help="Path to schema file")):
    """
    Validate a file against a schema.
    """
    typer.echo("validate command not implemented", err=True)
    raise typer.Exit(code=1)

@app.command()
def merge(files: list[Path] = typer.Argument(..., help="Files to merge"),
          output_path: Path = typer.Argument(..., help="Output file path")):
    """
    Merge multiple files of the same or different formats into one.
    """
    typer.echo("merge command not implemented", err=True)
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
           line_terminator: str = typer.Option("\n", help="Line terminator"),
           quote: str = typer.Option('"', help="Quote char")):
    """
    Convert one file to CSV format.
    """
    write_kwargs = {
        "include_header": has_header,
        "delimiter": delimiter,
        "line_terminator": line_terminator,
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
        
        # Write the table directly to the output file
        write(table, output_path, fmt=output_fmt)
        
        typer.echo(f"Sampled data written to {output_path}")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)
