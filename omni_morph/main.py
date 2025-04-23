import logging
from pathlib import Path
import importlib.metadata
import json
from omni_morph.data.converter import read, convert, Format
from omni_morph.data.extractor import head as extract_head, tail as extract_tail
import typer

DEFAULT_RECORDS = 20

app = typer.Typer()

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
def head(file_path: Path = typer.Argument(..., help="Path to a file"),
         n: int = typer.Option(DEFAULT_RECORDS, "-n", "--number", help="Number of records")):
    """
    Display the first n records of a file.
    """
    try:
        table = extract_head(str(file_path), n)
        for row in table.to_pylist():
            typer.echo(row)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def tail(file_path: Path = typer.Argument(..., help="Path to a file"),
         n: int = typer.Option(DEFAULT_RECORDS, "-n", "--number", help="Number of records")):
    """
    Display the last n records of a file.
    """
    try:
        table = extract_tail(str(file_path), n)
        for row in table.to_pylist():
            typer.echo(row)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def meta(file_path: Path = typer.Argument(..., help="Path to a file")):
    typer.echo("meta command not implemented", err=True)
    raise typer.Exit(code=1)

@app.command()
def schema(file_path: Path = typer.Argument(..., help="Path to a file")):
    try:
        # Import here to avoid import errors for other commands
        from omni_morph.utils.file_utils import get_schema
        schema = get_schema(str(file_path))
        typer.echo(json.dumps(schema, indent=2))
    except Exception as e:
        typer.echo(f"Error extracting schema: {e}", err=True)
        raise typer.Exit(code=1)

@app.command()
def stats(file_path: Path = typer.Argument(..., help="Path to a file")):
    typer.echo("stats command not implemented", err=True)
    raise typer.Exit(code=1)

@app.command()
def validate(file_path: Path = typer.Argument(..., help="Path to a file"),
             schema_path: Path = typer.Option(None, "--schema-path", help="Path to schema file")):
    typer.echo("validate command not implemented", err=True)
    raise typer.Exit(code=1)

@app.command()
def merge(files: list[Path] = typer.Argument(..., help="Files to merge"),
          output_path: Path = typer.Argument(..., help="Output file path")):
    typer.echo("merge command not implemented", err=True)
    raise typer.Exit(code=1)

@app.command()
def count(file_path: Path = typer.Argument(..., help="Path to a file")):
    table = read(file_path)
    typer.echo(table.num_rows)

@app.command()
def to_json(file_path: Path = typer.Argument(...),
            output_path: Path = typer.Argument(...),
            pretty: bool = typer.Option(False, "--pretty", help="Pretty print JSON")):
    convert(file_path, output_path, dst_fmt=Format.JSON, 
            write_kwargs={"pretty": pretty} if pretty else None)

@app.command()
def to_csv(file_path: Path = typer.Argument(...),
           output_path: Path = typer.Argument(...),
           has_header: bool = typer.Option(True, "--has-header", help="CSV has header"),
           delimiter: str = typer.Option(",", help="Delimiter"),
           line_terminator: str = typer.Option("\n", help="Line terminator"),
           quote: str = typer.Option('"', help="Quote char")):
    write_kwargs = {
        "include_header": has_header,
        "delimiter": delimiter,
        "line_terminator": line_terminator,
        "quoting_style": "needed" if quote == '"' else "all"
    }
    convert(file_path, output_path, dst_fmt=Format.CSV, write_kwargs=write_kwargs)

@app.command()
def to_avro(file_path: Path = typer.Argument(...),
            output_path: Path = typer.Argument(...),
            compression: str = typer.Option("uncompressed", "--compression", help="Compression method")):
    write_kwargs = {"compression": compression} if compression != "uncompressed" else None
    convert(file_path, output_path, dst_fmt=Format.AVRO, write_kwargs=write_kwargs)

@app.command()
def to_parquet(file_path: Path = typer.Argument(...),
               output_path: Path = typer.Argument(...),
               compression: str = typer.Option("uncompressed", "--compression", help="Compression method")):
    write_kwargs = {"compression": compression} if compression != "uncompressed" else None
    convert(file_path, output_path, dst_fmt=Format.PARQUET, write_kwargs=write_kwargs)

@app.command()
def random_sample(file_path: Path = typer.Argument(...),
                   output_path: Path = typer.Argument(...),
                   n: int = typer.Option(None, "--n", help="Number of records"),
                   fraction: float = typer.Option(None, "--fraction", help="Fraction of records")):
    typer.echo("random-sample command not implemented", err=True)
    raise typer.Exit(code=1)
