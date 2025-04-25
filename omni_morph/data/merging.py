from typing import Optional, List
from pathlib import Path
import json
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pandas as pd

from omni_morph.data.formats import Format
from .exceptions import ExtractError

try:
    from fastavro import reader as avro_reader
except ImportError:  # fastavro is optional until Avro is actually used
    avro_reader = None

# ---------------------------------------------------------------------------
#  MERGE FILES  --------------------------------------------------------------
# ---------------------------------------------------------------------------

def merge_files(
    sources: list[str],
    output_path: str,
    *,
    output_fmt: Optional[Format] = None,
    allow_cast: bool = True,
    chunksize: int = 100_000,
    progress: bool = False,
):
    """
    Merge multiple data files of possibly different format, like parquet, avro, JSON or CSV, 
    that share the same logical schema into a single output file.

    This function combines multiple data files into a single output file, handling
    schema reconciliation when needed. Large files are processed in chunks to maintain
    bounded memory usage.

    Args:
        sources: List of paths to source data files to merge.
        output_path: Path where the merged output file will be written.
        output_fmt: Optional format specification. If None, the format is inferred
                   from the output_path extension.
        allow_cast: If True, transparent casts are attempted between compatible types
                   (e.g. int64 → float64, timestamp[ms] → timestamp[ns]).
                   Incompatible columns raise ExtractError.
        chunksize: Maximum rows loaded per source batch to keep memory usage bounded.
        progress: If True, print progress information to stdout during the merge.

    Raises:
        ValueError: If no input files are supplied.
        ExtractError: If the output format is unsupported or if column types are incompatible.
        ImportError: If a required dependency is missing for a specific format.
    """
    if not sources:
        raise ValueError("No input files supplied.")

    resolved_fmt = output_fmt or Format.from_path(output_path)
    out_fmt = "jsonl" if resolved_fmt is Format.JSON else resolved_fmt.name.lower()
    if out_fmt not in {"parquet", "avro", "csv", "jsonl"}:
        raise ExtractError(f"Unsupported output format {out_fmt!r}")

    # ---------- establish target schema from the first source ---------------
    first_batches = _batch_reader(sources[0], chunksize)
    first = next(first_batches)
    schema = first.schema

    # open writer ------------------------------------------------------------
    _writer = _open_writer(output_path, out_fmt, schema)
    rows_written = 0

    def write_batch(tbl):
        nonlocal rows_written
        if allow_cast and tbl.schema != schema:
            tbl = _reconcile_schema(tbl, schema)
        _writer(tbl)
        rows_written += len(tbl)
        if progress and rows_written % (chunksize * 10) == 0:
            print(f"{rows_written:,} rows merged...", flush=True)

    write_batch(first)  # first batch from first file
    for tbl in first_batches:
        write_batch(tbl)

    # remaining files
    for src in sources[1:]:
        for tbl in _batch_reader(src, chunksize):
            write_batch(tbl)

    # close writer -----------------------------------------------------------
    _writer.close()
    if progress:
        print(f"Merge complete → {output_path}  ({rows_written:,} rows)")

# ---------------------------------------------------------------------------
#  Helpers for merge()  ------------------------------------------------------
# ---------------------------------------------------------------------------

def _reconcile_schema(tbl: pa.Table, target: pa.Schema) -> pa.Table:
    """
    Reconcile a table's schema with a target schema by adding missing columns and casting types.
    
    Args:
        tbl: PyArrow Table to reconcile
        target: Target PyArrow Schema to conform to
        
    Returns:
        PyArrow Table with schema matching the target schema
        
    Raises:
        ExtractError: If column types cannot be cast to target types
    """
    # add missing cols
    for name in target.names:
        if name not in tbl.schema.names:
            tbl = tbl.append_column(name, pa.nulls(len(tbl), type=target.field(name).type))
    # reorder & cast
    cols = []
    for field in target:
        arr = tbl[field.name]
        if arr.type != field.type:
            try:
                arr = pc.cast(arr, target_type=field.type, safe=False)
            except pa.ArrowInvalid as exc:
                raise ExtractError(f"Cannot cast column {field.name}: {exc}") from exc
        cols.append(arr)
    return pa.table(cols, names=target.names)

def _open_writer(path: str, fmt: str, schema: pa.Schema):
    """
    Create a format-specific writer function for the given output path and schema.
    
    Creates a callable that writes PyArrow Tables to the specified format.
    The returned callable has a .close() method to properly release resources.
    
    Args:
        path: Output file path
        fmt: Output format ('parquet', 'avro', 'csv', or 'jsonl')
        schema: PyArrow Schema for the output file
        
    Returns:
        A callable that accepts a PyArrow Table and writes it to the output file
        
    Raises:
        ImportError: If format-specific dependencies are missing
    """
    if fmt == "parquet":
        writer = pq.ParquetWriter(path, schema)
        write_func = lambda tbl: writer.write_table(tbl)
        write_func.close = lambda: writer.close()
        return write_func
    if fmt == "avro":
        if avro_reader is None:
            raise ImportError("fastavro needed for Avro.")
        from fastavro import writer as avro_writer, parse_schema
        avro_schema = json.loads(schema.to_json())
        fo = open(path, "wb")
        def _write(tbl):
            records = tbl.to_pylist()
            avro_writer(fo, parse_schema(avro_schema), records)
        _write.close = lambda : fo.close()
        return _write
    if fmt == "csv":
        header_written = False
        fo = open(path, "w", newline="", encoding="utf8")
        def _write(tbl):
            nonlocal header_written
            df = tbl.to_pandas()
            df.to_csv(fo, index=False, header=not header_written, mode="a")
            header_written = True
        _write.close = lambda : fo.close()
        return _write
    if fmt == "jsonl":
        fo = open(path, "w", encoding="utf8")
        def _write(tbl):
            for rec in tbl.to_pylist():
                fo.write(json.dumps(rec, default=str) + "\n")
        _write.close = lambda : fo.close()
        return _write

def _batch_reader(path: str, chunksize: int):
    """
    Read data from a file in batches, yielding PyArrow Tables.
    
    Handles different file formats (parquet, avro, csv, jsonl) with format-appropriate
    chunking strategies to maintain bounded memory usage.
    
    Args:
        path: Path to the input file
        chunksize: Maximum number of rows to load per batch
        
    Yields:
        PyArrow Tables containing batches of data from the input file
        
    Raises:
        ImportError: If format-specific dependencies are missing
        ExtractError: If the input format is not supported
    """
    resolved_fmt = Format.from_path(path)
    fmt = "jsonl" if resolved_fmt is Format.JSON else resolved_fmt.name.lower()
    if fmt == "parquet":
        pf = pq.ParquetFile(path)
        for rg in range(pf.num_row_groups):
            yield pf.read_row_group(rg)
    elif fmt == "avro":
        if avro_reader is None:
            raise ImportError("fastavro required.")
        with open(path, "rb") as fo:
            batch = []
            for rec in avro_reader(fo):
                batch.append(rec)
                if len(batch) == chunksize:
                    yield pa.Table.from_pylist(batch)
                    batch = []
            if batch:
                yield pa.Table.from_pylist(batch)
    elif fmt == "csv":
        for df in pd.read_csv(path, chunksize=chunksize):
            yield pa.Table.from_pandas(df, preserve_index=False)
    elif fmt == "jsonl":
        import json
        batch = []
        with open(path, "r", encoding="utf8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                batch.append(json.loads(line))
                if len(batch) == chunksize:
                    yield pa.Table.from_pylist(batch)
                    batch = []
            if batch:
                yield pa.Table.from_pylist(batch)
    else:
        raise ExtractError(f"Unsupported input format {fmt!r}")