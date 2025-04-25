from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union, Dict, List, Tuple
import math
import os

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from fastdigest import TDigest              # Required dependency

try:
    from fastavro import reader as avro_reader  # pip install fastavro
except ImportError:
    avro_reader = None

from omni_morph.data.exceptions import ExtractError
from omni_morph.data.formats import Format

__all__ = [
    "get_stats"
]
# ---------------------------------------------------------------------------
#  Compute data field statistics
# ---------------------------------------------------------------------------

@dataclass
class _NumAgg:
    count: int = 0
    mean: float = 0.0
    M2:   float = 0.0         # variance accumulator (not exposed yet)
    minv: Union[float, None] = None
    maxv: Union[float, None] = None
    td:   Union[TDigest, None] = None

    def update(self, arr):
        for v in arr:
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue
            # online mean / variance (Welford)
            self.count += 1
            delta = v - self.mean
            self.mean += delta / self.count
            self.M2   += delta * (v - self.mean)
            self.minv  = v if self.minv is None else min(self.minv, v)
            self.maxv  = v if self.maxv is None else max(self.maxv, v)
            if self.td is not None:
                self.td.update(v)

    def finish(self):
        median = None
        if self.td is not None and not self.td.is_empty:
            # Use direct median() method if available, fallback to quantile(0.5)
            try:
                median = self.td.median()
            except AttributeError:
                median = self.td.quantile(0.5)
        return {
            "count":   self.count,
            "min":     self.minv,
            "max":     self.maxv,
            "mean":    self.mean if self.count else None,
            "median":  median,
        }

@dataclass
class _CatAgg:
    counter: Counter = field(default_factory=Counter)
    hll:     object  = None
    max_card: int    = 100_000     # switch to HLL if too many uniques

    def update(self, arr):
        self.counter.update(("__NULL__" if v in (None, "") else v) for v in arr)
        if len(self.counter) > self.max_card and self.hll is None:
            try:
                from datasketch.hyperloglog import HyperLogLog
                self.hll = HyperLogLog()
                self.hll.update(b"seed")  # warm-up
                for k in self.counter:
                    self.hll.update(str(k).encode())
            except ImportError:
                pass  # stay with the Counter (memory ↑)

        if self.hll is not None:
            for v in arr:
                self.hll.update(str(v).encode())

    def finish(self):
        distinct = (self.hll.count()
                    if self.hll is not None
                    else len(self.counter))
        top5 = self.counter.most_common(5)
        return {"distinct": distinct,
                "top5": [{"value": v, "count": c} for v, c in top5]}

# ---------------------------------------------------------------------------
#  Compute data field statistics
# ---------------------------------------------------------------------------

def get_stats(
    path: Union[str, Path],
    *,
    fmt: Optional[Format] = None,
    columns: Optional[List[str]] = None,
    sample_size: int = 2048,                    # t-digest reservoir per column
    small_file_threshold: int = 100 * 1024 * 1024,
) -> Dict[str, Dict]:
    """Compute per-column statistics for a data file.
    
    This function analyzes a data file and computes statistics for each column.
    For numeric columns, it calculates min, max, mean, and approximate median (using TDigest).
    For categorical columns, it counts distinct values and identifies the top 5 categories.
    
    Large files are processed by streaming, while small files use Arrow compute kernels directly.
    
    Args:
        path: A string or Path object pointing to the data file to analyze.
        fmt: Optional format specification. If None, the format is inferred
             from the file extension.
        columns: Optional list of column names to analyze. If None, all columns
                are analyzed.
        sample_size: Number of samples to use for t-digest reservoir sampling per column.
                    Set to 0 to disable median approximation.
        small_file_threshold: File size threshold in bytes below which the file
                             is loaded entirely into memory.
    
    Returns:
        A dictionary mapping column names to their statistics. Each column's statistics
        include the type ('numeric' or 'categorical') and type-specific metrics.
    
    Raises:
        ExtractError: If the format is unsupported or cannot be processed.
        ImportError: If a required dependency is missing for a specific format.
    """
    # Convert path to string for internal functions
    path_str = str(path)

    resolved_fmt = fmt or Format.from_path(path_str)
    size = os.path.getsize(path_str)

    if resolved_fmt == Format.PARQUET:
        return _stats_parquet(path_str, columns, size, sample_size, small_file_threshold)
    elif resolved_fmt == Format.AVRO:
        return _stats_avro(path_str, columns, sample_size, small_file_threshold)
    elif resolved_fmt == Format.CSV:
        return _stats_csv(path_str, columns, sample_size, small_file_threshold)
    elif resolved_fmt == Format.JSON:
        return _stats_jsonl(path_str, columns, sample_size, small_file_threshold)
    else:
        raise ExtractError(f"Unsupported format {resolved_fmt!r}")

# ---------- Parquet --------------------------------------------------------

def _stats_parquet(
    path: str,
    cols: Optional[List[str]],
    size: int,
    sample_size: int,
    limit: int
) -> Dict[str, Dict]:
    pf = pq.ParquetFile(path)
    if cols is None:
        cols = [f.name for f in pf.schema_arrow]
    small = size < limit

    num_aggs, cat_aggs = _prep_aggs(pf.schema_arrow, cols, sample_size)

    if small:
        tbl = pf.read(columns=cols)
        _update_aggs_from_table(tbl, num_aggs, cat_aggs)
    else:
        for rg in range(pf.num_row_groups):
            tbl = pf.read_row_group(rg, columns=cols)
            _update_aggs_from_table(tbl, num_aggs, cat_aggs)

    return _finish_aggs(num_aggs, cat_aggs)

# ---------- Avro -----------------------------------------------------------

def _stats_avro(
    path: str,
    cols: Optional[List[str]],
    sample_size: int,
    limit: int
) -> Dict[str, Dict]:
    if avro_reader is None:
        raise ImportError("fastavro missing (`pip install fastavro`).")

    num_aggs, cat_aggs = {}, {}
    with open(path, "rb") as fo:
        for rec in avro_reader(fo):
            _update_aggs_from_dict(rec, num_aggs, cat_aggs, cols, sample_size)
    return _finish_aggs(num_aggs, cat_aggs)

# ---------- CSV & JSONL ----------------------------------------------------

def _stats_csv(
    path: str,
    cols: Optional[List[str]],
    sample_size: int,
    limit: int
) -> Dict[str, Dict]:
    try:
        # If no columns specified, read the header row first to get all column names
        if cols is None:
            # Read just the header to get column names
            df_header = pd.read_csv(path, nrows=0)
            cols = df_header.columns.tolist()
        
        chunks = pd.read_csv(path, chunksize=1_000_000)
        return _stats_from_chunks(chunks, cols, sample_size)
    except FileNotFoundError:
        raise ExtractError(f"CSV file not found: {path}")
    except PermissionError:
        raise ExtractError(f"Permission denied when reading CSV file: {path}")
    except pd.errors.EmptyDataError:
        raise ExtractError(f"CSV file is empty: {path}")
    except pd.errors.ParserError:
        raise ExtractError(f"Malformed CSV file: {path}")
    except Exception as e:
        raise ExtractError(f"Error processing CSV file {path}: {e}") from e

def _stats_jsonl(
    path: str,
    cols: Optional[List[str]],
    sample_size: int,
    limit: int
) -> Dict[str, Dict]:
    import json
    
    try:
        # If no columns specified, read the first record to get all keys
        if cols is None:
            with open(path, "r", encoding="utf8") as fh:
                for line in fh:
                    if line.strip():
                        try:
                            first_record = json.loads(line)
                            if isinstance(first_record, dict):
                                cols = list(first_record.keys())
                            break
                        except json.JSONDecodeError:
                            # We'll handle this in the main loop
                            pass
        
        def chunks():
            with open(path, "r", encoding="utf8") as fh:
                batch = []
                line_num = 0
                for line in fh:
                    line_num += 1
                    if line.strip():
                        try:
                            batch.append(json.loads(line))
                        except json.JSONDecodeError:
                            raise ExtractError(f"Invalid JSON at line {line_num} in {path}")
                    if len(batch) == 1_000_000:
                        yield batch
                        batch = []
                if batch:
                    yield batch
        return _stats_from_chunks(chunks(), cols, sample_size)
    except FileNotFoundError:
        raise ExtractError(f"JSONL file not found: {path}")
    except PermissionError:
        raise ExtractError(f"Permission denied when reading JSONL file: {path}")
    except UnicodeDecodeError:
        raise ExtractError(f"JSONL file contains invalid Unicode characters: {path}")
    except Exception as e:
        if isinstance(e, ExtractError):
            raise
        raise ExtractError(f"Error processing JSONL file {path}: {e}") from e

# ---------- shared helpers -------------------------------------------------

def _create_tdigest():
    """Create a TDigest object with proper error handling.
    
    Returns:
        A TDigest object if creation succeeds, otherwise None.
    """
    try:
        return TDigest()
    except Exception as e:
        import warnings
        warnings.warn(f"Failed to initialize TDigest: {e}. Median approximation will be disabled.")
        return None

def _stats_from_chunks(
    chunks: Union[pd.DataFrame, List[Dict]],
    cols: Optional[List[str]],
    sample_size: int
) -> Dict[str, Dict]:
    num_aggs, cat_aggs = {}, {}
    first_chunk = True
    
    for chunk in chunks:
        if isinstance(chunk, list):     # JSONL list-of-dicts
            for rec in chunk:
                _update_aggs_from_dict(rec, num_aggs, cat_aggs, cols, sample_size)
        else:                           # pandas DataFrame
            tbl = pa.Table.from_pandas(chunk, preserve_index=False)
            
            # Initialize aggregators on first chunk
            if first_chunk:
                # If cols is None, use all columns from the DataFrame
                if cols is None:
                    cols = list(chunk.columns)
                # Initialize aggregators
                schema = tbl.schema
                num_aggs_new, cat_aggs_new = _prep_aggs(schema, cols, sample_size)
                num_aggs.update(num_aggs_new)
                cat_aggs.update(cat_aggs_new)
                first_chunk = False
                
            _update_aggs_from_table(tbl, num_aggs, cat_aggs)
    return _finish_aggs(num_aggs, cat_aggs)

def _is_numeric_type(field_type):
    """Check if a PyArrow type is numeric in a version-compatible way."""
    try:
        # Try the newer API first
        if hasattr(pa.types, 'is_numeric'):
            return pa.types.is_numeric(field_type)
        # Fall back to manual type checking for older PyArrow versions
        numeric_types = (
            pa.int8(), pa.int16(), pa.int32(), pa.int64(),
            pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(),
            pa.float16(), pa.float32(), pa.float64()
        )
        return any(field_type == t for t in numeric_types)
    except Exception:
        # If all else fails, use string type name check
        type_name = str(field_type).lower()
        return any(name in type_name for name in ('int', 'float', 'double', 'decimal'))

def _prep_aggs(
    schema: pa.Schema,
    cols: List[str],
    sample_size: int
) -> Tuple[Dict[str, _NumAgg], Dict[str, _CatAgg]]:
    num_aggs, cat_aggs = {}, {}
    for name in cols:
        field = schema.field(name) if hasattr(schema, "field") else None
        is_num = (field and _is_numeric_type(field.type))
        if is_num:
            td = _create_tdigest() if sample_size > 0 else None
            num_aggs[name] = _NumAgg(td=td)
        else:
            cat_aggs[name] = _CatAgg()
    return num_aggs, cat_aggs

def _update_aggs_from_table(
    tbl: pa.Table,
    num_aggs: Dict[str, _NumAgg],
    cat_aggs: Dict[str, _CatAgg]
) -> None:
    for name, agg in num_aggs.items():
        arr = tbl[name].to_pylist()
        agg.update(arr)
    for name, agg in cat_aggs.items():
        arr = tbl[name].to_pylist()
        agg.update(arr)

def _update_aggs_from_dict(
    rec: Dict,
    num_aggs: Dict[str, _NumAgg],
    cat_aggs: Dict[str, _CatAgg],
    cols: Optional[List[str]],
    sample_size: int
) -> None:
    for k, v in rec.items():
        if cols and k not in cols:
            continue
        if isinstance(v, (int, float)) or v is None:
            agg = num_aggs.setdefault(k, _NumAgg(td=_create_tdigest() if sample_size > 0 else None))
            agg.update([v])
        else:
            agg = cat_aggs.setdefault(k, _CatAgg())
            agg.update([v])

def _finish_aggs(
    num_aggs: Dict[str, _NumAgg],
    cat_aggs: Dict[str, _CatAgg]
) -> Dict[str, Dict]:
    out = {}
    for k, agg in num_aggs.items():
        out[k] = {"type": "numeric", **agg.finish()}
    for k, agg in cat_aggs.items():
        out[k] = {"type": "categorical", **agg.finish()}
    return out