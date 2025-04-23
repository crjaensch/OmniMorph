from __future__ import annotations
import os
import random
from typing import Iterable, Optional

import pyarrow as pa
import pyarrow.parquet as pq

try:
    from fastavro import reader as avro_reader
except ImportError:  # fastavro is optional until Avro is actually used
    avro_reader = None

from .exceptions import ExtractError

# ---------------------------------------------------------------------------
#  PARQUET SAMPLING (efficient row-group aware)
# ---------------------------------------------------------------------------

def parquet_sample(
    path: str,
    n: Optional[int],
    fraction: Optional[float],
    rng: random.Random,
    replace: bool,
    limit: int,
) -> pa.Table:
    pfile = pq.ParquetFile(path)
    total_rows = pfile.metadata.num_rows
    file_size  = os.path.getsize(path)

    # ---------- choose indices ------------------------------------------------
    if fraction is not None:
        # keep each row with probability p
        chosen_idx = [i for i in range(total_rows) if rng.random() < fraction]
    else:
        if replace:
            chosen_idx = [rng.randrange(total_rows) for _ in range(n)]
        else:
            if n > total_rows:
                raise ExtractError("n exceeds number of rows in file.")
            chosen_idx = rng.sample(range(total_rows), n)

    if not chosen_idx:
        return pa.table({})      # empty sample

    chosen_idx.sort()

    # ---------- small file? just read once -----------------------------------
    if file_size < limit:
        tbl = pfile.read()
        return tbl.take(pa.array(chosen_idx, type=pa.int64()))

    # ---------- large file: read minimal row-groups ---------------------------
    rg_meta = pfile.metadata.row_group
    rg_boundaries = []
    offset = 0
    for rg in range(pfile.num_row_groups):
        rows_here = rg_meta(rg).num_rows
        rg_boundaries.append((offset, offset + rows_here, rg))
        offset += rows_here

    # Determine which row-groups contain each selected index
    needed_rgs   = set()
    by_rg_offset = {}
    for idx in chosen_idx:
        for start, stop, rg in rg_boundaries:
            if start <= idx < stop:
                needed_rgs.add(rg)
                by_rg_offset.setdefault(rg, []).append(idx - start)
                break

    # Read only those row-groups
    partial = []
    for rg in sorted(needed_rgs):
        rg_tbl = pfile.read_row_group(rg)
        offsets = by_rg_offset[rg]
        partial.append(rg_tbl.take(pa.array(offsets, type=pa.int64())))
    return pa.concat_tables(partial)


# ---------------------------------------------------------------------------
#  STREAMING (reservoir) SAMPLING  for Avro / JSONL / CSV
# ---------------------------------------------------------------------------

def streaming_sample(
    iterator: Iterable[dict],
    n: Optional[int],
    fraction: Optional[float],
    rng: random.Random,
    replace: bool,
    limit: int,
) -> pa.Table:
    # Cheap size test: if the underlying file is small, load into memory first
    if hasattr(iterator, "__len__") and (getattr(iterator, "_size_bytes", 0) < limit):
        data = list(iterator)
        return _sample_in_memory(data, n, fraction, rng, replace)

    # ---- streaming path -----------------------------------------------------
    if replace and n is not None:
        raise ExtractError("`with_replacement=True` not supported for large files.")

    if n is not None:                 # reservoir, one pass, O(n) memory
        reservoir: list[dict] = []
        for i, rec in enumerate(iterator):
            if i < n:
                reservoir.append(rec)
            else:
                j = rng.randint(0, i)
                if j < n:
                    reservoir[j] = rec
        data = reservoir
    else:                             # fraction sampling
        data = [rec for rec in iterator if rng.random() < fraction]

    return pa.Table.from_pylist(data) if data else pa.table({})


def sample_in_memory(
    data: list[dict],
    n: Optional[int],
    fraction: Optional[float],
    rng: random.Random,
    replace: bool,
) -> pa.Table:
    if fraction is not None:
        sample = [row for row in data if rng.random() < fraction]
    else:
        if replace:
            sample = [rng.choice(data) for _ in range(n)]
        else:
            if n > len(data):
                raise ExtractError("n exceeds number of available records.")
            sample = rng.sample(data, n)

    return pa.Table.from_pylist(sample) if sample else pa.table({})


# ---------------------------------------------------------------------------
#  PER-FORMAT ITERATORS  (streaming)
# ---------------------------------------------------------------------------

def iter_avro(path: str):
    if avro_reader is None:
        raise ImportError("fastavro is required (`pip install fastavro`).")
    with open(path, "rb") as fo:
        for rec in avro_reader(fo):
            yield rec


def iter_jsonl(path: str):
    import json, mmap
    with open(path, "r", encoding="utf8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def iter_csv(path: str):
    import csv
    with open(path, "r", newline="", encoding="utf8") as fh:
        rdr = csv.DictReader(fh)
        for row in rdr:
            yield row