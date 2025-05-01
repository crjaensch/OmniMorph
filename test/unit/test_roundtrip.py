from __future__ import annotations

from itertools import permutations
from pathlib import Path

import pyarrow as pa
import pytest

import omni_morph.data as omd

# ---------------------------- fixtures ------------------------------------ #


@pytest.fixture(scope="session")
def sample_table() -> pa.Table:
    """A deterministic table used for all tests."""
    return pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie"]),
            "score": pa.array([9.5, 7.0, 8.25], type=pa.float64()),
        }
    )


# ---------------------------- parameterised checks ------------------------ #

all_formats = list(omd.Format)
format_pairs = list(permutations(all_formats, 2))  # 12 combinations


@pytest.mark.parametrize("src_fmt,dst_fmt", format_pairs)
def test_conversion_roundtrip(tmp_path: Path, sample_table: pa.Table, src_fmt, dst_fmt):
    """
    For every pair of formats, write a file in *src_fmt*, convert it to *dst_fmt*,
    read it back and ensure the data are identical.
    """

    src_file = tmp_path / f"file.{src_fmt.name.lower()}"
    dst_file = tmp_path / f"converted.{dst_fmt.name.lower()}"

    # write original source
    omd.write(sample_table, src_file, fmt=src_fmt)

    # perform conversion
    omd.convert(src_file, dst_file, src_fmt=src_fmt, dst_fmt=dst_fmt)

    # read back and compare
    roundtrip = omd.read(dst_file, fmt=dst_fmt)
    assert sample_table.equals(roundtrip), f"{src_fmt}->{dst_fmt} mismatch"