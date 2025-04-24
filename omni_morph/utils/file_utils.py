"""
file_utils.py

Utilities for extracting schemas from data files.
Supports Parquet, Avro, JSON, and CSV formats.
"""

import json
from json import JSONDecodeError
from pathlib import Path
from omni_morph.data.formats import Format
from omni_morph.utils._csv_schema import infer_csv_schema

import pyarrow.parquet as pq
import fastavro
from genson import SchemaBuilder

def _infer_json_schema(filepath: str):
    """
    Infer JSON schema: handles both JSON and JSONL (first record) using GenSON.
    """
    builder = SchemaBuilder()
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except JSONDecodeError:
            f.seek(0)
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    break
            else:
                raise ValueError(f"No JSON records found in {filepath}")
    builder.add_object(data)
    return builder.to_schema()

def get_schema(filepath: str, fmt: Format = None):
    """
    Extract the schema from a data file.

    Args:
        filepath (str): Path to the data file.
        fmt (Format, optional): Override format inference. Supported: 'parquet', 'avro', 'json', 'csv'.

    Returns:
        The extracted schema. Type depends on format:
        - Parquet: pyarrow.Schema
        - Avro: JSON schema string
        - JSON: dict representing JSON Schema
        - CSV: dict representing inferred schema

    Raises:
        ValueError: If format is unsupported or cannot be inferred.
    """
    # Resolve format, accept Format enum or string
    resolved_fmt = fmt or Format.from_path(filepath)
    if isinstance(resolved_fmt, str):
        resolved_fmt = Format(resolved_fmt)

    if resolved_fmt == Format.PARQUET:
        return pq.read_schema(filepath)
    if resolved_fmt == Format.AVRO:
        with open(filepath, 'rb') as fo:
            reader = fastavro.reader(fo)
            schema_dict = reader.schema
        return json.dumps(schema_dict)
    if resolved_fmt == Format.JSON:
        return _infer_json_schema(filepath)
    if resolved_fmt == Format.CSV:
        try:
            return infer_csv_schema(filepath)
        except Exception as e:
            raise ValueError(f"CSV schema inference failed: {str(e)}")

    raise ValueError(
        f"Unsupported format {resolved_fmt!r}. Supported formats: parquet, avro, json, csv."
    )
