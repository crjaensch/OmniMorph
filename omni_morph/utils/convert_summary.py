"""Utilities for converting DuckDB's Summarize output to OmniMorph's statistics format.

This module provides functions to convert the markdown table produced by DuckDB's
'SUMMARIZE' command into the same format used by OmniMorph's get_stats() function
after it has been converted to markdown using the json2md.py module.

The conversion preserves:
* min, max, mean, median (q50) for numeric columns
* column-level info only (no top-5 categories) for categorical columns

Usage as CLI:
    python convert_summary.py path/to/summary.md -o path/to/converted_stats.md

Usage as module:
    from omni_morph.utils.convert_summary import convert_summary
    markdown_str = convert_summary(path_to_summary_file)

Supports both local paths and cloud URLs (Azure ADLS Gen2)."""
from pathlib import Path
import argparse
import pandas as pd

from omni_morph.data.filesystems import FileSystemHandler

# --------------------------------------------------------------------------- #
# 1.  Helpers                                                                  #
# --------------------------------------------------------------------------- #
# SQL data types - noqa: spell-checker
NUMERIC_SQL_TYPES = {
    "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
    "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL"
}

def _parse_summary_md(path: str) -> pd.DataFrame:
    """Read the markdown table produced by the DuckDB SUMMARIZE command.
    
    Args:
        path: Path to the markdown file containing DuckDB's summary output (local path or cloud URL)
        
    Returns:
        DataFrame containing the parsed summary data
    """
    # Use FileSystemHandler to read file content (works with both local and Azure paths)
    with FileSystemHandler.open_file(path, 'r', encoding='utf-8') as f:
        content = f.read()
    lines = content.splitlines()

    # keep only rows that look like markdown‑table lines ("| .. |")
    rows = [ln for ln in lines if ln.strip().startswith("|")] 

    # split on "|" and trim whitespace
    parsed = []
    for ln in rows:
        parts = [p.strip() for p in ln.split("|")] 
        if parts and parts[0] == "":               # drop leading table border
            parts = parts[1:]
        if parts and parts[-1] == "":              # drop trailing table border
            parts = parts[:-1]
        parsed.append(parts)

    header, align, *data = parsed                 # second row is just "---|" 
    df = pd.DataFrame(data, columns=header)

    # best‑effort numeric conversion for obvious columns
    numeric_cols = {
        "min", "max", "avg", "std", "q25", "q50", "q75",
        "count", "null_percentage", "approx_unique"
    }
    for col in numeric_cols & set(df.columns):
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            # Keep as is if conversion fails
            pass

    return df


def _split_numeric_categorical(df: pd.DataFrame):
    """Split the summary data into numeric and categorical dataframes.
    
    Args:
        df: DataFrame containing the parsed summary data
        
    Returns:
        Tuple of (numeric_df, categorical_df) ready for final markdown output
    """
    df["is_numeric"] = (
        df["column_type"].str.upper().isin(NUMERIC_SQL_TYPES)
    )

    # -------- numeric -------------------------------------------------------
    df["non_null_count"] = (
        df["count"] * (1.0 - df["null_percentage"].fillna(0) / 100.0)
    ).round().astype("Int64")

    numeric = (
        df[df["is_numeric"]]
        [["column_name", "non_null_count", "min", "max", "avg", "q50"]]
        .rename(
            columns={
                "column_name": "column",
                "non_null_count": "non-null count",
                "avg": "mean",
                "q50": "median",
            }
        )
    )

    # -------- categorical ---------------------------------------------------
    categorical = (
        df[~df["is_numeric"]][["column_name", "approx_unique"]]
        .rename(columns={"column_name": "column", "approx_unique": "distinct"})
    )

    return numeric, categorical


def _to_markdown_table(df: pd.DataFrame) -> str:
    """Convert a DataFrame to GitHub-flavored markdown table.
    
    Args:
        df: DataFrame to convert to markdown
        
    Returns:
        Markdown table string
    """
    # wide=0 makes numeric columns right‑aligned automatically
    return df.to_markdown(index=False, tablefmt="github")


def convert_summary(path: str) -> str:
    """Convert DuckDB's SUMMARIZE output to OmniMorph statistics format.
    
    This function reads the markdown table produced by DuckDB's SUMMARIZE command
    and converts it to the same format used by OmniMorph's get_stats() function
    after it has been converted to markdown using the json2md.py module.
    
    Args:
        path: Path to the markdown file containing DuckDB's summary output
        
    Returns:
        Markdown string in the format used by OmniMorph's stats command
    """
    numeric, categorical = _split_numeric_categorical(_parse_summary_md(path))

    md_out = (
        "# Numeric columns\n\n"
        + _to_markdown_table(numeric)
        + "\n\n# Categorical columns\n\n"
        + _to_markdown_table(categorical)
        + "\n"
    )
    return md_out


# --------------------------------------------------------------------------- #
# 2.  Command‑line test interface                                             #
# --------------------------------------------------------------------------- #
def _cli():
    parser = argparse.ArgumentParser(
        description="Convert summary‑stats markdown to stats.md layout."
    )
    parser.add_argument("input", help="Path to *_summary.md* file")
    parser.add_argument(
        "-o", "--output", help="Destination file (default: print to stdout)"
    )
    args = parser.parse_args()

    md_result = convert_summary(args.input)

    if args.output:
        Path(args.output).write_text(md_result, encoding="utf‑8")
        print(f"✓ Saved converted report to: {args.output}")
    else:
        print(md_result)


if __name__ == "__main__":
    _cli()