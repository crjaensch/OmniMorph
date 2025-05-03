"""Utilities for converting JSON data to Markdown format.

This module provides functions to convert JSON data structures to Markdown
formatted strings for better human readability in CLI output.
"""

from typing import Dict, Any, List
import pandas as pd


def stats_to_markdown(stats_data: Dict[str, Dict[str, Any]]) -> str:
    """
    Convert statistics data to a Markdown formatted string.

    Generates Markdown tables for numeric and categorical columns based on stats.

    Args:
        stats_data (Dict[str, Dict[str, Any]]): Mapping of column names to their statistics.
            For numeric columns, each stats dict should include:
                - type: 'numeric'
                - count, min, max, mean, median
            For categorical columns, each stats dict should include:
                - type: 'categorical'
                - distinct, top5 (List[dict] with 'value' and 'count')

    Returns:
        str: Markdown formatted representation of the statistics.
    """
    # Separate numeric and categorical columns
    numeric_cols = {}
    categorical_cols = {}
    
    for col_name, col_stats in stats_data.items():
        if col_stats.get("type") == "numeric":
            numeric_cols[col_name] = col_stats
        elif col_stats.get("type") == "categorical":
            categorical_cols[col_name] = col_stats
    
    markdown = []
    
    # Generate markdown for numeric columns using pandas DataFrame
    if numeric_cols:
        markdown.append("# Numeric columns\n")
        
        # Create DataFrame for numeric columns
        numeric_data = []
        for col_name, stats in numeric_cols.items():
            numeric_data.append({
                "column": col_name,
                "non-null count": stats.get("count", 0),
                "min": _format_value(stats.get("min")),
                "max": _format_value(stats.get("max")),
                "mean": _format_value(stats.get("mean")),
                "median": _format_value(stats.get("median"))
            })
        
        # Convert to DataFrame and generate markdown
        if numeric_data:
            df = pd.DataFrame(numeric_data)
            markdown.append(df.to_markdown(index=False, tablefmt="github"))
            markdown.append("")
    
    # Generate markdown for categorical columns using pandas DataFrame
    if categorical_cols:
        markdown.append("# Categorical columns\n")
        
        # Create DataFrame for categorical columns
        categorical_data = []
        for col_name, stats in categorical_cols.items():
            categorical_data.append({
                "column": col_name,
                "distinct": stats.get("distinct", 0),
                "top-5 categories (value · count)": _format_top5(stats.get("top5", []))
            })
        
        # Convert to DataFrame and generate markdown
        if categorical_data:
            df = pd.DataFrame(categorical_data)
            markdown.append(df.to_markdown(index=False, tablefmt="github"))
    
    return "\n".join(markdown)


def _format_value(value: Any) -> str:
    """Format a value for display in markdown table.
    
    Args:
        value: The value to format.
    
    Returns:
        A string representation of the value.
    """
    if value is None:
        return "__NULL__"
    elif isinstance(value, float):
        if abs(value) < 0.01 or abs(value) > 1_000_000:
            return f"{value:.2e}"
        return f"{value:.2f}"
    return str(value)


def _format_top5(top5: List[Dict[str, Any]]) -> str:
    """Format top 5 categories for display in markdown table.
    
    Args:
        top5: List of dictionaries containing value and count for top categories.
    
    Returns:
        A string representation of the top 5 categories.
    """
    if not top5:
        return ""
    
    formatted = []
    for item in top5:
        value = item.get("value", "__NULL__")
        count = item.get("count", 0)
        formatted.append(f"{value} · {count}")
    
    return " ; ".join(formatted)


def schema_to_markdown(schema_data: Dict[str, Any]) -> str:
    """Convert schema or JSON schema data to a Markdown formatted string.

    Renders a Markdown table showing each field's name, data type, nullability, and description.

    Args:
        schema_data (Dict[str, Any]): Schema mapping. Supports two forms:
            - {'fields': List[dict]}: list of field dicts with keys 'name', 'type', 'nullable', optional 'description'.
            - JSON Schema dict: with 'properties' mapping field names to schemas containing 'type' and optional 'description'.

    Returns:
        str: Markdown formatted representation of the schema.
    """
    # Add title
    markdown = ["\n# 📦 Data Schema Overview\n"]
    
    # Normalize fields list
    fields = []
    
    # JSON Schema 'properties' branch
    if isinstance(schema_data.get("properties"), dict):
        for fname, props in schema_data["properties"].items():
            ftype = props.get("type")
            # union types list or single
            if isinstance(ftype, list):
                nullable = "✅" if "null" in ftype else "❌"
                types = [t for t in ftype if t != "null"]
                dtype = ",".join(types)
                # Simplify timestamp types
                dtype = dtype.replace("timestamp[us]", "timestamp")
            else:
                nullable = "❌"
                dtype = str(ftype)
                # Simplify timestamp types
                dtype = dtype.replace("timestamp[us]", "timestamp")
            fields.append({
                "Field Name": fname,
                "Data Type": dtype,
                "Nullable": nullable,
                "Description": props.get("description", "")
            })
    
    # Avro record schema branch
    elif schema_data.get("type") == "record" and isinstance(schema_data.get("fields"), list):
        for f in schema_data["fields"]:
            name = f.get("name", "")
            ftype = f.get("type")
            # union types possibly list
            if isinstance(ftype, list):
                nullable = "✅" if "null" in ftype else "❌"
                types = [t for t in ftype if t != "null"]
                dtype = ",".join(types)
                # Simplify timestamp types
                dtype = dtype.replace("timestamp[us]", "timestamp")
            else:
                nullable = "❌"
                dtype = str(ftype)
                # Simplify timestamp types
                dtype = dtype.replace("timestamp[us]", "timestamp")
            fields.append({
                "Field Name": name,
                "Data Type": dtype,
                "Nullable": nullable,
                "Description": f.get("doc", "")
            })
    
    # Simple field definitions list branch
    elif isinstance(schema_data.get("fields"), list):
        for f in schema_data["fields"]:
            nullable = "✅" if f.get("nullable") else "❌"
            dtype = str(f.get("type", ""))
            # Simplify timestamp types
            dtype = dtype.replace("timestamp[us]", "timestamp")
            fields.append({
                "Field Name": f.get("name", ""),
                "Data Type": dtype,
                "Nullable": nullable,
                "Description": f.get("description", "")
            })
    
    # Add rows for each field definition using pandas for alignment
    if fields:
        # Create a DataFrame for better alignment
        df = pd.DataFrame(fields)
        # Use github format with additional styling
        table_lines = df.to_markdown(index=False, tablefmt="github")
        markdown.append(table_lines)
    
    return "\n".join(markdown)
