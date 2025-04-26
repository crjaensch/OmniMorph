"""Utilities for converting JSON data to Markdown format.

This module provides functions to convert JSON data structures to Markdown
formatted strings for better human readability in CLI output.
"""

from typing import Dict, Any, List


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
    
    # Generate markdown for numeric columns
    if numeric_cols:
        markdown.append("# Numeric columns\n")
        
        # Create table header
        markdown.append("| column | non-null count | min | max | mean | median |")
        markdown.append("| ------ | ------------- | --- | --- | ---- | ------ |")
        
        # Add rows for each numeric column
        for col_name, stats in numeric_cols.items():
            row = [
                col_name,
                str(stats.get("count", 0)),
                _format_value(stats.get("min")),
                _format_value(stats.get("max")),
                _format_value(stats.get("mean")),
                _format_value(stats.get("median"))
            ]
            markdown.append(f"| {' | '.join(row)} |")
        
        markdown.append("")
    
    # Generate markdown for categorical columns
    if categorical_cols:
        markdown.append("# Categorical columns\n")
        
        # Create table header
        markdown.append("| column | distinct | top-5 categories (value · count) |")
        markdown.append("| ------ | -------- | ------------------------------- |")
        
        # Add rows for each categorical column
        for col_name, stats in categorical_cols.items():
            top5_str = _format_top5(stats.get("top5", []))
            row = [
                col_name,
                str(stats.get("distinct", 0)),
                top5_str
            ]
            markdown.append(f"| {' | '.join(row)} |")
    
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
    """
    Convert schema or JSON schema data to a Markdown formatted string.

    Renders a Markdown table showing each field's name, data type, nullability, and description.

    Args:
        schema_data (Dict[str, Any]): Schema mapping. Supports two forms:
            - {'fields': List[dict]}: list of field dicts with keys 'name', 'type', 'nullable', optional 'description'.
            - JSON Schema dict: with 'properties' mapping field names to schemas containing 'type' and optional 'description'.

    Returns:
        str: Markdown formatted representation of the schema.
    """
    markdown = []
    
    # Add title
    markdown.append("# 📦 Data Schema Overview\n")
    
    # Create table header
    markdown.append("| Field Name | Data Type | Nullable | Description |")
    markdown.append("| ---------- | --------- | -------- | ----------- |")
    
    # Normalize fields list
    fields: List[Dict[str, Any]] = []
    # JSON Schema 'properties' branch
    if isinstance(schema_data.get("properties"), dict):
        for fname, props in schema_data["properties"].items():
            ftype = props.get("type")
            # union types list or single
            if isinstance(ftype, list):
                nullable = "✅" if "null" in ftype else "❌"
                types = [t for t in ftype if t != "null"]
                dtype = ",".join(types)
            else:
                nullable = "❌"
                dtype = str(ftype)
            fields.append({
                "name": fname,
                "type": dtype,
                "nullable": nullable,
                "description": props.get("description", "")
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
            else:
                nullable = "❌"
                dtype = str(ftype)
            fields.append({
                "name": name,
                "type": dtype,
                "nullable": nullable,
                "description": f.get("doc", "")
            })
    # Simple field definitions list branch
    elif isinstance(schema_data.get("fields"), list):
        for f in schema_data["fields"]:
            fields.append({
                "name": f.get("name", ""),
                "type": str(f.get("type", "")),
                "nullable": "✅" if f.get("nullable") else "❌",
                "description": f.get("description", "")
            })
    # Add rows for each field definition
    for field in fields:
        name = field.get("name", "")
        data_type = field.get("type", "")
        # nullable may be stored as bool or already as checkmark
        nullable = field.get("nullable")
        nullable_mark = "✅" if nullable is True or nullable == "✅" else "❌"
        description = field.get("description", "")
        row = [name, data_type, nullable_mark, description]
        markdown.append(f"| {' | '.join(row)} |")
     
    return "\n".join(markdown)
