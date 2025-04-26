"""Utilities for converting JSON data to Markdown format.

This module provides functions to convert JSON data structures to Markdown
formatted strings for better human readability in CLI output.
"""

from typing import Dict, Any, List, Union, Optional


def stats_to_markdown(stats_data: Dict[str, Dict[str, Any]]) -> str:
    """Convert statistics data to a Markdown formatted string.
    
    Args:
        stats_data: Dictionary containing statistics data with column names as keys
                   and their statistics as values.
    
    Returns:
        A string containing the Markdown formatted representation of the statistics.
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


def schema_to_markdown(schema_data: Dict[str, List[Dict[str, Any]]]) -> str:
    """Convert schema data to a Markdown formatted string.
    
    Args:
        schema_data: Dictionary containing schema data with 'fields' as the key
                    and a list of field definitions as the value.
    
    Returns:
        A string containing the Markdown formatted representation of the schema.
    """
    markdown = []
    
    # Add title
    markdown.append("# 📦 Data Schema Overview\n")
    
    # Create table header
    markdown.append("| Field Name | Data Type | Nullable | Description |")
    markdown.append("| ---------- | --------- | -------- | ----------- |")
    
    # Add rows for each field
    for field in schema_data.get("fields", []):
        name = field.get("name", "")
        data_type = field.get("type", "")
        nullable = "✅" if field.get("nullable", False) else "❌"
        
        # Description is not in the schema JSON, so we leave it empty
        description = ""
        
        row = [
            name,
            data_type,
            nullable,
            description
        ]
        
        markdown.append(f"| {' | '.join(row)} |")
    
    return "\n".join(markdown)
