"""
_csv_schema.py

Utilities for inferring schemas from CSV files.
"""

import csv
from typing import Dict, Any, List, Union

def infer_csv_schema(filepath: str) -> Dict[str, Any]:
    """
    Infer schema from a CSV file.
    
    Args:
        filepath (str): Path to the CSV file
        
    Returns:
        Dict[str, Any]: A dictionary representing the inferred schema
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        # Read the first few rows to infer types
        csv_reader = csv.reader(f)
        headers = next(csv_reader)  # Get headers
        
        # Read a sample of rows for type inference
        sample_rows = []
        for _ in range(100):  # Sample up to 100 rows
            try:
                sample_rows.append(next(csv_reader))
            except StopIteration:
                break
    
    # Initialize schema
    schema = {
        "type": "object",
        "properties": {}
    }
    
    # Infer types for each column
    for i, header in enumerate(headers):
        column_values = [row[i] if i < len(row) else "" for row in sample_rows]
        column_type = _infer_column_type(column_values)
        
        schema["properties"][header] = {
            "type": column_type,
            "description": f"Column {header}"
        }
    
    return schema

def _infer_column_type(values: List[str]) -> Union[str, List[str]]:
    """
    Infer the data type of a column based on its values.
    
    Args:
        values (List[str]): List of string values from the column
        
    Returns:
        Union[str, List[str]]: Inferred data type(s)
    """
    # Remove empty values for type inference
    non_empty_values = [v for v in values if v.strip()]
    if not non_empty_values:
        return "string"  # Default to string for empty columns
    
    # Try to infer types
    is_integer = all(v.strip().lstrip('-').isdigit() for v in non_empty_values)
    if is_integer:
        return "integer"
    
    is_number = all(_is_number(v) for v in non_empty_values)
    if is_number:
        return "number"
    
    is_boolean = all(v.lower() in ("true", "false", "0", "1") for v in non_empty_values)
    if is_boolean:
        return "boolean"
    
    # Check for date/time formats (simplified)
    date_patterns = [
        all(v.count('-') == 2 and len(v) == 10 for v in non_empty_values),  # YYYY-MM-DD
        all('/' in v and len(v) <= 10 for v in non_empty_values)  # MM/DD/YYYY
    ]
    if any(date_patterns):
        return "string"  # Use string for dates (could be refined to date-time)
    
    # Default to string
    return "string"

def _is_number(value: str) -> bool:
    """
    Check if a string represents a number (integer or float).
    
    Args:
        value (str): String value to check
        
    Returns:
        bool: True if the string represents a number, False otherwise
    """
    try:
        float(value)
        return True
    except ValueError:
        return False
