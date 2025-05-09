# ---------------------------------------------------------------------------
# query_engine.py  – Lightweight DuckDB wrapper
# ---------------------------------------------------------------------------

import duckdb, pathlib, pyarrow.dataset as ds
from typing import Optional, Literal, Union, Dict, Any
from pathlib import Path
from omni_morph.data.formats import Format

class QueryError(RuntimeError):
    """Raised when SQL cannot be parsed / bound or during execution."""

# ---------------------------------------------------------------------------
# CORE API
# ---------------------------------------------------------------------------

def query(
    sql: str,
    source: Union[str, Path],
    *,
    fmt: Optional[Format] = None,
    return_type: Literal["arrow", "pandas", "stdout"] = "stdout",
    azure_credentials: Optional[Dict[str, Any]] = None,
) -> Optional[object]:
    """Run SQL queries against a data file (source) in various formats.
    
    This function executes an SQL query against a data file (CSV/Parquet/JSONL/Avro).
    The file is exposed as a view whose name equals its file stem.
    For example, 'sales_2025.parquet' becomes a table named 'sales_2025'.
    
    Args:
        sql: The SQL query to execute against the data source.
        source: A string or Path object pointing to the data file to query.
        fmt: Optional format specification. If None, the format is inferred
             from the file extension.
        return_type: Determines the return type of the query results:
                    - "arrow": Returns a pyarrow.Table
                    - "pandas": Returns a pandas.DataFrame
                    - "stdout": Prints a Markdown table to stdout and returns None
    
    Returns:
        Depending on return_type:
        - pyarrow.Table if return_type is "arrow"
        - pandas.DataFrame if return_type is "pandas"
        - None if return_type is "stdout" (results are printed to stdout)
    
    Raises:
        QueryError: If the SQL query cannot be parsed, bound, or executed.
    """
    con = duckdb.connect(database=":memory:", config={"allow_unsigned_extensions": "true"})

    # Convert path to string for internal functions
    path_str = str(source)
    resolved_fmt = fmt or Format.from_path(path_str)

    # Install and load Avro extension if needed
    if resolved_fmt == Format.AVRO:
        _ensure_avro_extension(con)
        
    # Load Azure extension if needed for Azure paths
    if path_str.startswith(('abfss://', 'abfs://')):
        azure_loaded = _ensure_azure_extension(con)
        if azure_loaded:
            _configure_azure_credentials(con, azure_credentials)

    _register_source(con, resolved_fmt, path_str)

    try:
        result = con.sql(sql)
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc

    if return_type == "arrow":
        return result.arrow()
    if return_type == "pandas":
        return result.df()

    # markdown pretty-print to stdout
    import pandas as pd, sys
    pd.set_option("display.max_columns", None)
    df = result.df()
    sys.stdout.write("\n" + df.to_markdown(index=False) + "\n")
    return None

# ---------------------------------------------------------------------------
# VALIDATION (fast “lint”)
# ---------------------------------------------------------------------------

def validate_sql(
    sql: str,     
    source: Union[str, Path],
    *,
    fmt: Optional[Format] = None,
    azure_credentials: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Validate SQL syntax against a data source without executing the query.
    
    This function parses and binds the SQL query against the specified data source
    without actually executing it. It's useful for checking SQL syntax and
    validating column references before running a potentially expensive query.
    
    Args:
        sql: The SQL query to validate.
        source: A string or Path object pointing to the data file to validate against.
        fmt: Optional format specification. If None, the format is inferred
             from the file extension.
    
    Returns:
        None if the SQL is valid, or a human-readable error string if validation fails.
    
    Raises:
        No exceptions are raised as errors are returned as strings.
    """
    con = duckdb.connect(database=":memory:", config={"allow_unsigned_extensions": "true"})

    # Convert path to string for internal functions
    path_str = str(source)
    resolved_fmt = fmt or Format.from_path(path_str)

    # Install and load Avro extension if needed
    if resolved_fmt == Format.AVRO:
        _ensure_avro_extension(con)
        
    # Load Azure extension if needed for Azure paths
    if path_str.startswith(('abfss://', 'abfs://')):
        azure_loaded = _ensure_azure_extension(con)
        if azure_loaded:
            _configure_azure_credentials(con, azure_credentials)

    _register_source(con, resolved_fmt, path_str, lazy=True)    # lazy views
    try:
        con.execute("EXPLAIN " + sql)                     # parse & bind only
        return None
    except duckdb.Error as exc:
        return str(exc)

# ---------------------------------------------------------------------------
# AI-powered SQL query fix suggestion
# ---------------------------------------------------------------------------

def ai_suggest(sql: str, error_msg: str, schema_txt: str, source: Union[str, Path] = None) -> str:
    """Generate an improved SQL query suggestion using AI.
    
    This function uses OpenAI to analyze SQL errors and suggest corrections.
    It takes the original SQL query, the error message from DuckDB, and the
    schema information to provide context-aware suggestions.
    
    Args:
        sql: The original SQL query that caused an error.
        error_msg: The error message returned by DuckDB.
        schema_txt: Text representation of the schema for the data source.
        source: Optional source file path to derive the table name.
    
    Returns:
        A string containing either a corrected SQL query or a short explanation
        of how to fix the issue.
    
    Raises:
        ImportError: If the OpenAI package is not installed.
        RuntimeError: If the OPENAI_API_KEY environment variable is not set.
    """
    import os
    from openai import OpenAI
    
    # Initialize the OpenAI client
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Add table name information if source is provided
    table_name_hint = ""
    if source:
        p = pathlib.Path(source)
        table_name = p.stem
        table_name_hint = f"\n\nThe data is stored in a table named `{table_name}`."\
                          f" You can refer to this table directly in your SQL query."

    prompt = f"""I tried to run the SQL below but DuckDB returned an error.
    SQL:
    ```sql
    {sql}
    ```
    Error: {error_msg}

    The schema of the source file(s) is:
    {schema_txt}{table_name_hint}

    Suggest a corrected query, or explain the fix in one sentence.
    """

    # Use the new API format for OpenAI 1.0.0+
    completion = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "user", "content": prompt}
        ],
        temperature=0.2,
    )
    
    return completion.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# INTERNAL HELPERS
# ---------------------------------------------------------------------------

def _ensure_avro_extension(con):
    """Ensure the Avro extension is installed and loaded in DuckDB.
    
    Args:
        con: DuckDB connection object.
        
    Returns:
        bool: True if the extension was successfully installed and loaded, False otherwise.
    """
    try:
        # Check if the extension is already installed and loaded
        result = con.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'avro' AND loaded").fetchall()
        if result:
            return True  # Extension is already installed and loaded
            
        # Install the extension if not already installed
        con.execute("INSTALL avro FROM community;")
        # Load the extension
        con.execute("LOAD avro;")
        
        # Verify the extension is properly installed and loaded
        result = con.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'avro' AND loaded").fetchall()
        return len(result) > 0
    except duckdb.Error:
        return False


def _configure_azure_credentials(con, azure_credentials):
    """Configure Azure credentials for DuckDB connection.
    
    Args:
        con: DuckDB connection object.
        azure_credentials: Dictionary containing Azure credentials.
        
    Returns:
        None
    """
    if not azure_credentials:
        return
        
    # Configure Azure credentials
    if azure_credentials.get('azure_connection_string'):
        con.execute(f"SET azure_storage_connection_string='{azure_credentials['azure_connection_string']}'")
    elif azure_credentials.get('azure_account_name') and azure_credentials.get('azure_account_key'):
        con.execute(f"SET azure_account_name='{azure_credentials['azure_account_name']}'")
        con.execute(f"SET azure_account_key='{azure_credentials['azure_account_key']}'")
    elif all(azure_credentials.get(k) for k in ['azure_tenant_id', 'azure_client_id', 'azure_client_secret']):
        con.execute(f"SET azure_tenant_id='{azure_credentials['azure_tenant_id']}'")
        con.execute(f"SET azure_client_id='{azure_credentials['azure_client_id']}'")
        con.execute(f"SET azure_client_secret='{azure_credentials['azure_client_secret']}'")

def _ensure_azure_extension(con):
    """Ensure the Azure extension is installed and loaded in DuckDB.
    
    Args:
        con: DuckDB connection object.
        
    Returns:
        bool: True if the extension was successfully installed and loaded, False otherwise.
    """
    try:
        # Check if the extension is already installed and loaded
        con.execute("SELECT loaded FROM duckdb_extensions() WHERE extension_name='azure'")
        result = con.fetchone()
        
        if result and result[0]:
            return True  # Already loaded
            
        # Install and load the extension
        con.execute("INSTALL azure")
        con.execute("LOAD azure")
        return True
    except duckdb.Error as e:
        print(f"Warning: Could not load Azure extension: {e}")
        return False

def _register_source(con, fmt, source, *, lazy=False):
    """Create DuckDB views for the specified data file.
    
    This internal helper function registers a data file as a DuckDB view.
    The view name is derived from the file's stem (filename without extension).
    
    Args:
        con: DuckDB connection object.
        fmt: Format of the source file (Format enum).
        source: Path to the source file as a string.
        lazy: If True, only reads headers/metadata for schema inference without
               loading the full data. Useful for validation purposes.
    
    Raises:
        QueryError: If the source file format is unsupported.
    """
    # For Azure paths, we need to use the full path as the source
    # but still extract just the filename for the view name
    if source.startswith(('abfss://', 'abfs://')):
        # Extract the filename from the Azure path
        name = source.split('/')[-1].split('.')[0]
    else:
        p = pathlib.Path(source)
        name = p.stem

    if fmt == Format.PARQUET:
        # read_parquet doesn't scan the whole file until execution time
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{source}')")

    elif fmt == Format.CSV:
        # sample=1 forces only header inference when lazy=True
        sample_clause = ", sample_size=1" if lazy else ""
        con.execute(
            f"CREATE VIEW {name} AS "
            f"SELECT * FROM read_csv_auto('{source}'{sample_clause})"
        )

    elif fmt == Format.JSON:
        con.execute(
            f"CREATE VIEW {name} AS "
            f"SELECT * FROM read_json_auto('{source}', maximum_object_size=131072)"
        )

    elif fmt == Format.AVRO:
        # Try to use DuckDB's native Avro support via the community extension first
        avro_extension_loaded = _ensure_avro_extension(con)
        
        if avro_extension_loaded:
            # Use DuckDB's native Avro support
            try:
                con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_avro('{source}')")
                return
            except duckdb.Error as exc:
                # If read_avro fails, fall back to fastavro
                pass
        
        # Fall back to fastavro
        try:
            import fastavro
            import pandas as pd
            import pyarrow as pa
            
            # Read the Avro file using fastavro
            records = []
            
            # Handle both local and Azure paths
            if source.startswith(('abfss://', 'abfs://')):
                from omni_morph.data.filesystems import FileSystemHandler
                with FileSystemHandler.open_file(source, 'rb') as f:
                    reader = fastavro.reader(f)
                    if not lazy:
                        records = list(reader)
                    schema = reader.schema
            else:
                with open(str(source), 'rb') as f:
                    reader = fastavro.reader(f)
                    if not lazy:
                        records = list(reader)
                    schema = reader.schema
            
            # If lazy loading, just create an empty DataFrame with the schema
            if lazy and not records:
                # Create an empty DataFrame with the correct schema
                fields = schema.get('fields', [])
                empty_data = {field['name']: [] for field in fields}
                df = pd.DataFrame(empty_data)
            else:
                df = pd.DataFrame(records)
            
            # Convert to PyArrow table and register with DuckDB
            table = pa.Table.from_pandas(df)
            con.register(name, table)
            return
        except ImportError as imp_err:
            raise QueryError(f"The fastavro package is required for Avro support: {str(imp_err)}") from imp_err
        except Exception as exc:
            raise QueryError(f"Failed to read Avro file using fastavro: {str(exc)}") from exc
    else:
        raise QueryError(f"Unsupported source file: {p}")