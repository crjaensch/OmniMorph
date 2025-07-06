"""
File system abstraction layer for OmniMorph.
Supports local and cloud storage (Azure ADLS Gen2).
"""

from __future__ import annotations

from typing import Union, BinaryIO, TextIO, Any, Dict

# For Azure ADLS Gen2 support
import adlfs
import fsspec


class FileSystemHandler:
    """Abstract file system operations for different storage backends."""

    # Class variables to store Azure credentials
    _azure_credentials = {
        "connection_string": None,
        "account_key": None,
        "account_name": None,
        "tenant_id": None,
        "client_id": None,
        "client_secret": None,
    }

    @classmethod
    def set_azure_credentials(cls, credentials: dict) -> None:
        """
        Set Azure credentials for use with ADLS Gen2 storage.

        Args:
            credentials: Dictionary containing Azure credentials
                - connection_string: Azure Storage connection string
                - account_key: Azure Storage account key
                - account_name: Azure Storage account name
                - tenant_id: Azure tenant ID for service principal
                - client_id: Azure client ID for service principal
                - client_secret: Azure client secret for service principal
        """
        if credentials:
            for key, value in credentials.items():
                if key.startswith("azure_") and value is not None:
                    cls._azure_credentials[key.replace("azure_", "")] = value

    @classmethod
    def get_fs_and_path(cls, path: str) -> tuple[Any, str]:
        """
        Parse a path and return the appropriate filesystem and path.

        Supports:
        - Local paths: /path/to/file.csv
        - Azure ADLS Gen2: abfss://container@account.dfs.core.windows.net/path/to/file.csv

        Args:
            path: A string path to parse

        Returns:
            A tuple of (filesystem, path)
        """
        if path.startswith(("abfss://", "abfs://")):
            # Parse Azure ADLS Gen2 path
            credential = None
            account_name = None

            # Extract account name from URL if possible
            if "@" in path:
                account_part = path.split("@")[1].split(".")[0]
                account_name = (
                    cls._azure_credentials.get("account_name") or account_part
                )

            # Try connection string first
            if cls._azure_credentials.get("connection_string"):
                credential = cls._azure_credentials["connection_string"]
            # Then try account key
            elif cls._azure_credentials.get("account_key") and account_name:
                credential = cls._azure_credentials["account_key"]
            # Then try service principal
            elif all(
                [
                    cls._azure_credentials.get(k)
                    for k in ["tenant_id", "client_id", "client_secret"]
                ]
            ):
                from azure.identity import ClientSecretCredential

                credential = ClientSecretCredential(
                    tenant_id=cls._azure_credentials["tenant_id"],
                    client_id=cls._azure_credentials["client_id"],
                    client_secret=cls._azure_credentials["client_secret"],
                )

            # Create the filesystem
            fs = adlfs.AzureBlobFileSystem(
                account_name=account_name, credential=credential
            )
            return fs, path
        else:
            # Local filesystem
            return fsspec.filesystem("file"), str(path)

    @classmethod
    def open_file(
        cls, path: str, mode: str = "rb", **kwargs
    ) -> Union[BinaryIO, TextIO]:
        """
        Open a file from any supported filesystem.

        Args:
            path: Path to the file to open
            mode: File mode ('rb', 'r', 'wb', 'w', etc.)
            **kwargs: Additional arguments to pass to the underlying filesystem's open method
                      (e.g., encoding for text files)

        Returns:
            An open file-like object
        """
        fs, path = cls.get_fs_and_path(path)
        return fs.open(path, mode, **kwargs)

    @classmethod
    def exists(cls, path: str) -> bool:
        """
        Check if a path exists.

        Args:
            path: Path to check

        Returns:
            True if the path exists, False otherwise
        """
        fs, path = cls.get_fs_and_path(path)
        return fs.exists(path)

    @classmethod
    def get_file_info(cls, path: str) -> Dict[str, Any]:
        """
        Get file information (size, modified time, etc.).

        Args:
            path: Path to get information for

        Returns:
            A dictionary of file information
        """
        fs, path = cls.get_fs_and_path(path)
        return fs.info(path)

    @classmethod
    def read_excel(
        cls,
        path: str,
        *,
        sheet_name: int | str = 0,
        **pd_kwargs,
    ):
        """Read an Excel workbook (.xlsx) from local or Azure storage.

        Parameters
        ----------
        path : str
            Local path (e.g. ``/data/file.xlsx``) or cloud URI
            (e.g. ``abfss://container@account.dfs.core.windows.net/file.xlsx``).
        sheet_name : int | str, default 0
            Worksheet selector forwarded to :func:`pandas.read_excel`.
            • ``int`` – zero-based index (0 = first sheet)
            • ``str`` – worksheet name
        **pd_kwargs
            Additional keyword arguments for :func:`pandas.read_excel`.

        Returns
        -------
        pandas.DataFrame
            Loaded worksheet contents.

        Notes
        -----
        For remote filesystems (anything other than ``file`` protocol) the
        workbook is first streamed into an in-memory ``BytesIO`` buffer because
        ``openpyxl`` requires a seek-able object.
        """
        import io
        import pandas as pd

        # Accept pathlib.Path objects as well as plain strings
        path_str = str(path)
        fs, norm_path = cls.get_fs_and_path(path_str)
        protocol = getattr(fs, "protocol", "file")

        if protocol == "file":
            # Local paths can be passed directly to pandas/openpyxl
            return pd.read_excel(
                norm_path, sheet_name=sheet_name, engine="openpyxl", **pd_kwargs
            )

        # Remote: download to memory then hand to pandas
        with fs.open(norm_path, "rb") as fo:
            data = fo.read()
        return pd.read_excel(
            io.BytesIO(data), sheet_name=sheet_name, engine="openpyxl", **pd_kwargs
        )
