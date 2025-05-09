"""
File system abstraction layer for OmniMorph.
Supports local and cloud storage (Azure ADLS Gen2).
"""
from __future__ import annotations

import io
from pathlib import Path
from typing import Union, BinaryIO, TextIO, Optional, Any, Dict

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
                if key.startswith('azure_') and value is not None:
                    cls._azure_credentials[key.replace('azure_', '')] = value
    
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
        if path.startswith(('abfss://', 'abfs://')):
            # Parse Azure ADLS Gen2 path
            credential = None
            account_name = None
            
            # Extract account name from URL if possible
            if '@' in path:
                account_part = path.split('@')[1].split('.')[0]
                account_name = cls._azure_credentials.get('account_name') or account_part
            
            # Try connection string first
            if cls._azure_credentials.get('connection_string'):
                credential = cls._azure_credentials['connection_string']
            # Then try account key
            elif cls._azure_credentials.get('account_key') and account_name:
                credential = cls._azure_credentials['account_key']
            # Then try service principal
            elif all([cls._azure_credentials.get(k) for k in ['tenant_id', 'client_id', 'client_secret']]):
                from azure.identity import ClientSecretCredential
                credential = ClientSecretCredential(
                    tenant_id=cls._azure_credentials['tenant_id'],
                    client_id=cls._azure_credentials['client_id'],
                    client_secret=cls._azure_credentials['client_secret']
                )
            
            # Create the filesystem
            fs = adlfs.AzureBlobFileSystem(
                account_name=account_name,
                credential=credential
            )
            return fs, path
        else:
            # Local filesystem
            return fsspec.filesystem('file'), str(path)
    
    @classmethod
    def open_file(cls, path: str, mode: str = 'rb', **kwargs) -> Union[BinaryIO, TextIO]:
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
