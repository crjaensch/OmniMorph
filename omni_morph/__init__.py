"""
omni_morph
==========

Transform, inspect, and merge data files with a single command-line Swiss Army knife for data engineers.
"""

from .data.converter import Format, read, write, convert

__all__ = ["Format", "read", "write", "convert"]
