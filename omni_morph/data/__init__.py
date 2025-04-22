"""
omni_morph.data
==============

Data handling modules for OmniMorph.
"""

from .formats import Format
from .converter import read, write, convert

__all__ = ["Format", "read", "write", "convert"]
