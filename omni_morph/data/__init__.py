"""
omni_morph.data
==============

Data handling modules for OmniMorph.
"""

from .formats import Format
from .converter import read, write, convert
from .extractor import head, tail, sample

__all__ = ["Format", "read", "write", "convert", "head", "tail", "sample"]
