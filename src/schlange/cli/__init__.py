"""
Command-line interface.

Thin wrapper around the public API. Each command opens a short-lived
client and delegates to the appropriate service.
"""

from .main import main
