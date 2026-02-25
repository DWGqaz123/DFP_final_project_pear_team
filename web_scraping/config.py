"""Centralized project configuration for API keys.

Set keys here (or via environment variables) and import from this module.
"""

from __future__ import annotations

import os

# Bureau of Labor Statistics API key.
BLS_API_KEY = os.getenv("BLS_API_KEY", "")

# FBI Crime Data Explorer API key (api.usa.gov).
FBI_API_KEY = os.getenv("FBI_API_KEY", "")
