"""Shared pytest setup for TVDownloader offline tests.

The scripts under test read config.yaml and .env at import time and abort when
required secrets are missing. Provide harmless placeholders here so modules can
be imported without touching any network service.
"""

import os
import sys
from pathlib import Path

_TV_DIR = Path(__file__).resolve().parent.parent
if str(_TV_DIR) not in sys.path:
    sys.path.insert(0, str(_TV_DIR))

os.environ.setdefault("TMDB_API_KEY", "test-key")
os.environ.setdefault("SUBDL_API_KEY", "test-key")
os.environ.setdefault("PROXY_USER", "test-user")
os.environ.setdefault("PROXY_PASSWORD", "test-pass")
os.environ.setdefault("R2_ACCESS_KEY", "test-id")
os.environ.setdefault("R2_SECRET_KEY", "test-secret")
os.environ.setdefault("R2_ENDPOINT_URL", "https://example.invalid")
os.environ.setdefault("R2_BUCKET", "test-bucket")
