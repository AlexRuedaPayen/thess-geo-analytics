from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

from thess_geo_analytics.core.constants import NDVI_NODATA

# Load .env if available
load_dotenv()

# ---------------------------------------------------------------------
# DATA LAYER & CACHE CONFIG
# ---------------------------------------------------------------------

# The base of your repo (used as fallback)
REPO_ROOT = Path(__file__).resolve().parents[3]

# DATA_LAKE_DIR:
# Priority:
#   1) Environment variable DATA_LAKE
#   2) "DATA_LAKE" folder in repo root
DATA_LAKE_DIR = Path(os.environ.get("DATA_LAKE", REPO_ROOT / "DATA_LAKE"))

# Cache folders inside DATA_LAKE
CACHE_DIR = DATA_LAKE_DIR / "cache"
CACHE_S2_DIR = CACHE_DIR / "s2"

# ---------------------------------------------------------------------
# COLLECTIONS
# ---------------------------------------------------------------------

DEFAULT_COLLECTION = os.environ.get("DEFAULT_COLLECTION", "sentinel-2-l2a")

# ---------------------------------------------------------------------
# NDVI / RASTER CONFIG
# ---------------------------------------------------------------------

GLOBAL_NODATA = float(os.environ.get("NDVI_NODATA", NDVI_NODATA))
DEFAULT_RESAMPLING = os.environ.get("DEFAULT_RESAMPLING", "nearest")  

# ---------------------------------------------------------------------
# LOGGING & VERBOSITY
# ---------------------------------------------------------------------

VERBOSE = os.environ.get("VERBOSE", "0") in {"1", "true", "TRUE", "yes"}

# ---------------------------------------------------------------------
# CDSE AUTH
# ---------------------------------------------------------------------

CDSE_USERNAME = os.environ.get("CDSE_USERNAME", None)
CDSE_PASSWORD = os.environ.get("CDSE_PASSWORD", None)
CDSE_TOTP = os.environ.get("CDSE_TOTP", None)

# ---------------------------------------------------------------------
# Notebook behavior
# ---------------------------------------------------------------------

# Make notebooks reproducible without weird path issues
NOTEBOOK_MODE = os.environ.get("NOTEBOOK_MODE", "0") in {"1", "true", "yes"}

# ---------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------

def debug():
    """Print current settings for debugging."""
    print("=== SETTINGS ===")
    print("REPO_ROOT:       ", REPO_ROOT)
    print("DATA_LAKE_DIR:   ", DATA_LAKE_DIR)
    print("CACHE_S2_DIR:    ", CACHE_S2_DIR)
    print("DEFAULT_COLLECTION:", DEFAULT_COLLECTION)
    print("GLOBAL_NODATA:   ", GLOBAL_NODATA)
    print("VERBOSE:         ", VERBOSE)
    print("=================")


if __name__=='__main__':
    debug()