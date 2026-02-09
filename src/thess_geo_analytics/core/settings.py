from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

from thess_geo_analytics.core.constants import (
    NDVI_NODATA,
    DEFAULT_NUTS_FILENAME,
)

# Load .env if available (keep only here)
load_dotenv()

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}

# ---------------------------------------------------------------------
# Repo + data roots
# ---------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]

# DATA_LAKE base (env override)
DATA_LAKE_DIR = Path(os.environ.get("DATA_LAKE", str(REPO_ROOT / "DATA_LAKE")))

# Standardized subfolders inside the data lake
DATA_RAW_DIR = DATA_LAKE_DIR / "data_raw"
CACHE_DIR = DATA_LAKE_DIR / "cache"
CACHE_S2_DIR = CACHE_DIR / "s2"

# ---------------------------------------------------------------------
# GISCO / NUTS data (auto-download)
# ---------------------------------------------------------------------

NUTS_LOCAL_PATH = DATA_RAW_DIR / DEFAULT_NUTS_FILENAME

AUTO_DOWNLOAD_GISCO = _as_bool(os.environ.get("AUTO_DOWNLOAD_GISCO"), default=True)

# ---------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------

DEFAULT_COLLECTION = os.environ.get("DEFAULT_COLLECTION", "sentinel-2-l2a")

# ---------------------------------------------------------------------
# Raster defaults
# ---------------------------------------------------------------------

GLOBAL_NODATA = float(os.environ.get("NDVI_NODATA", str(NDVI_NODATA)))
DEFAULT_RESAMPLING = os.environ.get("DEFAULT_RESAMPLING", "nearest")

# ---------------------------------------------------------------------
# Logging / verbosity
# ---------------------------------------------------------------------

VERBOSE = _as_bool(os.environ.get("VERBOSE"), default=False)

# ---------------------------------------------------------------------
# CDSE Auth
# ---------------------------------------------------------------------

CDSE_USERNAME = os.environ.get("CDSE_USERNAME")
CDSE_PASSWORD = os.environ.get("CDSE_PASSWORD")
CDSE_TOTP = os.environ.get("CDSE_TOTP")

# ---------------------------------------------------------------------
# Notebook behavior
# ---------------------------------------------------------------------

NOTEBOOK_MODE = _as_bool(os.environ.get("NOTEBOOK_MODE"), default=False)

# ---------------------------------------------------------------------
# Debug
# ---------------------------------------------------------------------

def debug() -> None:
    print("=== SETTINGS ===")
    print("REPO_ROOT:           ", REPO_ROOT)
    print("DATA_LAKE_DIR:       ", DATA_LAKE_DIR)
    print("DATA_RAW_DIR:        ", DATA_RAW_DIR)
    print("CACHE_S2_DIR:        ", CACHE_S2_DIR)
    print("NUTS_LOCAL_PATH:     ", NUTS_LOCAL_PATH)
    print("AUTO_DOWNLOAD_GISCO: ", AUTO_DOWNLOAD_GISCO)
    print("DEFAULT_COLLECTION:  ", DEFAULT_COLLECTION)
    print("GLOBAL_NODATA:       ", GLOBAL_NODATA)
    print("DEFAULT_RESAMPLING:  ", DEFAULT_RESAMPLING)
    print("VERBOSE:             ", VERBOSE)
    print("NOTEBOOK_MODE:       ", NOTEBOOK_MODE)
    print("=================")

if __name__ == "__main__":
    debug()
