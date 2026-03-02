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

# Root of the git repo (src/thess_geo_analytics/core/settings.py → up 3 levels)
REPO_ROOT = Path(__file__).resolve().parents[3]

# Base DATA_LAKE path as a string (used by some entrypoints)
# e.g. /data_lake inside Docker, or <repo>/DATA_LAKE locally
DATA_LAKE = os.environ.get("DATA_LAKE", str(REPO_ROOT / "DATA_LAKE"))

# Path-typed versions used throughout the codebase
DATA_LAKE_DIR = Path(DATA_LAKE)
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

# Global default nodata for NDVI / rasters (used if YAML doesn't override)
GLOBAL_NODATA = float(os.environ.get("NDVI_NODATA", str(NDVI_NODATA)))

# Global default resampling (used if YAML doesn't override)
DEFAULT_RESAMPLING = os.environ.get("DEFAULT_RESAMPLING", "nearest")

# Global default raster resolution (meters), when YAML.raster.resolution is missing
DEFAULT_RASTER_RESOLUTION = float(os.environ.get("DEFAULT_RASTER_RESOLUTION", "10"))

# ---------------------------------------------------------------------
# Project-specific paths (AOI, tables)
# ---------------------------------------------------------------------

AOI_DIR = REPO_ROOT / "aoi"
TABLES_DIR = REPO_ROOT / "tables"

SCENE_CATALOG_TABLE = Path(
    os.environ.get("SCENE_CATALOG_TABLE", str(TABLES_DIR / "scenes_s2_all.csv"))
)
SCENES_SELECTED_TABLE = Path(
    os.environ.get("SCENES_SELECTED_TABLE", str(TABLES_DIR / "scenes_selected.csv"))
)
ASSETS_MANIFEST_TABLE = Path(
    os.environ.get("ASSETS_MANIFEST_TABLE", str(TABLES_DIR / "assets_manifest_selected.csv"))
)
NDVI_PERIOD_STATS_TABLE = Path(
    os.environ.get("NDVI_PERIOD_STATS_TABLE", str(TABLES_DIR / "ndvi_period_stats.csv"))
)

# ---------------------------------------------------------------------
# Cloud storage / upload locations
# ---------------------------------------------------------------------

UPLOAD_COMPOSITES_BUCKET = os.environ.get("UPLOAD_COMPOSITES_BUCKET", "thess-geo-analytics")
UPLOAD_COMPOSITES_PREFIX = os.environ.get("UPLOAD_COMPOSITES_PREFIX", "ndvi/composites")

UPLOAD_PIXEL_FEATURES_BUCKET = os.environ.get(
    "UPLOAD_PIXEL_FEATURES_BUCKET", "thess-geo-analytics"
)
UPLOAD_PIXEL_FEATURES_PREFIX = os.environ.get(
    "UPLOAD_PIXEL_FEATURES_PREFIX", "ndvi/pixel_features"
)

UPLOAD_RAW_S2_BUCKET = os.environ.get("UPLOAD_RAW_S2_BUCKET", "thess-geo-analytics")
UPLOAD_RAW_S2_PREFIX = os.environ.get("UPLOAD_RAW_S2_PREFIX", "raw_s2")

# ---------------------------------------------------------------------
# Scene Catalog internals
# ---------------------------------------------------------------------

SCENE_USE_TILE_SELECTOR = _as_bool(os.environ.get("SCENE_USE_TILE_SELECTOR"), True)
SCENE_ALLOW_UNION = _as_bool(os.environ.get("SCENE_ALLOW_UNION"), True)
SCENE_MAX_UNION_TILES = int(os.environ.get("SCENE_MAX_UNION_TILES", "20"))

# ---------------------------------------------------------------------
# Assets Manifest internals
# ---------------------------------------------------------------------

ASSETS_SORT_MODE = os.environ.get("ASSETS_SORT_MODE", "cloud_then_time")
ASSETS_DOWNLOAD_N = int(os.environ.get("ASSETS_DOWNLOAD_N", "999999"))
ASSETS_RAW_STORAGE_MODE = os.environ.get("ASSETS_RAW_STORAGE_MODE", "url_to_local")
ASSETS_BAND_RESOLUTION = int(os.environ.get("ASSETS_BAND_RESOLUTION", "10"))
ASSETS_DELETE_LOCAL_AFTER_UPLOAD = _as_bool(
    os.environ.get("ASSETS_DELETE_LOCAL_AFTER_UPLOAD"), False
)

# ---------------------------------------------------------------------
# Timestamps Aggregation internals
# ---------------------------------------------------------------------

# Bands list as comma-separated env, e.g. "B04,B08,SCL"
TIMESTAMPS_BANDS = os.environ.get("TIMESTAMPS_BANDS", "B04,B08,SCL").split(",")

# ---------------------------------------------------------------------
# NDVI internals
# ---------------------------------------------------------------------

NDVI_MAX_SCENES_PER_PERIOD = os.environ.get("NDVI_MAX_SCENES_PER_PERIOD")  # None = no cap
NDVI_DOWNLOAD_MISSING = _as_bool(os.environ.get("NDVI_DOWNLOAD_MISSING"), True)
NDVI_VERBOSE = _as_bool(os.environ.get("NDVI_VERBOSE"), False)

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
    print("REPO_ROOT:                    ", REPO_ROOT)
    print("DATA_LAKE:                    ", DATA_LAKE)
    print("DATA_LAKE_DIR:                ", DATA_LAKE_DIR)
    print("DATA_RAW_DIR:                 ", DATA_RAW_DIR)
    print("CACHE_S2_DIR:                 ", CACHE_S2_DIR)
    print("NUTS_LOCAL_PATH:              ", NUTS_LOCAL_PATH)
    print("AUTO_DOWNLOAD_GISCO:          ", AUTO_DOWNLOAD_GISCO)
    print("DEFAULT_COLLECTION:           ", DEFAULT_COLLECTION)
    print("GLOBAL_NODATA:                ", GLOBAL_NODATA)
    print("DEFAULT_RESAMPLING:           ", DEFAULT_RESAMPLING)
    print("DEFAULT_RASTER_RESOLUTION:    ", DEFAULT_RASTER_RESOLUTION)
    print("SCENE_CATALOG_TABLE:          ", SCENE_CATALOG_TABLE)
    print("SCENES_SELECTED_TABLE:        ", SCENES_SELECTED_TABLE)
    print("ASSETS_MANIFEST_TABLE:        ", ASSETS_MANIFEST_TABLE)
    print("NDVI_PERIOD_STATS_TABLE:      ", NDVI_PERIOD_STATS_TABLE)
    print("UPLOAD_COMPOSITES_BUCKET:     ", UPLOAD_COMPOSITES_BUCKET)
    print("UPLOAD_COMPOSITES_PREFIX:     ", UPLOAD_COMPOSITES_PREFIX)
    print("UPLOAD_RAW_S2_BUCKET:         ", UPLOAD_RAW_S2_BUCKET)
    print("UPLOAD_RAW_S2_PREFIX:         ", UPLOAD_RAW_S2_PREFIX)
    print("TIMESTAMPS_BANDS:             ", TIMESTAMPS_BANDS)
    print("NDVI_MAX_SCENES_PER_PERIOD:   ", NDVI_MAX_SCENES_PER_PERIOD)
    print("NDVI_DOWNLOAD_MISSING:        ", NDVI_DOWNLOAD_MISSING)
    print("NDVI_VERBOSE:                 ", NDVI_VERBOSE)
    print("VERBOSE:                      ", VERBOSE)
    print("NOTEBOOK_MODE:                ", NOTEBOOK_MODE)
    print("=================")


if __name__ == "__main__":
    debug()