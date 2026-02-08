from __future__ import annotations

# ---------------------------------------------------------------------
# Copernicus Data Space Ecosystem (CDSE)
# ---------------------------------------------------------------------

# Identity / OAuth token endpoint
CDSE_TOKEN_URL = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
)

# STAC Catalog base
CDSE_STAC_BASE_URL = "https://stac.dataspace.copernicus.eu/v1"

# Optional: Process API base (if you later use the /process endpoint)
CDSE_PROCESS_BASE_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"

# ---------------------------------------------------------------------
# Sentinel-2 Cloud & Masking
# ---------------------------------------------------------------------

# Sentinel-2 Scene Classification Layer (SCL) classes considered invalid
# According to ESA documentation:
#   3 = Cloud Shadow
#   8 = Cloud Medium Probability
#   9 = Cloud High Probability
#   10 = Thin Cirrus
SCL_INVALID_CLASSES = {3, 8, 9, 10}

# Optional: SCL classes considered "valid vegetation"
SCL_VALID_CLASSES = {
    4, 5, 6, 7  # vegetation, bare soil, water, etc.
}

# ---------------------------------------------------------------------
# HTTP & Request Settings
# ---------------------------------------------------------------------

HTTP_TIMEOUT = 60         # seconds
HTTP_RETRIES = 3          # basic retry count
HTTP_BACKOFF = 2          # seconds between retries

# ---------------------------------------------------------------------
# File system and reprojection defaults
# ---------------------------------------------------------------------

# Default nodata for NDVI rasters
NDVI_NODATA = -9999.0

# Raster I/O defaults
DEFAULT_DRIVER = "GTiff"
DEFAULT_COMPRESS = "deflate"
DEFAULT_OVERVIEWS = [2, 4, 8, 16]  # for QGIS-friendly pyramids

# ---------------------------------------------------------------------
# AOI defaults
# ---------------------------------------------------------------------

# Default AOI CRS target (Sentinel-2 UTM zones)
# Your AOI extraction transforms to the raster CRS anyway,
# so you can use this for consistency.
DEFAULT_AOI_CRS = "EPSG:4326"
