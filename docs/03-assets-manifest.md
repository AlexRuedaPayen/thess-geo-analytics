# 03 — Assets Manifest & Raw Downloads (Sentinel‑2)

## Overview

The **Assets Manifest** step transforms the selected scenes from Step 02 (Scene Catalog) into concrete references to raw band assets (B04, B08, SCL), resolves their URLs, and optionally downloads and validates the GeoTIFFs. This step forms the **raster backbone** of the entire project — all later stages such as NDVI, timestamp mosaics, or pixel feature extraction depend on these raw assets being accurate and available.

The manifest acts as the **bridge** between the “metadata world” (scene IDs, timestamps) and “raster world” (GeoTIFFs on disk or GCS).

---

## Objectives

- Convert each scene in `scenes_selected.csv` into:
  - STAC item metadata (cached locally)
  - HREFs for B04, B08, and SCL raw bands
  - Local filesystem paths for downloaded tiles
  - Optional cloud storage (GCS) locations  
- Efficiently **download**, **validate**, and **register** raw band files
- Produce a consistent, indexed table:  
  `assets_manifest_selected.csv`

This step is designed for both **deep mode** (full dataset) and **dev mode** (smaller resolution & fewer downloads).

---

## Inputs

### 1. `scenes_selected.csv` (from Step 02)

Contains one row per selected tile, with:
- `id`
- `datetime`
- `cloud_cover`
- `anchor_date`
- `acq_datetime`
- …and tile coverage metrics

### 2. Configuration (`config/pipeline.thess.yaml`)

Relevant block:

```yaml
assets_manifest:
  scenes_selected_table: "scenes_selected.csv"
  out_table: "assets_manifest_selected.csv"

  max_scenes: null
  sort_mode: "cloud_then_time"

  download_n: 999999
  upload_to_gcs: false

  gcs_bucket: "thess-geo-analytics"
  gcs_prefix: "raw_s2"
  delete_local_after_upload: false
  raw_storage_mode: "url_to_local"

  band_resolution: 10
```

### 3. STAC Provider  
Copernicus Data Space Ecosystem (CDSE).

---

## Core Components

### 1. `AssetsManifestBuilder`

Responsible for:
- Validating `scenes_selected.csv`
- Retrieving STAC items (with local caching)
- Resolving band HREFs (via `StacAssetResolver`)
- Applying filters:
  - `max_scenes`
  - `sort_mode`
  - `date_start`
- Producing local file paths for each band

Produces columns such as:
- `href_b04`, `href_b08`, `href_scl`
- `local_b04`, `local_b08`, `local_scl`
- Optional: `gcs_b04`, `gcs_b08`, `gcs_scl`

---

### 2. `BuildAssetsManifestPipeline`

Pipeline orchestrator:
- Loads configuration
- Builds manifest DataFrame
- Prints stats (missing hrefs, counts)
- Performs downloads if enabled:
  - Uses `RawAssetStorageManager`
  - Validates rasters via `rasterio`
- Writes outputs:
  - `assets_manifest_selected.csv`
  - `assets_download_status.csv`

---

## Download Logic

Each scene triggers three main steps per band:

1. **URL/HREF resolution**
2. **Local or cloud fetch**
3. **Optional validation**

Download behavior depends on `raw_storage_mode`:

| Mode | Meaning |
|------|---------|
| `url_to_local` | Download all bands locally |
| `url_to_gcs_keep_local` | Upload to GCS and keep local copy |
| `url_to_gcs_drop_local` | Upload to GCS then delete local |
| `gcs_to_local` | Pull from GCS (if previously uploaded) |

Parallelism controlled by:
- `--max-download-workers`
- `THESS_MAX_DOWNLOAD_WORKERS`

---

## Outputs

### 1. `assets_manifest_selected.csv`

Contains finalized metadata for each scene:
- STAC-resolved HREFs  
- Local paths to downloaded bands  
- Optional GCS paths  
- Per-band availability flags  

### 2. `assets_download_status.csv`

Per-scene diagnostic information:
- `status` (`success`, `download_incomplete`, `validation_failed`, etc.)
- Band availability (b04_ok, b08_ok, scl_ok)
- Human-readable messages

---

## Complexity, Time & Resource Usage

### Time Complexity

Let *N = number of scenes*.

- Metadata resolution: **O(N)**  
- STAC fetches: **O(N)** but cached → first run slow, next runs fast  
- Downloads: **O(N)** dominated by network & disk IO  
- Parallelism: Speed scales with `max_download_workers`

### Memory Usage

Very small:
- Manifest DF uses a few MB
- STAC JSONs loaded one at a time
- Biggest resource usage is disk space for `.tif` files

### Disk-Space Expectations

Rule of thumb (approx):
- 10 m resolution Sentinel-2 band ≈ 20–70 MB  
- Each scene = 3 bands → ~60–210 MB  
- `download_n = 300` → **18–60 GB**

### Debuggability

- Investigate missing HREFs in STAC JSON cache  
- Inspect `assets_download_status.csv` for:
  - `missing_href`
  - `validation_failed`
  - `download_incomplete`
- Try manual raster open:
  ```python
  import rasterio
  rasterio.open("path/to/file.tif")
  ```

---

## How to Run

### 1. Makefile (recommended)

```bash
make assets-manifest
```

### 2. Direct CLI

Example:

```bash
python -m thess_geo_analytics.entrypoints.BuildAssetsManifest   --max-scenes 500   --date-start 2021-01-01   --sort-mode cloud_then_time   --download-n 100   --band-resolution 20   --max-download-workers 8   --raw-storage-mode url_to_local
```

---

## Next Steps

→ Proceed to **Step 04 — Timestamp Aggregation**, where:
- `time_serie.csv`
- and `assets_manifest_selected.csv`

are combined to create per-timestamp mosaics using the **TileAggregator**.

