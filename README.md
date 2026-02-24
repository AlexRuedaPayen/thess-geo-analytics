# Thess Geo Analytics
A Modular and Scalable Sentinel-2 Processing Pipeline

Developed by: Alex Rueda Payen (Independent Research Project)  
(Project inspired by applications at CERTH — aiming to demonstrate EO pipeline engineering skills.)

## Overview

Thess Geo Analytics is a fully modular, configuration-driven pipeline for transforming raw Sentinel-2 Level-2A data into analysis-ready products:

- AOI extraction from NUTS boundaries  
- Sentinel-2 scene catalog creation  
- Assets manifest generation  
- Raw band retrieval & organization  
- Per-timestamp mosaics (tile aggregation)  
- Optional NDVI monthly/quarterly composites  

The project is designed to be:

- **Simple to use** → one YAML config  
- **Technically deep** → scalable architecture, parallelism, efficient caching  
- **Transparent** → parameters printed at each step  
- **Reproducible** → deterministic execution  

The intent is to showcase a well-engineered EO processing framework, suitable for consideration by institutions such as CERTH, NOA, or other European EO groups.

---

## 1. Repository Structure

```
thess-geo-analytics/
│
├── config/
│   ├── pipeline.thess.yaml     # Main user-facing configuration
│
├── src/thess_geo_analytics/
│   ├── entrypoints/            # Makefile entrypoints
│   │   ├── ExtractAoi.py
│   │   ├── BuildSceneCatalog.py
│   │   ├── BuildAssetsManifest.py
│   │   └── BuildAggregatedTimestamps.py
│   │
│   ├── pipelines/              # High-level orchestrators
│   ├── builders/               # Heavy processing units (mosaics, downloads, etc.)
│   ├── core/
│   │   ├── pipeline_config.py  # YAML config → structured access
│   │   ├── mode_settings.py    # dev/deep scaling logic
│   │   └── settings.py         # advanced defaults, central paths
│   │
│   └── utils/
│       ├── RepoPaths.py
│       └── logging_params.py
│
├── DATA_LAKE/                  # Auto-created output lake
├── Makefile
└── README.md
```

---

## 2. Installation

### Prerequisites

- Python ≥ 3.11  
- GDAL / Rasterio dependencies  
- `make` installed  

Optional:
- CDSE (Copernicus Data Space) credentials  
- Google Cloud Storage credentials  

### Setup

```
git clone https://github.com/<your-repo>/thess-geo-analytics.git
cd thess-geo-analytics

python -m venv .venv
source .venv/bin/activate      # Linux/Mac
.venv\Scripts\activate       # Windows

pip install -r requirements.txt
```

Optional `.env`:

```
DATA_LAKE=/path/to/data_lake
CDSE_USERNAME=...
CDSE_PASSWORD=...
```

---

## 3. Configuration (`pipeline.thess.yaml`)

This is the only file users normally edit.

### Example:

```yaml
mode: "dev"
debug: false

region: "Thessaloniki"
aoi_id: "el522"

pipeline:
  date_start: "2021-01-01"

raster:
  resolution: 10

scene_catalog:
  cloud_cover_max: 20.0
  max_items: 3000
  full_cover_threshold: 0.95
  n_anchors: 64
  window_days: 42
  collection: "sentinel-2-l2a"

assets_manifest:
  max_scenes: null
  upload_to_gcs: false

timestamps_aggregation:
  merge_method: "first"
  resampling: "nearest"
  nodata: 0.0
  bands: ["B04", "B08", "SCL"]
```

### User should focus on:

| Section | Purpose |
|--------|---------|
| mode | `dev` = quick tests, `deep` = full run |
| pipeline.date_start | temporal start for all steps |
| scene_catalog.* | how strict / dense sampling is |
| raster.resolution | output resolution |
| upload_to_gcs | whether to upload intermediate results |
| timestamps_aggregation.* | mosaic behavior |

Advanced storage rules, paths, and internal defaults live in `settings.py`.

---

## 4. Running the Pipeline

### Individual steps

```
make extract-aoi
make scene-catalog
make assets-manifest
make timestamps-aggregation
```

### Full pipeline

```
make full
```

Each step prints a structured log:

```
[ENTRYPOINT] BuildSceneCatalog
[PARAMETERS]
  mode = dev
  region = Thessaloniki
  aoi_id = el522
  date_start = 2021-01-01    (Earliest acquisition date)
  max_items = 3000           (Max STAC items)
  full_cover_threshold = 0.95 (Tile coverage threshold)
------------------------------------------------------------
```

---

## 5. Pipeline Steps

### 1. AOI Extraction
- Downloads NUTS boundaries if missing  
- Extracts AOI by region name  
- Produces a GeoJSON AOI file  

### 2. Scene Catalog
- Queries Sentinel-2 collection  
- Filters scenes by cloud cover and AOI coverage  
- Uses anchor dates + temporal windows  
- Produces:  
  - `scenes_s2_all.csv`  
  - `scenes_selected.csv`  

### 3. Assets Manifest
- Determines required assets for selected scenes  
- Optionally downloads raw S2 bands  
- Validates TIFFs  
- Creates:  
  - `assets_manifest_selected.csv`  

### 4. Timestamp Aggregation
- Groups tiles by timestamp  
- Performs mosaicking with user-defined merge + resampling  
- Saves mosaics under:  

```
DATA_LAKE/data_raw/aggregated/<timestamp>/
```

---

## 6. Recommended Parameter Guidelines

### Development mode (fast)

```yaml
mode: dev
scene_catalog:
  max_items: 300–800
  n_anchors: 8–16
  window_days: 8–20
```

### Production mode

```yaml
mode: deep
scene_catalog:
  max_items: 3000+
  n_anchors: 64
  window_days: 42
```

### Cloud cover
- 5–20% = clean scenes  
- 20–40% = more frequent dates  

### Resolution
- **10 m** = best for NDVI + cloud mask  
- **20 m** = faster, lighter outputs  

---

## 7. Expected Outputs

A typical run produces:

### Tables
- Scene catalog (full & filtered)  
- Assets manifest  
- NDVI composite statistics (optional)

### Rasters
- Aggregated mosaics per timestamp  
- NDVI composite rasters (optional)

### Data Lake Structure

```
DATA_LAKE/
  data_raw/
    s2/
    aggregated/
  cache/
```

---

## 8. Purpose of This Project

This project is built with the intention to demonstrate:

- Earth Observation processing engineering  
- Scalable architecture design  
- Clean reproducible pipelines  
- Experience with Sentinel-2, STAC, mosaicking, caching, tiling  
- Ability to build realistic production-like EO pipelines  
