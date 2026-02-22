# 02 — Scene Catalog (Sentinel‑2 STAC)

## Overview

The **Scene Catalog** step retrieves all Sentinel‑2 L2A scenes intersecting the Area of Interest (AOI), applies 
geometric and cloud‑cover filtering, and constructs a clean, time‑ordered list of usable acquisitions.

This catalog forms the **time-series backbone** of the entire pipeline.  
Every downstream step — NDVI computation, composites, climatology, anomalies, pixel features — depends on the quality 
and consistency of this scene catalog.

---

## Objectives

- Query a **STAC API** (Copernicus Data Space Ecosystem).
- Retrieve all Sentinel‑2 scenes intersecting the AOI within a time window.
- Filter scenes by:
  - cloud cover
  - geometric coverage (AOI × tile intersection)
- Organize scenes into temporally distributed **anchors** for consistent monthly/quarterly sampling.
- Optionally merge tiles when the AOI spans multiple granules.
- Save two tables:
  - `scenes_s2_all.csv` → raw results
  - `scenes_selected.csv` → curated list used downstream

---

## Input

### 1. AOI (from Step 01)

```
aoi/EL522_Thessaloniki.geojson
```

### 2. Parameters in `config/pipeline.thess.yaml`

```yaml
scene_catalog:
  days: 365
  cloud_cover_max: 20.0
  max_items: 5000
  collection: "sentinel-2-l2a"
  use_tile_selector: true
  full_cover_threshold: 0.999
  allow_union: true
  n_anchors: 24
  window_days: 21
  max_union_tiles: 2
```

### 3. STAC provider

Default: **Copernicus Data Space Ecosystem (CDSE)**

---

## Output

Two CSV files stored in:

```
outputs/tables/
```

### 1. `scenes_s2_all.csv`
- Raw list from STAC API
- Contains metadata for *all* returned scenes

### 2. `scenes_selected.csv`
Filtered according to:
- AOI intersection score  
- cloud cover  
- tile selector rules  
- temporal window construction  

Used for:
- Assets manifest  
- Monthly NDVI composites  
- Climatology & anomaly detection  
- Pixel feature extraction  

---

## Choices

### Cloud Filtering  
Scenes exceeding `cloud_cover_max` (default: 20%) are discarded.

### Tile Selector  
If `use_tile_selector = true`, only scenes with AOI coverage ≥ `full_cover_threshold` (default: 0.999) are kept.  

This avoids partial coverage from neighboring tiles.

### Tile Union  
If `allow_union = true`, up to `max_union_tiles` tiles may be merged to achieve full AOI coverage.

### Temporal Sampling  
The catalog computes:
- `n_anchors` temporal anchors (e.g., 24)
- Each anchor covers `window_days` (e.g., ±21 days)
- The best scene per anchor is selected

---

## Process

### 1. Query STAC API
A search query is composed using:
- AOI geometry  
- time range (`days` look-back)  
- chosen collection (`sentinel-2-l2a`)  

### 2. Retrieve candidate scenes
Returned items include:
- acquisition date  
- cloud cover  
- tile/granule ID  
- asset links (B04, B08, SCL)  

### 3. Apply filtering
- Cloud threshold  
- AOI × tile coverage  
- Geometry validity  

### 4. Tile selection logic
- If AOI spans multiple MGRS tiles, apply merging under constraints  
- If merging fails, only the dominant tile is selected  

### 5. Temporal anchor selection
- Anchor dates evenly spaced  
- Best scene per anchor chosen based on:
  - cloud cover  
  - proximity to anchor date  
  - coverage score  

### 6. Save results
Write two CSVs:

```
outputs/tables/scenes_s2_all.csv
outputs/tables/scenes_selected.csv
```

---

## How to Run It

### Option 1 — Via Makefile (recommended)

```bash
make scene-catalog
```

This automatically loads the parameters from  
`config/pipeline.thess.yaml`.

### Option 2 — Direct entrypoint

```bash
python -m thess_geo_analytics.entrypoints.BuildSceneCatalog     aoi/EL522_Thessaloniki.geojson     365 20 5000 sentinel-2-l2a     1 0.999 1 24 21 2
```

Arguments correspond exactly to the YAML configuration.

---

## Dependencies

- Step 01 AOI extraction  
- Internet access for STAC queries  

---

## Limitations

- STAC API rate limits may restrict large searches.
- Cloud cover metadata is dependent on provider quality.
- Tile merging rules do not yet consider per-band masking or precise visibility.
- No retry logic for transient STAC network errors (planned enhancement).

---

## Next Step

Proceed to **Step 03 — Assets Manifest**, where the selected scenes are resolved into concrete URLs for raw band assets 
(B04, B08, SCL) and optionally cached or uploaded to GCS.

