
# 04 — Timestamp Aggregation (Tile Mosaicking)

## Overview

The **Timestamp Aggregation** step transforms the list of per‑timestamp tile IDs from `time_serie.csv` into actual
**per‑timestamp mosaics** for each required band (typically B04, B08, SCL).

This stage turns *metadata* (lists of tiles) into *raster data* (merged `.tif` mosaics), which are then used for
NDVI computation, composites, climatology, pixel features, and anomaly analyses.

It is the **heaviest I/O and CPU-bound part** of the entire pipeline.

---

## Objectives

- For each timestamp in `time_serie.csv`:
  - Read all referenced tile raster files (`B04.tif`, `B08.tif`, `SCL.tif`, …)
  - Validate their CRS, dtype, and nodata consistency
  - Reproject if necessary (robust CRS-handling mode)
  - Merge them into a single mosaic per band
- Save each timestamp’s output under:

```
DATA_LAKE/data_raw/aggregated/<timestamp>/B04.tif
DATA_LAKE/data_raw/aggregated/<timestamp>/B08.tif
DATA_LAKE/data_raw/aggregated/<timestamp>/SCL.tif
```

- Produce detailed debug- and audit-oriented logs:
  - `timestamps_aggregation_status.csv`
  - `timestamps_aggregation_summary.csv`
  - `timestamps_aggregation_band_report.csv`

---

## Inputs

### 1. `time_serie.csv`

Generated in the Scene Catalog step. Must contain:

- `acq_datetime`
- `tile_ids` (list-like string, e.g. `"id1|id2|id3"`)

### 2. Raw tile cache

```
DATA_LAKE/cache/s2/<scene_id>/<band>.tif
```

### 3. Parameters (`TimestampsAggregationParams`)

```python
max_workers: int              # parallelism (1 = sequential, debug-friendly)
merge_method: str = "first"   # rasterio.merge merge policy
resampling: str = "nearest"   # how to resample when reprojecting
nodata: float = NaN           # nodata override
bands: ("B04","B08","SCL")    # which per-timestamp mosaics we generate
debug: bool = False           # forces sequential execution + full tracebacks
```

---

## Outputs

### 1. Per‑timestamp mosaics  
One folder per timestamp, containing output `.tif` files:

```
DATA_LAKE/data_raw/aggregated/<ts>/B04.tif
DATA_LAKE/data_raw/aggregated/<ts>/B08.tif
DATA_LAKE/data_raw/aggregated/<ts>/SCL.tif
```

### 2. Logs (essential for debugging)

#### `timestamps_aggregation_status.csv`
One row per timestamp, coarse success/failure indicator.

#### `timestamps_aggregation_summary.csv`
Expanded summary: failed bands, missing files, output folder.

#### `timestamps_aggregation_band_report.csv`
Per-band detailed entries:
- list of input files
- output path
- failure reasons
- missing tile details

These allow auditing and re-running individual timestamps.

---

## Internals & Algorithm

### Step 1 — Parse `time_serie.csv`
- Expand tile lists: supports separators `,`, `;`, `|`
- Deduplicate while preserving order
- Validate basic schema

### Step 2 — Validate inputs
For each scene ID:
- check existence of `<band>.tif`
- ensure CRS is known
- ensure dtype and nodata handling is safe

### Step 3 — Robust CRS Handling
This pipeline uses the **reprojection‑aware** merge strategy:

- Read CRS and transform of each tile
- If CRSs disagree → automatically reproject to the first CRS
- Uses `rasterio.warp.reproject`, ensuring full compatibility
- Guarantees no hard crash on CRS mismatch

### Step 4 — Merge / Mosaic
Uses `rasterio.merge.merge` with:

- positional dataset list
- optional nodata override
- controlled merge policy (`first`, preferred)

### Step 5 — Write output safely
- Ensures directory creation
- Applies compression + tiling + BigTIFF where appropriate
- Ensures dtype is preserved or safely promoted

---

## Complexity & Performance Characteristics

| Component | Cost | Notes |
|----------|------|-------|
| Reading tiles | I/O-heavy | SSD strongly recommended |
| Reprojection | CPU-heavy | 2–3× slower for 3–4 tiles per timestamp |
| Merge | Memory-light | Typically <100 MB per tile set |
| Writing output | I/O-heavy | Compression increases CPU usage |
| Parallelism | CPU/I/O bound | Overuse can overwhelm disk bandwidth |

### Recommended Parallelism
- **dev mode**: 2–4 workers  
- **deep mode**: 4–8 workers (if on NVMe SSD)  
- **HDD**: avoid >2 workers  

### Expected Runtime (Thessaloniki example)
- ~46 timestamps  
- 3 tiles per timestamp  
- B04 / B08 / SCL  

**Sequential**: ~45–60 min  
**4 workers**: ~12–18 min  

### Memory Usage
Typical tile size: **20–40 MB**  
Merging 3 tiles × 3 bands ≈ **200–350 MB** peak in-flight RAM.

---

## Debuggability

This step is now **highly observable**:

### 1. Debug Mode
Set `debug=True` → no parallel threads, full exceptions raised.

### 2. Band-Level Logging
Every band generates:
- status
- input files
- output file
- failure reason

### 3. Timestamp-Level Summary
You can quickly identify:
- missing tiles
- CRS conflicts
- invalid rasters
- merge failures
- datatype conflicts

### 4. Folder-Oriented Outputs
Each timestamp mosaic is isolated → you can inspect/resume easily.

---

## How to Run

### Option 1 — Makefile
```bash
make timestamp-aggregation
```

### Option 2 — Direct entrypoint
```bash
python -m thess_geo_analytics.entrypoints.BuildAggregatedTimestamps
```

Optional flags can be added if you want to expose them later.

---

## Limitations & Future Work

- No raster checksum verification (planned)
- No automatic fallback to raw re-download (planned)
- No patch-wise merging for massive AOIs (for V2)
- Cloud masking per-pixel not yet implemented

---

## Next Step

Proceed to **NDVI Computation / Composite Generation**, which uses the aggregated mosaics to build monthly and quarterly temporal products.
