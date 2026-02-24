# 02 — Scene Catalog (Sentinel-2 STAC)

## Overview

The **Scene Catalog** step talks to a Sentinel-2 STAC API, finds all scenes intersecting the Area of Interest (AOI), filters them, and then builds a **regular time series** of “best” scenes.

It does three main things:

1. **Raw catalog** of all Sentinel-2 L2A scenes over the AOI and time range.
2. **Tile selection** to pick, for each anchor date, the best timestamp and union of tiles that fully cover the AOI.
3. **Coverage diagnostics** to understand which timestamps are usable.

All later steps — assets manifest, NDVI, composites, anomalies, pixel/superpixel features — depend on this.

---

## Objectives

- Query a **STAC API** (Copernicus Data Space Ecosystem, CDSE).
- Retrieve all Sentinel-2 scenes intersecting the AOI within a given period.
- Filter scenes by:
  - global cloud cover (scene‐level metadata),
  - geometric overlap between AOI and tile footprint.
- Build a **regular time series** of scenes by:
  - defining **anchor dates** (equally spaced across the period),
  - for each anchor, finding timestamps whose tiles can fully cover the AOI,
  - choosing the timestamp with **best cloud / coverage trade-off**.
- Save three tables:
  - `scenes_catalog.csv` → raw STAC results (filtered only by STAC params),
  - `scenes_selected.csv` → per-tile list for each chosen anchor+timestamp,
  - `time_serie.csv` → per-anchor time-series metadata,
  - `timestamps_coverage.csv` → per-timestamp coverage diagnostics.

---

## Inputs

### 1. AOI (from Step 01)

GeoJSON produced by the AOI extraction step, e.g.:

```text
aoi/EL522_Thessaloniki.geojson
```

### 2. Global temporal knob (from YAML)

From `config/pipeline.thess.yaml`:

```yaml
pipeline:
  date_start: "2021-01-01"
```

`date_start` is the **only** temporal parameter used by the pipeline.  
`BuildSceneCatalogPipeline` uses:

- `date_start = pipeline.date_start`
- `date_end   = today()`

### 3. Scene catalog parameters (`config/pipeline.thess.yaml`)

```yaml
scene_catalog:
  cloud_cover_max: 20.0
  max_items: 3000
  collection: "sentinel-2-l2a"
  use_tile_selector: true
  full_cover_threshold: 0.95
  allow_union: true
  n_anchors: 64
  window_days: 42
  max_union_tiles: 20
  out_table: "scenes_s2_all.csv"
```

**Key knobs:**

- `cloud_cover_max`  
  Max allowed scene-level cloud percentage. Scenes with higher cloud are dropped **before** any geometric / AOI logic.

- `max_items`  
  Upper bound on the number of STAC items to fetch. This protects you from accidentally pulling 50k scenes. In dev mode, this is further clamped by `ModeSettings`.

- `collection`  
  STAC collection ID. Default: `sentinel-2-l2a`.

- `use_tile_selector`  
  If `false`, the pipeline stops after producing the raw catalog; `scenes_selected.csv`, `time_serie.csv`, and `timestamps_coverage.csv` will be empty.

- `full_cover_threshold`  
  Minimum AOI coverage fraction for a timestamp to be considered **valid**.  
  Example: `0.95` means “we accept up to 5% of the AOI as missing (NoData) for a timestamp, but not more.”

- `allow_union`  
  If `true`, multiple tiles acquired at the same timestamp can be **unioned** to cover the AOI. If `false`, only single-tile coverage is considered.

- `max_union_tiles`  
  Hard cap on how many tiles can be combined at once. Keeps you from using weird slivers that cover 0.001% of the AOI.

- `n_anchors`  
  Number of **anchor dates** in the final regular time series. Anchors are midpoints of equal subdivisions between `date_start` and `date_end`.

- `window_days`  
  Temporal window (in days) around each anchor within which candidate timestamps are considered.  
  With `window_days=42`, each anchor looks at scenes in ±21 days.

### 4. STAC provider

Default: **Copernicus Data Space Ecosystem (CDSE)**.

---

## Algorithm (high level)

### 1. Raw scene catalog

1. Query CDSE for all Sentinel-2 L2A scenes:
   - AOI intersects `aoi/EL522_Thessaloniki.geojson`.
   - `date_start <= datetime <= today`.
   - `cloud_cover <= cloud_cover_max`.
   - Up to `max_items` scenes.

2. Build a DataFrame containing at least:
   - `id`, `datetime`, `cloud_cover`, `platform`, `constellation`, `collection`.

3. Save as:

```text
outputs/tables/scenes_catalog.csv
```

(YAML may alias this as `scenes_s2_all.csv` via `tables.scene_catalog`.)

If no items are found, `scenes_catalog.csv` is written empty and all downstream tables are empty.

---

### 2. Coverage analysis (`timestamps_coverage.csv`)

Before building the regular time series, the pipeline computes **per-timestamp coverage**:

1. Compute intersections between AOI and each tile footprint.
2. Group tiles by their acquisition datetime (`acq_datetime`).
3. For each timestamp:
   - union all tile coverage geometries,
   - compute:
     - `coverage_frac` = AOI area covered / AOI total area,
     - `tiles_count`   = number of tiles at that timestamp,
     - `min_cloud`, `max_cloud` across those tiles,
     - `has_full_cover` = `coverage_frac >= full_cover_threshold`.

4. Save as:

```text
outputs/tables/timestamps_coverage.csv
```

**Example:**

```text
acq_datetime,coverage_frac,tiles_count,min_cloud,max_cloud,has_full_cover
2021-01-05T09:13:51.024000+00:00,0.25339217635186717,6,0.01,4.16,False
2021-01-15T09:13:31.024000+00:00,0.25697978229998825,4,1.97,6.7,False
2021-01-18T09:23:21.024000+00:00,1.0000000000000013,4,3.08,9.07,True
2021-01-28T09:22:41.024000+00:00,1.0000000000000029,6,1.84,15.88,True
```

**Usage**
- Debug: verify that most of your period has at least some `has_full_cover=True` timestamps.
- Control: you can visualize coverage over time and decide if you want to make `full_cover_threshold` looser/tighter.

---

### 3. Filter to full-coverage timestamps

The regular time series is only allowed to use timestamps that **can reach full coverage**.

- From `timestamps_coverage.csv`, we collect all timestamps where `has_full_cover == True`.
- All items belonging to other timestamps are excluded from tile selection.

If **no** timestamp has full coverage:
- `scenes_catalog.csv` is still written,
- `timestamps_coverage.csv` is written,
- `scenes_selected.csv` and `time_serie.csv` are written **empty**,
- the pipeline prints a warning.

---

### 4. Regular time series (anchors + TileSelector)

#### Anchor dates

- The period `[date_start, today]` is split into `n_anchors` equal intervals.
- For each interval we define an **anchor date** at the midpoint.
- Duplicates are avoided; in degenerate cases, anchors are shifted and padded.

#### Per-anchor selection

For each anchor date:

1. Compute window:
   - `anchor ± window_days // 2` (e.g., ±21 days).
2. Within that window, consider all **timestamps** that:
   - have full coverage (`has_full_cover=True` from the prefilter step),
   - have at least one tile intersecting the AOI.
3. For each candidate timestamp:
   - Search over unions of tiles (up to `max_union_tiles`) to find the **best union** that:
     - covers at least `full_cover_threshold` of the AOI,  
     - respects `min_intersection_frac` for each tile (very tiny overlaps are ignored).
   - For each union compute:
     - `coverage_frac` (fraction of AOI covered),
     - `cloud_score` = **max** cloud cover percentage among tiles in the union.
4. Among candidate timestamps, choose the one with:
   1. lowest `cloud_score`,
   2. highest `coverage_frac`,
   3. smallest distance in days to the anchor,
   4. fewest tiles in the union.

This gives **one SelectedScene per anchor** (if any timestamp is available).

---

## Outputs

All tables are stored under:

```text
outputs/tables/
```

### 1. `scenes_catalog.csv`

Raw (but filtered) catalog of all Sentinel-2 scenes intersecting the AOI and meeting `cloud_cover_max`, `max_items`, etc.

Used by:
- Scene inspection and QA,
- Downstream selection (as the “universe” of candidate tiles).

---

### 2. `scenes_selected.csv`

One row **per tile** used in the final time series.

Filtered according to:

- timestamp must have full AOI coverage (`has_full_cover=True`),
- tile must be part of the chosen union for some anchor,
- cloud/temporal selection as described above.

Used for:

- Building the **assets manifest** (`BuildAssetsManifestPipeline`),
- Debugging selected tiles per anchor.

**Example:**

```text
anchor_date,acq_datetime,id,datetime,cloud_cover,platform,constellation,collection,coverage_frac_union,coverage_area_union
2021-01-15,2021-01-18 09:23:21.024000+00:00,S2A_MSIL2A_20210118T092321_N0500_R093_T35TKF_20230527T115723,2021-01-18 09:23:21.024000+00:00,3.1,sentinel-2a,sentinel-2,sentinel-2-l2a,1.0000000000000022,5739455232.61008
2021-01-15,2021-01-18 09:23:21.024000+00:00,S2A_MSIL2A_20210118T092321_N0500_R093_T34TFL_20230527T115723,2021-01-18 09:23:21.024000+00:00,5.16,sentinel-2a,sentinel-2,sentinel-2-l2a,1.0000000000000022,5739455232.61008
2021-01-15,2021-01-18 09:23:21.024000+00:00,S2A_MSIL2A_20210118T092321_N0500_R093_T34TFK_20230527T115723,2021-01-18 09:23:21.024000+00:00,9.07,sentinel-2a,sentinel-2,sentinel-2-l2a,1.0000000000000022,5739455232.61008
2021-02-14,2021-02-27 09:20:31.024000+00:00,S2A_MSIL2A_20210227T092031_N0500_R093_T35TKF_20230524T055517,2021-02-27 09:20:31.024000+00:00,0.0,sentinel-2a,sentinel-2,sentinel-2-l2a,1.0000000000000022,5739455232.61008
...
```

Notes:

- `anchor_date` — regular grid date,
- `acq_datetime` — real acquisition timestamp,
- `coverage_frac_union`, `coverage_area_union` — same values for all tiles in a union (they describe the union, not individual tiles).

---

### 3. `time_serie.csv`

One row **per anchor** (when a scene was successfully selected).

Filtered according to:

- Only anchors with at least one timestamp within the window having full AOI coverage.
- Only timestamps that pass the union/coverage/cloud rules mentioned above.

Used for:

- High-level time-series analysis,
- Choosing which timestamps to aggregate in later steps (e.g., timestamp-level mosaics).

**Example:**

```text
anchor_date,acq_datetime,tile_ids,tiles_count,cloud_score,coverage_frac,coverage_area
2021-01-15,2021-01-18 09:23:21.024000+00:00,S2A_MSIL2A_20210118T092321_N0500_R093_T35TKF_20230527T115723|S2A_MSIL2A_20210118T092321_N0500_R093_T34TFL_20230527T115723|S2A_MSIL2A_20210118T092321_N0500_R093_T34TFK_20230527T115723,3,9.07,1.0000000000000022,5739455232.61008
2021-02-14,2021-02-27 09:20:31.024000+00:00,S2A_MSIL2A_20210227T092031_N0500_R093_T35TKF_20230524T055517|S2A_MSIL2A_20210227T092031_N0500_R093_T34TFL_20230524T055517|S2A_MSIL2A_20210227T092031_N0500_R093_T34TFK_20230524T055517,3,0.0,1.0000000000000022,5739455232.61008
2021-04-13,2021-04-03 09:20:29.024000+00:00,S2B_MSIL2A_20210403T092029_N0500_R093_T35TKE_20230607T120134|S2B_MSIL2A_20210403T092029_N0500_R093_T34TGL_20230607T120134|S2B_MSIL2A_20210403T092029_N0500_R093_T34TFL_20230607T120134|S2B_MSIL2A_20210403T092029_N0500_R093_T34TFK_20230607T120134,4,16.7,1.0000000000000013,5739455232.610075
...
```

Fields:

- `anchor_date`    — regular-grid date,
- `acq_datetime`   — chosen timestamp for that anchor,
- `tile_ids`       — union of tiles (pipe-separated),
- `tiles_count`    — number of tiles in the union,
- `cloud_score`    — max cloud percentage across tiles in the union,
- `coverage_frac`  — AOI fraction covered by the union,
- `coverage_area`  — AOI area covered by the union (m², in the projected CRS used for coverage).

---

### 4. `timestamps_coverage.csv`

Already described above in detail; used as a **debugger and QA** for the time-series coverage.

---

## Design choices

### Cloud filtering

Scenes exceeding `cloud_cover_max` (default 20%) are dropped early.  
Reason: heavily cloudy scenes are rarely useful and slow down all later steps unnecessarily.

Caveat: this is global scene cloud. In a future v2, you might want per-tile (or per-AOI) cloud estimation using SCL or cloud masks.

### Full cover vs partial cover

- Only timestamps where the union of tiles covers at least `full_cover_threshold` of the AOI are allowed into the regular time series.
- Timestamps that only partially cover the AOI remain visible in `timestamps_coverage.csv`, but are never selected for anchors.

This ensures that downstream NDVI / feature rasters are not systematically missing large parts of the AOI.

### Temporal sampling

- `n_anchors` controls how many **effective observations** your time series will have.
- `window_days` controls how tolerant you are to temporal shifts.  
  Bigger window ⇒ more chance to find clear scenes, but less temporal precision.

---

## How to run

### Option 1 — via Makefile (recommended)

```bash
make scene-catalog
```

This:

- reads `config/pipeline.thess.yaml`,
- uses the AOI path from `aoi.file`,
- uses `pipeline.date_start` and `scene_catalog` settings.

### Option 2 — direct entrypoint

The entrypoint now uses **named arguments**, not positional ones:

```bash
python -m thess_geo_analytics.entrypoints.BuildSceneCatalog   --aoi aoi/EL522_Thessaloniki.geojson   --date-start 2021-01-01   --cloud-max 20.0   --max-items 3000   --collection sentinel-2-l2a   --use-tile-selector true   --full-cover-threshold 0.95   --allow-union true   --n-anchors 64   --window-days 42   --max-union-tiles 20
```

If you omit flags, the defaults come from `pipeline.thess.yaml`.

---

## Dependencies

- Step 01 — AOI extraction (must produce the AOI GeoJSON used here)
- Internet access to query the CDSE STAC API

---

## Limitations & future work

- STAC API rate limits can affect very long time ranges or large AOIs.
- Cloud cover uses STAC metadata; per-pixel masks (SCL) are not yet used at selection time.
- Full-cover threshold is global; you might want seasonal or AOI-specific thresholds.
- No retry/backoff logic for transient STAC errors (could be added via a small wrapper).

---

## Next step

Proceed to **Step 03 — Assets Manifest**, where:

- `scenes_selected.csv` is turned into per-scene band URLs (B04, B08, SCL),
- local paths and optional GCS URLs are prepared,
- downloads and validations are orchestrated for the selected tiles.
