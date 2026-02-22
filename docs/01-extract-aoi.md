# 01 — Extract AOI

## Overview

The **Area of Interest (AOI)** is a geographically bounded region used as the foundation for all downstream geospatial processing tasks.  
In this first step, the system retrieves and extracts a precise geometric delimitation of the target region, using **EPSG:4326** coordinates.

For this project, the AOI corresponds to a **NUTS-level 2 regional unit**, centered around the broader Thessaloniki area.  
This region combines diverse environmental characteristics:

- urban areas (Thessaloniki city),
- coastal environments (Thermaikos Gulf),
- agricultural zones (Axios–Loudias–Aliakmonas delta),
- mountainous terrain (Mount Chortiatis),
- lakes (Koronia & Volvi).

The objective of this stage is simply to **retrieve, clean, and save** this region in a standardized GeoJSON format.

---

## Input

- A regional or national GIS dataset (e.g., **GISCO NUTS boundaries**)
- Target region name (e.g., `"Thessaloniki"`), configured in:

```
config/pipeline.thess.yaml
```

---

## Output

A cleaned AOI GeoJSON:

```
aoi/<AOI_ID>_<RegionName>.geojson
```

Example:

```
aoi/EL522_Thessaloniki.geojson
```

This file becomes the spatial reference for all subsequent processing steps:
scene catalog construction, NDVI scenes, composites, anomalies, and pixel features.

---

## Choices

No specific algorithmic choices are required in this step.

The AOI is extracted based solely on:
- region name,
- region code (NUTS ID),
- raw GIS input.

---

## Process

### 1. GISCO Service

The AOI extraction uses predefined constants from `core.constants`:

```
GISCO_NUTS_URL
HTTP_TIMEOUT
DEFAULT_NUTS_FILENAME
```

These parameters control the download of the GISCO NUTS dataset (EPSG:4326).

Additional project-level parameters from `core.parameters` specify storage locations:

```
DATA_RAW_DIR
NUTS_LOCAL_PATH
AUTO_DOWNLOAD_GISCO
```

The workflow:

1. Download GISCO regional boundaries (unless already cached).
2. Filter the dataset to isolate the desired region.
3. Validate and normalize geometry.
4. Save the final AOI under `aoi/`.

---

## How to Run It

### 1. Set the region inside **config/pipeline.thess.yaml**

```yaml
region:
  name: "Thessaloniki"

aoi:
  id: "el522"
  file: "EL522_Thessaloniki.geojson"
```

### 2. Run the pipeline

```bash
make extract-aoi
```

This command automatically loads the configured region and writes the AOI GeoJSON.

---

## Dependencies

None.

---

## Limitations

- The extraction does **not** remove low-interest zones (water bodies, urban areas, etc.).  
  Future versions may include semantic masking.
- No full geometry validator is implemented yet (self-intersections, holes, etc.),  
  although the current GISCO geometries are generally clean.  
  A future feature will integrate stricter geometry checks.

---
