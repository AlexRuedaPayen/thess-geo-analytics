# ================================
# Makefile for thess-geo-analytics
# ================================

# Default to "python" (Linux / Docker / generic)
PYTHON ?= python

# Local dev override: if Windows venv exists, use it
ifneq ("$(wildcard .venv/Scripts/python.exe)","")
  PYTHON := .venv/Scripts/python.exe
endif

PIPELINE_CONFIG_MODULE := thess_geo_analytics.core.pipeline_config

# ------------------------
# Cleaning
# ------------------------
.PHONY: clean
clean:
	@echo "[CLEAN] Removing generated output files..."
	@rm -f outputs/tables/*.csv 2>/dev/null || true
	@rm -f outputs/tables/*.parquet 2>/dev/null || true
	@rm -f outputs/cogs/*.tif 2>/dev/null || true
	@rm -f outputs/png/*.png 2>/dev/null || true
	@rm -rf outputs/composites 2>/dev/null || true
	@echo "[CLEAN] Done."

.PHONY: clean-hard
clean-hard: clean
	@echo "[CLEAN HARD] Removing AOI cache, scene cache, raw S2 downloads..."
	@rm -rf aoi/*.geojson 2>/dev/null || true
	@rm -rf cache/s2 2>/dev/null || true
	@rm -rf cache/s2_downloads 2>/dev/null || true
	@rm -rf cache/nuts 2>/dev/null || true
	@echo "[CLEAN HARD] All cached data removed."

# Data lake step-specific cleans
.PHONY: clean-cache-s2
clean-cache-s2:
	@echo "[CLEAN] Removing Sentinel-2 cache (DATA_LAKE/cache/s2)..."
	@rm -rf DATA_LAKE/cache/s2 2>/dev/null || true

.PHONY: clean-aggregated-raw
clean-aggregated-raw:
	@echo "[CLEAN] Removing aggregated raw rasters (DATA_LAKE/data_raw/aggregated)..."
	@rm -rf DATA_LAKE/data_raw/aggregated 2>/dev/null || true

# ----------
# Help
# ----------
.PHONY: help
help:
	@echo "Targets:"
	@echo "  make extract-aoi                - extract AOI geometry for region (ExtractAoiPipeline)"
	@echo "  make scene-catalog              - build Sentinel-2 scene catalog"
	@echo "  make assets-manifest            - build assets_manifest_selected.csv"
	@echo "  make timestamps-aggregation     - merge all tiles from same timestamp into one (per band)"
	@echo "  make ndvi-aggregated-composites - build NDVI composites from aggregated timestamps"
	@echo "  make monthly-statistics         - build NDVI period stats + monthly time series + plot"
	@echo "  make full                       - run full pipeline (with step-wise cleanup)"

# ----------
# Pipeline steps
# ----------
.PHONY: extract-aoi
extract-aoi:
	@echo "_____________________________________________________________"
	@echo
	@echo "[RUN] ExtractAoi for region from config"
	$(PYTHON) -m thess_geo_analytics.entrypoints.ExtractAoi

.PHONY: scene-catalog
scene-catalog:
	@echo "_____________________________________________________________"
	@echo
	@echo "[RUN] BuildSceneCatalog from config"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildSceneCatalog

.PHONY: assets-manifest
assets-manifest:
	@echo "_____________________________________________________________"
	@echo
	@echo "[RUN] BuildAssetsManifest (manifest only, config-driven)"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildAssetsManifest

.PHONY: timestamps-aggregation
timestamps-aggregation:
	@echo "_____________________________________________________________"
	@echo
	@echo "[RUN] BuildAggregatedTimestamps"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildAggregatedTimestamps

.PHONY: ndvi-aggregated-composites
ndvi-aggregated-composites:
	@echo "_____________________________________________________________"
	@echo
	@echo "[RUN] BuildNdviAggregatedComposite (NDVI from aggregated timestamps)"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviAggregatedComposite

.PHONY: monthly-statistics
monthly-statistics:
	@echo "_____________________________________________________________"
	@echo
	@echo "[RUN] NDVI Monthly Statistics (period stats + time series + plot)"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviMonthlyStatistics

.PHONY: ndvi-climatology
ndvi-climatology:
	@echo "_____________________________________________________________"
	@echo
	@echo "[RUN] BuildNdviClimatology (seasonal NDVI baseline)"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviClimatology

.PHONY: ndvi-anomaly-maps
ndvi-anomaly-maps:
	@echo "_____________________________________________________________"
	@echo
	@echo "[RUN] BuildNdviAnomalyMaps (pixel-wise NDVI anomalies from monthly composites)"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviAnomalyMaps

.PHONY: ndvi-pixel-features
ndvi-pixel-features:
	@echo "_____________________________________________________________"
	@echo
	@echo "[RUN] BuildPixelFeatures (7D pixelwise temporal NDVI features)"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildPixelFeatures

# ----------
# Full pipeline with step-wise cleanup
# ----------
.PHONY: full
full:
	@echo "_____________________________________________________________"
	@echo "[FULL] Step 1/8: Extract AOI"
	$(MAKE) extract-aoi

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 2/8: Build scene catalog"
	$(MAKE) scene-catalog

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 3/8: Build assets manifest"
	$(MAKE) assets-manifest

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 4/8: Aggregate timestamps"
	$(MAKE) timestamps-aggregation

	@echo "_____________________________________________________________"
	@echo "[FULL] Cleanup: remove S2 cache (DATA_LAKE/cache/s2)"
	$(MAKE) clean-cache-s2

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 5/8: Build NDVI aggregated composites"
	$(MAKE) ndvi-aggregated-composites

	@echo "_____________________________________________________________"
	@echo "[FULL] Cleanup: remove aggregated raw rasters (DATA_LAKE/data_raw/aggregated)"
	$(MAKE) clean-aggregated-raw

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 6/8: NDVI monthly statistics"
	$(MAKE) monthly-statistics

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 7/8: NDVI climatology"
	$(MAKE) ndvi-climatology

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 8/8: NDVI anomaly maps"
	$(MAKE) ndvi-anomaly-maps

	@echo "_____________________________________________________________"
	@echo "[FULL] Final step: NDVI pixel features"
	$(MAKE) ndvi-pixel-features

	@echo "✓ Data ingestion + NDVI composites + monthly statistics pipeline completed (with step-wise cleanup)."