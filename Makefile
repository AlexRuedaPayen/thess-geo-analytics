# ================================
# Makefile for thess-geo-analytics
# ================================

PYTHON ?= .venv/Scripts/python.exe

PIPELINE_CONFIG_MODULE := thess_geo_analytics.core.pipeline_config

# ------------------------
# Config-derived variables
# ------------------------

AOI_FILE := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.raw['aoi']['file'])")
AOI_ID   := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.aoi_id)")
REGION_NAME := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.region_name)")


.PHONY: clean
clean:
	@echo "[CLEAN] Removing generated output files..."
	@rm -f outputs/tables/*.csv 2>/dev/null || true
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

# ----------
# Help
# ----------
.PHONY: help
help:
	@echo "Targets:"
	@echo "  make extract-aoi           - extract AOI geometry for region (ExtractAoiPipeline)"
	@echo "  make scene-catalog         - build Sentinel-2 scene catalog"
	@echo "  make assets-manifest       - build assets_manifest_selected.csv"
	@echo "  make ndvi-composites       - build NDVI monthly/quarterly composites"
	@echo "  make timestamps-aggregation - merge all tiles from same timestamp into one (per band)"

# 1. Extract AOI from larger region (optional)
.PHONY: extract-aoi
extract-aoi:
	@echo "[RUN] ExtractAoi for region=$(REGION_NAME)"
	$(PYTHON) -m thess_geo_analytics.entrypoints.ExtractAoi $(REGION_NAME)

# 2. Build scene catalog
.PHONY: scene-catalog
scene-catalog:
	@echo "[RUN] BuildSceneCatalog from config (AOI=$(AOI_FILE))"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildSceneCatalog

# 3. Build assets manifest
.PHONY: assets-manifest
assets-manifest:
	@echo "[RUN] BuildAssetsManifest (manifest only, light downloads)"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildAssetsManifest

# 4. Timestamp aggregation
.PHONY: timestamps-aggregation
timestamps-aggregation:
	@echo "[RUN] BuildAggregatedTimestamps"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildAggregatedTimestamps

# ----------
# "Full" 
# ----------
.PHONY: full
full: extract-aoi scene-catalog assets-manifest timestamps-aggregation
	@echo "✓ Data ingestion completed.