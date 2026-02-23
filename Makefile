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
	@echo "  make ndvi-period-stats     - compute NDVI period stats CSV"
	@echo "  make ndvi-anomalies        - build NDVI anomaly rasters (if entrypoint exists)"
	@echo "  make pixel-features        - build pixel_features_7d.tif (if entrypoint exists)"
	@echo "  make superpixels           - build superpixels rasters (if entrypoint exists)"
	@echo "  make superpixel-features   - build superpixel_features.csv (if entrypoint exists)"
	@echo "  make upload-composites     - upload NDVI composites (COGs+PNGs) to GCS"
	@echo "  make upload-pixel-features - upload pixel_features_7d.tif to GCS"
	@echo "  make full                  - run only AOI extraction + scene catalog"

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

# 4. Build NDVI monthly/quarterly composites
.PHONY: ndvi-composites
ndvi-composites:
	@echo "[RUN] BuildNdviMonthlyComposite"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviMonthlyComposite \
		--aoi $(AOI_FILE) \
		--aoi-id $(AOI_ID) \
		--time-serie $(SCENES_SELECTED_CSV) \
		--assets-manifest $(ASSETS_MANIFEST_CSV)

# 5. NDVI period stats (per composite)
.PHONY: ndvi-period-stats
ndvi-period-stats:
	@echo "[RUN] BuildNdviPeriodStats for all periods"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviPeriodStats \
		--aoi-id $(AOI_ID) \
		--out $(NDVI_PERIOD_STATS_CSV)

# 6. NDVI anomaly maps (assuming this entrypoint exists)
.PHONY: ndvi-anomalies
ndvi-anomalies:
	@echo "[RUN] BuildNdviAnomalyMaps"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviAnomalyMaps

# 7. Pixel features raster (assuming this entrypoint exists)
.PHONY: pixel-features
pixel-features:
	@echo "[RUN] BuildPixelFeatures"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildPixelFeatures

# 8. Superpixels (assuming this entrypoint exists)
.PHONY: superpixels
superpixels:
	@echo "[RUN] BuildSuperpixels"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildSuperpixels

# 9. Superpixel-level features (assuming this entrypoint exists)
.PHONY: superpixel-features
superpixel-features:
	@echo "[RUN] BuildSuperpixelFeatures"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildSuperpixelFeatures



# ----------
# "Full" for now = only AOI + Scene Catalog
# ----------
.PHONY: full
full: extract-aoi scene-catalog
	@echo "✓ AOI extraction + scene catalog completed.