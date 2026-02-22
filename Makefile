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

SCENE_CATALOG_CSV := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.scene_catalog_csv)")
SCENES_SELECTED_CSV := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.scenes_selected_csv)")
ASSETS_MANIFEST_CSV := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.assets_manifest_csv)")
NDVI_PERIOD_STATS_CSV := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.ndvi_period_stats_csv)")

UPLOAD_COMPOSITES_BUCKET := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.upload_composites_bucket)")
UPLOAD_COMPOSITES_PREFIX := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.upload_composites_prefix)")
UPLOAD_PIXEL_BUCKET := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.upload_pixel_features_bucket)")
UPLOAD_PIXEL_PREFIX := $(shell $(PYTHON) -c "from $(PIPELINE_CONFIG_MODULE) import load_pipeline_config; cfg = load_pipeline_config(); print(cfg.upload_pixel_features_prefix)")

COGS_DIR ?= outputs/cogs
PNG_DIR ?= outputs/png
PIXEL_FEATURES_PATH ?= $(COGS_DIR)/pixel_features_7d.tif

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
	@echo "[RUN] BuildSceneCatalog for AOI=$(AOI_FILE)"
	@echo "$(PYTHON) -m thess_geo_analytics.entrypoints.BuildSceneCatalog aoi/$(AOI_FILE)"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildSceneCatalog aoi/$(AOI_FILE)

# 3. Build assets manifest
.PHONY: assets-manifest
assets-manifest:
	@echo "[RUN] BuildAssetsManifest (scenes_selected -> assets_manifest_selected)"
	# Uses defaults inside the entrypoint (scenes_selected.csv, etc.)
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