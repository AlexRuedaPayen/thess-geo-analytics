# ================================
# Makefile for thess-geo-analytics
# ================================

# ------------------------
# Python selection
# ------------------------

# Default to "python" (Linux / Docker / generic)
PYTHON ?= python

# Local dev override: if Windows venv exists, use it
ifneq ("$(wildcard .venv/Scripts/python.exe)","")
  PYTHON := .venv/Scripts/python.exe
endif

# ------------------------
# Cleaning (OS-agnostic via Python)
# ------------------------

.PHONY: clean
clean:
	@echo "[CLEAN] Removing generated output files..."
	@$(PYTHON) -m thess_geo_analytics.utils.cleanup outputs

.PHONY: clean-hard
clean-hard: clean
	@echo "[CLEAN HARD] Removing AOI cache, scene cache, raw S2 downloads..."
	@$(PYTHON) -m thess_geo_analytics.utils.cleanup hard

.PHONY: clean-cache-s2
clean-cache-s2:
	@echo "[CLEAN] Removing Sentinel-2 cache..."
	@$(PYTHON) -m thess_geo_analytics.utils.cleanup cache_s2

.PHONY: clean-aggregated-raw
clean-aggregated-raw:
	@echo "[CLEAN] Removing aggregated raw rasters..."
	@$(PYTHON) -m thess_geo_analytics.utils.cleanup aggregated_raw

# ------------------------
# Testing
# ------------------------

.PHONY: test
test:
	$(PYTHON) -m unittest discover -s tests/auto/unit -v

# ------------------------
# Visualization / QA
# ------------------------

.PHONY: visualize
visualize:
	@echo "_____________________________________________________________"
	@echo "[QA] Export raster preview PNGs"
	$(PYTHON) -m thess_geo_analytics.tools.visualize_outputs \
		--cogs-dir outputs/cogs \
		--save-previews \
		--out-dir outputs/figures \
		--no-prompt

.PHONY: visualize-interactive
visualize-interactive:
	@echo "_____________________________________________________________"
	@echo "[QA] Interactive raster visualization"
	$(PYTHON) -m thess_geo_analytics.tools.visualize_outputs \
		--cogs-dir outputs/cogs

# ------------------------
# Help
# ------------------------

.PHONY: help
help:
	@echo "Targets:"
	@echo "  make extract-aoi                - extract AOI geometry"
	@echo "  make scene-catalog              - build Sentinel-2 scene catalog"
	@echo "  make assets-manifest            - build assets_manifest_selected.csv"
	@echo "  make timestamps-aggregation     - merge tiles into timestamp rasters"
	@echo "  make downsample-aggregated      - downsample aggregated timestamp rasters"
	@echo "  make ndvi-aggregated-composites - build NDVI composites"
	@echo "  make monthly-statistics         - NDVI period stats + monthly series + plot"
	@echo "  make ndvi-climatology           - build NDVI climatology baseline"
	@echo "  make ndvi-anomaly-maps          - build NDVI anomaly rasters"
	@echo "  make ndvi-pixel-features        - build 7D pixelwise NDVI features"
	@echo "  make visualize                  - export PNG previews from outputs/cogs to outputs/figures"
	@echo "  make visualize-interactive      - open rasters interactively with matplotlib"
	@echo "  make clean                      - remove generated outputs"
	@echo "  make clean-hard                 - deep cleanup"
	@echo "  make full                       - full EO pipeline"

# ------------------------
# Pipeline steps
# ------------------------

.PHONY: extract-aoi
extract-aoi:
	@echo "_____________________________________________________________"
	@echo "[RUN] ExtractAoi"
	$(PYTHON) -m thess_geo_analytics.entrypoints.ExtractAoi

.PHONY: scene-catalog
scene-catalog:
	@echo "_____________________________________________________________"
	@echo "[RUN] BuildSceneCatalog"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildSceneCatalog

.PHONY: assets-manifest
assets-manifest:
	@echo "_____________________________________________________________"
	@echo "[RUN] BuildAssetsManifest"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildAssetsManifest

.PHONY: timestamps-aggregation
timestamps-aggregation:
	@echo "_____________________________________________________________"
	@echo "[RUN] BuildAggregatedTimestamps"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildAggregatedTimestamps

.PHONY: downsample-aggregated
downsample-aggregated:
	@echo "_____________________________________________________________"
	@echo "[RUN] DownsampleAggregatedTimestamps"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildDownsampledAggregatedTimestamps

.PHONY: ndvi-aggregated-composites
ndvi-aggregated-composites:
	@echo "_____________________________________________________________"
	@echo "[RUN] BuildNdviAggregatedComposite"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviAggregatedComposite

.PHONY: monthly-statistics
monthly-statistics:
	@echo "_____________________________________________________________"
	@echo "[RUN] BuildNdviMonthlyStatistics"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviMonthlyStatistics

.PHONY: ndvi-climatology
ndvi-climatology:
	@echo "_____________________________________________________________"
	@echo "[RUN] BuildNdviClimatology"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviClimatology

.PHONY: ndvi-anomaly-maps
ndvi-anomaly-maps:
	@echo "_____________________________________________________________"
	@echo "[RUN] BuildNdviAnomalyMaps"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildNdviAnomalyMaps

.PHONY: ndvi-pixel-features
ndvi-pixel-features:
	@echo "_____________________________________________________________"
	@echo "[RUN] BuildPixelFeatures"
	$(PYTHON) -m thess_geo_analytics.entrypoints.BuildPixelFeatures

# ------------------------
# Full pipeline
# ------------------------

.PHONY: full
full:
	@echo "_____________________________________________________________"
	@echo "[FULL] Step 1: Extract AOI"
	$(MAKE) extract-aoi

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 2: Scene catalog"
	$(MAKE) scene-catalog

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 3: Assets manifest"
	$(MAKE) assets-manifest

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 4: Aggregate timestamps"
	$(MAKE) timestamps-aggregation

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 5: Downsample aggregated timestamps"
	$(MAKE) downsample-aggregated

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 6: NDVI aggregated composites"
	$(MAKE) ndvi-aggregated-composites

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 7: NDVI monthly statistics"
	$(MAKE) monthly-statistics

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 8: NDVI climatology"
	$(MAKE) ndvi-climatology

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 9: NDVI anomaly maps"
	$(MAKE) ndvi-anomaly-maps

	@echo "_____________________________________________________________"
	@echo "[FULL] Step 10: Pixel features"
	$(MAKE) ndvi-pixel-features

	@echo "✓ FULL EO/NDVI pipeline completed."