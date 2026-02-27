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
	@echo "  make full                       - run full pipeline (AOI → catalog → manifest → aggregation → NDVI → stats)"

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
# Full pipeline
# ----------
.PHONY: full
full: extract-aoi scene-catalog assets-manifest timestamps-aggregation ndvi-aggregated-composites monthly-statistics ndvi-climatology ndvi-anomaly-maps ndvi-pixel-features
	@echo "✓ Data ingestion + NDVI composites + monthly statistics pipeline completed."