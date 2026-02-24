from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.core.mode_settings import ModeSettings
from thess_geo_analytics.core import settings


CONFIG_PATH = RepoPaths.ROOT / "config" / "pipeline.thess.yaml"


@dataclass(frozen=True)
class PipelineConfig:
    raw: Dict[str, Any]

    # ---- mode / debug ----
    @property
    def mode_settings(self) -> ModeSettings:
        # centralize construction so we don't duplicate the logic
        return ModeSettings.from_raw_config(self.raw)

    @property
    def mode(self) -> str:
        return self.mode_settings.mode

    @property
    def debug(self) -> bool:
        # keep debug as a simple flag in YAML
        return bool(self.raw.get("debug", False))

    # ---- AOI / region ----
    @property
    def region_name(self) -> str:
        """
        New config: region is a string (e.g. 'Thessaloniki').
        Older config: region: { name: ... }.
        Support both for robustness.
        """
        region = self.raw.get("region")
        if isinstance(region, dict):
            return region.get("name", "")
        return region or ""

    @property
    def aoi_id(self) -> str:
        """
        New config: top-level aoi_id: "el522".
        Older config: aoi: { id: ... }.
        """
        if "aoi_id" in self.raw:
            return self.raw["aoi_id"]
        aoi = self.raw.get("aoi", {})
        return aoi.get("id", "")

    @property
    def aoi_path(self) -> Path:
        """
        Prefer deriving AOI path from aoi_id and region via settings,
        but still support explicit aoi.file from older configs.
        """
        # If old-style config has aoi.file, honor it
        aoi = self.raw.get("aoi", {})
        if "file" in aoi:
            return RepoPaths.aoi(aoi["file"])

        if not self.aoi_id or not self.region_name:
            raise ValueError("aoi_id and region must be set in pipeline.thess.yaml")

        # Example naming convention "EL522_Thessaloniki.geojson"
        filename = f"{self.aoi_id.upper()}_{self.region_name}.geojson"
        return settings.AOI_DIR / filename

    # ---- pipeline dates ----
    @property
    def date_start(self) -> str:
        return self.raw.get("pipeline", {}).get("date_start", "2021-01-01")

    @property
    def date_end(self) -> Optional[str]:
        return self.raw.get("pipeline", {}).get("date_end")

    # ---- raster ----
    @property
    def raster_resolution(self) -> float:
        """
        User-facing resolution in meters (e.g. 10 for S2).
        Falls back to settings.DEFAULT_RASTER_RESOLUTION if absent.
        """
        r = self.raw.get("raster", {})
        return float(r.get("resolution", settings.DEFAULT_RASTER_RESOLUTION))

    # ---- tables (now mostly from settings, but keep backward compatibility) ----
    @property
    def scene_catalog_csv(self) -> Path:
        """
        Prefer settings.SCENE_CATALOG_TABLE, but support old
        tables.scene_catalog if present.
        """
        tables = self.raw.get("tables", {})
        if "scene_catalog" in tables:
            return RepoPaths.table(tables["scene_catalog"])
        return settings.SCENE_CATALOG_TABLE

    @property
    def scenes_selected_csv(self) -> Path:
        tables = self.raw.get("tables", {})
        if "scenes_selected" in tables:
            return RepoPaths.table(tables["scenes_selected"])
        return settings.SCENES_SELECTED_TABLE

    @property
    def assets_manifest_csv(self) -> Path:
        tables = self.raw.get("tables", {})
        if "assets_manifest" in tables:
            return RepoPaths.table(tables["assets_manifest"])
        return settings.ASSETS_MANIFEST_TABLE

    @property
    def ndvi_period_stats_csv(self) -> Path:
        tables = self.raw.get("tables", {})
        if "ndvi_period_stats" in tables:
            return RepoPaths.table(tables["ndvi_period_stats"])
        return settings.NDVI_PERIOD_STATS_TABLE

    # ---- raw params (user-facing parts) ----
    @property
    def scene_catalog_params(self) -> Dict[str, Any]:
        """
        Direct view of user-facing scene_catalog config.
        Mode-aware adjustments live in ModeSettings (effective_*).
        """
        return self.raw.get("scene_catalog", {})

    @property
    def assets_manifest_params(self) -> Dict[str, Any]:
        return self.raw.get("assets_manifest", {})

    @property
    def ndvi_composite_params(self) -> Dict[str, Any]:
        # new name: ndvi_composites
        if "ndvi_composites" in self.raw:
            return self.raw["ndvi_composites"]
        # fallback for older config name
        return self.raw.get("ndvi_monthly_composite", {})

    @property
    def ndvi_period_stats_params(self) -> Dict[str, Any]:
        # if you still have a dedicated section; else fallback to empty
        return self.raw.get("ndvi_period_stats", {})

    @property
    def timestamps_aggregation_params(self) -> Dict[str, Any]:
        return self.raw.get("timestamps_aggregation", {})

    # ---- effective params (mode-aware) ----
    @property
    def effective_scene_catalog_params(self) -> Dict[str, Any]:
        """
        Let ModeSettings shrink/max things for dev/deep.
        It should internally look at cloud_cover_max, max_items, etc.
        """
        return self.mode_settings.effective_scene_catalog(self.scene_catalog_params)

    @property
    def effective_assets_manifest_params(self) -> Dict[str, Any]:
        return self.mode_settings.effective_assets_manifest(self.assets_manifest_params)

    @property
    def effective_ndvi_composite_params(self) -> Dict[str, Any]:
        return self.mode_settings.effective_ndvi_composites(self.ndvi_composite_params)

    @property
    def effective_timestamps_aggregation_params(self) -> Dict[str, Any]:
        # add a helper in ModeSettings if you like; otherwise this can just
        # return timestamps_aggregation_params for now.
        ms = self.mode_settings
        if hasattr(ms, "effective_timestamps_aggregation"):
            return ms.effective_timestamps_aggregation(self.timestamps_aggregation_params)
        return self.timestamps_aggregation_params

    # ---- upload defaults (now mostly in settings, keep YAML override) ----
    @property
    def upload_composites_bucket(self) -> str:
        upload = self.raw.get("upload", {}).get("composites", {})
        return upload.get("bucket", settings.UPLOAD_COMPOSITES_BUCKET)

    @property
    def upload_composites_prefix(self) -> str:
        upload = self.raw.get("upload", {}).get("composites", {})
        return upload.get("remote_prefix", settings.UPLOAD_COMPOSITES_PREFIX)

    @property
    def upload_pixel_features_bucket(self) -> str:
        upload = self.raw.get("upload", {}).get("pixel_features", {})
        return upload.get("bucket", settings.UPLOAD_PIXEL_FEATURES_BUCKET)

    @property
    def upload_pixel_features_prefix(self) -> str:
        upload = self.raw.get("upload", {}).get("pixel_features", {})
        return upload.get("remote_prefix", settings.UPLOAD_PIXEL_FEATURES_PREFIX)

    @property
    def upload_raw_s2_bucket(self) -> str:
        upload_raw = self.raw.get("upload", {}).get("raw_s2", {})
        return upload_raw.get("bucket", settings.UPLOAD_RAW_S2_BUCKET)

    @property
    def upload_raw_s2_prefix(self) -> str:
        upload_raw = self.raw.get("upload", {}).get("raw_s2", {})
        return upload_raw.get("remote_prefix", settings.UPLOAD_RAW_S2_PREFIX)


def load_pipeline_config(path: Optional[Path] = None) -> PipelineConfig:
    cfg_path = path or CONFIG_PATH
    with cfg_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return PipelineConfig(raw=data)