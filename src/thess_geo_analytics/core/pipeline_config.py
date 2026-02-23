from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.core.mode_settings import ModeSettings


CONFIG_PATH = RepoPaths.ROOT / "config" / "pipeline.thess.yaml"


@dataclass(frozen=True)
class PipelineConfig:
    raw: Dict[str, Any]

    # ---- mode ----
    @property
    def mode(self) -> str:
        return ModeSettings.from_raw_config(self.raw).mode

    @property
    def mode_settings(self) -> ModeSettings:
        return ModeSettings.from_raw_config(self.raw)

    # ---- AOI / region ----
    @property
    def region_name(self) -> str:
        return self.raw["region"]["name"]

    @property
    def aoi_id(self) -> str:
        return self.raw["aoi"]["id"]

    @property
    def aoi_path(self) -> Path:
        return RepoPaths.aoi(self.raw["aoi"]["file"])

    # ---- tables ----
    @property
    def scene_catalog_csv(self) -> Path:
        filename = self.raw["tables"]["scene_catalog"]
        return RepoPaths.table(filename)

    @property
    def scenes_selected_csv(self) -> Path:
        filename = self.raw["tables"]["scenes_selected"]
        return RepoPaths.table(filename)

    @property
    def assets_manifest_csv(self) -> Path:
        filename = self.raw["tables"]["assets_manifest"]
        return RepoPaths.table(filename)

    @property
    def ndvi_period_stats_csv(self) -> Path:
        filename = self.raw["tables"]["ndvi_period_stats"]
        return RepoPaths.table(filename)

    # ---- raw params ----
    @property
    def scene_catalog_params(self) -> Dict[str, Any]:
        return self.raw["scene_catalog"]

    @property
    def assets_manifest_params(self) -> Dict[str, Any]:
        return self.raw["assets_manifest"]

    @property
    def ndvi_composite_params(self) -> Dict[str, Any]:
        return self.raw["ndvi_monthly_composite"]

    @property
    def ndvi_period_stats_params(self) -> Dict[str, Any]:
        return self.raw["ndvi_period_stats"]

    # ---- effective params (mode-aware) ----
    @property
    def effective_scene_catalog_params(self) -> Dict[str, Any]:
        return self.mode_settings.effective_scene_catalog(self.scene_catalog_params)

    @property
    def effective_assets_manifest_params(self) -> Dict[str, Any]:
        return self.mode_settings.effective_assets_manifest(self.assets_manifest_params)

    @property
    def effective_ndvi_composite_params(self) -> Dict[str, Any]:
        return self.mode_settings.effective_ndvi_composites(self.ndvi_composite_params)

    # ---- upload defaults ----
    @property
    def upload_composites_bucket(self) -> str:
        return self.raw["upload"]["composites"]["bucket"]

    @property
    def upload_composites_prefix(self) -> str:
        return self.raw["upload"]["composites"]["remote_prefix"]

    @property
    def upload_pixel_features_bucket(self) -> str:
        return self.raw["upload"]["pixel_features"]["bucket"]

    @property
    def upload_pixel_features_prefix(self) -> str:
        return self.raw["upload"]["pixel_features"]["remote_prefix"]


def load_pipeline_config(path: Optional[Path] = None) -> PipelineConfig:
    cfg_path = path or CONFIG_PATH
    with cfg_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return PipelineConfig(raw=data)