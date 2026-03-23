from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from thess_geo_analytics.utils.RepoPaths import RepoPaths


STEP_SCENE_CATALOG = "02_scene_catalog"

MODALITY_NDVI = "ndvi"
MODALITY_VV_VH = "vv_vh"
MODALITY_JOINT = "joint"


@dataclass(frozen=True)
class SceneCatalogStepPaths:
    scenes_catalog: Path
    scenes_selected: Path
    time_series: Path
    timestamps_coverage: Path


def scene_catalog_step_paths(modality: str) -> SceneCatalogStepPaths:
    return SceneCatalogStepPaths(
        scenes_catalog=RepoPaths.step_file(
            STEP_SCENE_CATALOG, "scenes_catalog.csv", modality
        ),
        scenes_selected=RepoPaths.step_file(
            STEP_SCENE_CATALOG, "scenes_selected.csv", modality
        ),
        time_series=RepoPaths.step_file(
            STEP_SCENE_CATALOG, "time_series.csv", modality
        ),
        timestamps_coverage=RepoPaths.step_file(
            STEP_SCENE_CATALOG, "timestamps_coverage.csv", modality
        ),
    )