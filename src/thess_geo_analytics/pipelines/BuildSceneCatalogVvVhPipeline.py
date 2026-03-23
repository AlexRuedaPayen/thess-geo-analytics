from __future__ import annotations

from thess_geo_analytics.builders.VvVhSceneCatalogBuilder import VvVhSceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.pipelines.BaseBuildSceneCatalogPipeline import (
    BaseBuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)
from thess_geo_analytics.core.step_paths import (
    MODALITY_VV_VH,
    scene_catalog_step_paths,
)


class BuildSceneCatalogVvVhPipeline(BaseBuildSceneCatalogPipeline):
    def __init__(self, aoi_path, service=None) -> None:
        super().__init__(
            aoi_path=aoi_path,
            builder=VvVhSceneCatalogBuilder(service=service),
        )

    def scene_catalog_paths(self):
        return scene_catalog_step_paths(MODALITY_VV_VH)

    def should_write_legacy_scene_catalog_outputs(self) -> bool:
        return False

    #def run(self, params: BuildSceneCatalogParams) -> str:
    #    print("[INFO] vv_vh is in construction")
    #    return "in construction"

    def build_stac_params(self, params: BuildSceneCatalogParams) -> StacQueryParams:
        return StacQueryParams(
            collection=params.collection,
            max_items=params.max_items,
        )

    def empty_raw_columns(self) -> list[str]:
        return [
            "id",
            "datetime",
            "platform",
            "constellation",
            "collection",
        ]

    def empty_selected_columns(self) -> list[str]:
        return [
            "anchor_date",
            "acq_datetime",
            "id",
            "datetime",
            "platform",
            "constellation",
            "collection",
        ]

    def empty_time_series_columns(self) -> list[str]:
        return [
            "anchor_date",
            "acq_datetime",
            "tile_ids",
            "tiles_count",
            "coverage_frac",
            "coverage_area",
        ]

    def empty_coverage_columns(self) -> list[str]:
        return [
            "acq_datetime",
            "coverage_frac",
            "tiles_count",
            "has_full_cover",
        ]