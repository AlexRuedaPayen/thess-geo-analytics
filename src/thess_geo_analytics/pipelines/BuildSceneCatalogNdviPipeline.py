from __future__ import annotations

import pandas as pd

from thess_geo_analytics.builders.NdviSceneCatalogBuilder import NdviSceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.pipelines.BaseBuildSceneCatalogPipeline import (
    BaseBuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)


class BuildSceneCatalogNdviPipeline(BaseBuildSceneCatalogPipeline):
    def __init__(self, aoi_path, service=None) -> None:
        super().__init__(
            aoi_path=aoi_path,
            builder=NdviSceneCatalogBuilder(service=service),
        )

    def build_stac_params(self, params: BuildSceneCatalogParams) -> StacQueryParams:
        return StacQueryParams(
            collection=params.collection,
            cloud_cover_max=params.cloud_cover_max,
            max_items=params.max_items,
        )

    def empty_raw_columns(self) -> list[str]:
        return [
            "id",
            "datetime",
            "cloud_cover",
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
            "cloud_cover",
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
            "cloud_score",
            "coverage_frac",
            "coverage_area",
        ]

    def empty_coverage_columns(self) -> list[str]:
        return [
            "acq_datetime",
            "coverage_frac",
            "tiles_count",
            "min_cloud",
            "max_cloud",
            "has_full_cover",
        ]

    def coverage_row_from_group(self, dt, cis, aoi_area_value: float, params: BuildSceneCatalogParams) -> dict:
        union_geom = cis[0].covered_geom
        for ci in cis[1:]:
            union_geom = union_geom.union(ci.covered_geom)

        coverage_frac = float(union_geom.area) / float(aoi_area_value)
        tiles_count = len(cis)
        min_cloud = float(min(ci.cloud for ci in cis))
        max_cloud = float(max(ci.cloud for ci in cis))
        has_full_cover = coverage_frac >= params.full_cover_threshold

        return {
            "acq_datetime": dt.isoformat(),
            "coverage_frac": coverage_frac,
            "tiles_count": tiles_count,
            "min_cloud": min_cloud,
            "max_cloud": max_cloud,
            "has_full_cover": has_full_cover,
        }

    def print_time_series_summary(self, ts_df: pd.DataFrame) -> None:
        super().print_time_series_summary(ts_df)

        if not ts_df.empty and "cloud_score" in ts_df.columns:
            print(
                f"[INFO] Cloud score:   min={ts_df['cloud_score'].min():.2f} "
                f"median={ts_df['cloud_score'].median():.2f} max={ts_df['cloud_score'].max():.2f}"
            )