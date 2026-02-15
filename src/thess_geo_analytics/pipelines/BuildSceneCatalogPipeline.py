# thess_geo_analytics/pipelines/BuildSceneCatalogPipeline.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from shapely.geometry import shape

from thess_geo_analytics.builders.SceneCatalogBuilder import SceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION
from thess_geo_analytics.geo.TileSelector import TileSelector
from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildSceneCatalogParams:
    # STAC query range
    days: int = 365
    cloud_cover_max: float = 20.0
    max_items: int = 5000
    collection: str = DEFAULT_COLLECTION

    # Selection
    use_tile_selector: bool = True

    # Regular time series params (your rule)
    n_anchors: int = 24
    window_days: int = 21

    # Union / coverage behavior
    full_cover_threshold: float = 0.999
    allow_union: bool = True
    max_union_tiles: int = 2


class BuildSceneCatalogPipeline:
    def __init__(self, aoi_path: Path, builder: SceneCatalogBuilder | None = None) -> None:
        self.aoi_path = aoi_path
        self.builder = builder or SceneCatalogBuilder()

    def run(self, params: BuildSceneCatalogParams = BuildSceneCatalogParams()) -> Path:
        end = date.today()
        start = end - timedelta(days=params.days)

        stac_params = StacQueryParams(
            collection=params.collection,
            cloud_cover_max=params.cloud_cover_max,
            max_items=params.max_items,
        )

        # 1) Query STAC as items (needed for geometry-based selection)
        items, aoi_geom_geojson = self.builder.build_scene_items(
            aoi_path=self.aoi_path,
            date_start=start.isoformat(),
            date_end=end.isoformat(),
            params=stac_params,
        )

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)

        raw_csv = RepoPaths.table("scenes_catalog.csv")
        selected_csv = RepoPaths.table("scenes_selected.csv")
        ts_csv = RepoPaths.table("time_serie.csv")

        # 2) Always write RAW catalog
        raw_df = self.builder.build_scene_catalog_df(items, collection=params.collection) if items else pd.DataFrame(
            columns=["id", "datetime", "cloud_cover", "platform", "constellation", "collection"]
        )
        raw_df.to_csv(raw_csv, index=False)

        print(f"[OK] scenes_catalog exported => {raw_csv}")
        print(f"[OK] Raw scenes found: {len(raw_df)}")

        # If no items, also write empty outputs and exit
        if not items or raw_df.empty or not params.use_tile_selector:
            if params.use_tile_selector:
                pd.DataFrame(
                    columns=[
                        "anchor_date",
                        "acq_datetime",
                        "id",
                        "datetime",
                        "cloud_cover",
                        "platform",
                        "constellation",
                        "collection",
                    ]
                ).to_csv(selected_csv, index=False)

                pd.DataFrame(
                    columns=[
                        "anchor_date",
                        "acq_datetime",
                        "tile_ids",
                        "tiles_count",
                        "cloud_score",
                        "coverage_frac",
                    ]
                ).to_csv(ts_csv, index=False)

                print(f"[OK] scenes_selected exported => {selected_csv} (empty)")
                print(f"[OK] time_serie exported     => {ts_csv} (empty)")

            return raw_csv

        # 3) Selection -> selected_scenes (regular anchors)
        aoi_shp = shape(aoi_geom_geojson)

        selector = TileSelector(
            full_cover_threshold=params.full_cover_threshold,
            allow_union=params.allow_union,
            max_union_tiles=params.max_union_tiles,
        )

        selected_scenes = selector.select_regular_time_series(
            items=items,
            aoi_geom_4326=aoi_shp,
            period_start=start,
            period_end=end,
            n_anchors=params.n_anchors,
            window_days=params.window_days,
        )

        # 4) Write scenes_selected (tile-level)
        selected_df = self.builder.selected_scenes_to_selected_tiles_df(
            selected_scenes,
            collection=params.collection,
        )
        selected_df.to_csv(selected_csv, index=False)

        # 5) Write time_serie (anchor-level)
        ts_df = self.builder.selected_scenes_to_time_serie_df(selected_scenes)
        ts_df.to_csv(ts_csv, index=False)

        print(f"[OK] scenes_selected exported => {selected_csv}")
        print(f"[OK] time_serie exported     => {ts_csv}")
        print(f"[OK] Anchors requested: {params.n_anchors}, anchors with selection: {len(ts_df)}")

        if not ts_df.empty:
            print(
                f"[OK] Coverage frac: min={ts_df['coverage_frac'].min():.3f} "
                f"median={ts_df['coverage_frac'].median():.3f} max={ts_df['coverage_frac'].max():.3f}"
            )
            print(
                f"[OK] Cloud score:   min={ts_df['cloud_score'].min():.2f} "
                f"median={ts_df['cloud_score'].median():.2f} max={ts_df['cloud_score'].max():.2f}"
            )

        return ts_csv
