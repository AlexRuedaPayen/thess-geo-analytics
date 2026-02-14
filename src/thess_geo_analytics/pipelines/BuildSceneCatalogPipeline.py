# thess_geo_analytics/pipelines/BuildSceneCatalogPipeline.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from shapely.geometry import shape

from thess_geo_analytics.builders.SceneCatalogBuilder import SceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION
from thess_geo_analytics.geo.tile_selection import TileSelector
from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildSceneCatalogParams:
    days: int = 90
    cloud_cover_max: float = 20.0
    max_items: int = 300
    collection: str = DEFAULT_COLLECTION

    # --- Tile selection (post-processing) ---
    use_tile_selector: bool = True
    full_cover_threshold: float = 0.999
    allow_pair: bool = True


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

        if not items:
            RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)
            out_csv = RepoPaths.table("scenes_catalog.csv")
            pd.DataFrame(
                columns=["id", "datetime", "cloud_cover", "platform", "constellation", "collection"]
            ).to_csv(out_csv, index=False)
            print(f"[OK] STAC catalog exported => {out_csv}")
            print("[OK] Scenes found: 0")
            return out_csv

        # 2) Optional post-processing: least-cloudy coverage-per-date selection
        selected_items = items
        if params.use_tile_selector:
            aoi_shp = shape(aoi_geom_geojson)
            selector = TileSelector(
                full_cover_threshold=params.full_cover_threshold,
                allow_pair=params.allow_pair,
            )
            by_date = selector.select_best_items_per_date(items, aoi_shp)
            # flatten date->items into a list
            selected_items = [it for d in sorted(by_date) for it in by_date[d]]

        # 3) Convert to DataFrame + write
        df = self.builder.service.items_to_dataframe(selected_items, collection=params.collection)

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)
        out_csv = RepoPaths.table("scenes_catalog.csv")
        df.to_csv(out_csv, index=False)

        print(f"[OK] STAC catalog exported => {out_csv}")
        print(f"[OK] Scenes found: {len(df)}")
        if params.use_tile_selector:
            print(f"[OK] Dates kept: {df['datetime'].dt.date.nunique() if not df.empty else 0}")

        return out_csv
