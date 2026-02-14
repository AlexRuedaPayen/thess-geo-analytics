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
    days: int = 90
    cloud_cover_max: float = 20.0
    max_items: int = 300
    collection: str = DEFAULT_COLLECTION

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

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)

        raw_csv = RepoPaths.table("scenes_catalog.csv")
        selected_csv = RepoPaths.table("scenes_selected.csv")

        if not items:
            empty = pd.DataFrame(
                columns=["id", "datetime", "cloud_cover", "platform", "constellation", "collection"]
            )
            empty.to_csv(raw_csv, index=False)
            if params.use_tile_selector:
                empty.to_csv(selected_csv, index=False)

            print(f"[OK] STAC catalog exported => {raw_csv}")
            print("[OK] Scenes found: 0")
            if params.use_tile_selector:
                print(f"[OK] Selected scenes exported => {selected_csv}")
            return selected_csv if params.use_tile_selector else raw_csv

        # 2) Always write RAW catalog first
        raw_df = self.builder.service.items_to_dataframe(items, collection=params.collection)
        raw_df.to_csv(raw_csv, index=False)
        print(f"[OK] STAC catalog exported => {raw_csv}")
        print(f"[OK] Raw scenes found: {len(raw_df)}")

        # 3) Optional selection -> write scenes_selected.csv
        if params.use_tile_selector:
            aoi_shp = shape(aoi_geom_geojson)
            selector = TileSelector(
                full_cover_threshold=params.full_cover_threshold,
                allow_pair=params.allow_pair,
            )

            by_date = selector.select_best_items_per_date(items, aoi_shp)
            selected_items = [it for d in sorted(by_date) for it in by_date[d]]

            selected_df = self.builder.service.items_to_dataframe(
                selected_items, collection=params.collection
            )
            selected_df.to_csv(selected_csv, index=False)

            print(f"[OK] Selected scenes exported => {selected_csv}")
            print(f"[OK] Selected scenes: {len(selected_df)}")
            print(f"[OK] Dates kept: {selected_df['datetime'].dt.date.nunique() if not selected_df.empty else 0}")

            return selected_csv

        # 4) If selector disabled, the ingestable file is the raw catalog
        return raw_csv
