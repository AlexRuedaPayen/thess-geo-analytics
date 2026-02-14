from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from thess_geo_analytics.builders.SceneCatalogBuilder import SceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION
from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildSceneCatalogParams:
    days: int = 90
    cloud_cover_max: float = 20.0
    max_items: int = 300
    collection: str = DEFAULT_COLLECTION


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

        df = self.builder.build_scene_catalog(
            aoi_path=self.aoi_path,
            date_start=start.isoformat(),
            date_end=end.isoformat(),
            params=stac_params,
        )

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)
        out_csv = RepoPaths.table("scenes_catalog.csv")
        df.to_csv(out_csv, index=False)

        print(f"[OK] STAC catalog exported => {out_csv}")
        print(f"[OK] Scenes found: {len(df)}")

        return out_csv