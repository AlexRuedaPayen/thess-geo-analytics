from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from thess_geo_analytics.RepoPaths import RepoPaths
from thess_geo_analytics.CdseSceneCatalogService import CdseSceneCatalogService
from thess_geo_analytics.StacQueryParams import StacQueryParams


class STACCatalogBuilder:


    def __init__(
        self,
        aoi_path: Path,
        days: int = 90,
        cloud_cover_max: float = 20.0,
        max_items: int = 300,
        service: CdseSceneCatalogService | None = None,
    ) -> None:
        self.aoi_path = aoi_path
        self.days = days
        self.cloud_cover_max = cloud_cover_max
        self.max_items = max_items
        self.service = service or CdseSceneCatalogService()

    def run(self) -> Path:
        end = date.today()
        start = end - timedelta(days=self.days)

        params = StacQueryParams(
            collection="sentinel-2-l2a",
            cloud_cover_max=self.cloud_cover_max,
            max_items=self.max_items,
        )

        df = self.service.search_scenes(
            aoi_geojson_path=self.aoi_path,
            date_start=start.isoformat(),
            date_end=end.isoformat(),
            params=params,
        )

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)

        out_csv = RepoPaths.table("scenes_catalog.csv")
        df.to_csv(out_csv, index=False)

        print(f"STAC catalog exported => {out_csv}")
        print(f"Scenes found: {len(df)}")

        return out_csv
