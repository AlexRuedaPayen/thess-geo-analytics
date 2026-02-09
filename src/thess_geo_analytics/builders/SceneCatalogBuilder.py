from __future__ import annotations

from pathlib import Path
import pandas as pd

from thess_geo_analytics.services.CdseSceneCatalogService import CdseSceneCatalogService
from thess_geo_analytics.core.params import StacQueryParams


class SceneCatalogBuilder:
    """
    Builds a scene catalog (DataFrame) by querying STAC.
    Thin abstraction: service call + returns a tabular artifact.
    """

    def __init__(self, service: CdseSceneCatalogService | None = None) -> None:
        self.service = service or CdseSceneCatalogService()

    def build_scene_catalog(
        self,
        aoi_path: Path,
        date_start: str,
        date_end: str,
        params: StacQueryParams,
    ) -> pd.DataFrame:
        return self.service.search_scenes(
            aoi_geojson_path=aoi_path,
            date_start=date_start,
            date_end=date_end,
            params=params,
        )
