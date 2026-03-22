from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.services.CdseSceneCatalogService import CdseSceneCatalogService


class BaseSceneCatalogBuilder:
    """
    Base builder interface for scene catalog generation.

    Shared responsibility:
      - query STAC items

    Subclasses define:
      - raw catalog dataframe schema
      - selected scenes dataframe schema
      - time series dataframe schema
    """

    def __init__(self, service: CdseSceneCatalogService | None = None) -> None:
        self.service = service or CdseSceneCatalogService()

    def build_scene_items(
        self,
        aoi_path: Path,
        date_start: str,
        date_end: str,
        params: StacQueryParams,
    ) -> Tuple[List[Any], Dict[str, Any]]:
        return self.service.search_items(
            aoi_geojson_path=aoi_path,
            date_start=date_start,
            date_end=date_end,
            params=params,
        )

    def build_scene_catalog_df(
        self,
        items: List[Any],
        *,
        collection: str,
    ) -> pd.DataFrame:
        raise NotImplementedError

    def selected_scenes_to_time_serie_df(
        self,
        selected_scenes: List[Any],
    ) -> pd.DataFrame:
        raise NotImplementedError

    def selected_scenes_to_selected_tiles_df(
        self,
        selected_scenes: List[Any],
        *,
        collection: str,
    ) -> pd.DataFrame:
        raise NotImplementedError