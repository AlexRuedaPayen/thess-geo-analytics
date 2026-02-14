# thess_geo_analytics/builders/SceneCatalogBuilder.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.services.CdseSceneCatalogService import CdseSceneCatalogService


class SceneCatalogBuilder:
    """
    Builds a scene catalog by querying STAC.

    Thin abstraction:
      - delegates STAC search to CdseSceneCatalogService
      - returns either raw STAC items (+ AOI geometry) or a tabular DataFrame

    Pipeline decides whether to apply geo.TileSelector.
    """

    def __init__(self, service: CdseSceneCatalogService | None = None) -> None:
        self.service = service or CdseSceneCatalogService()

    # ------------------------------------------------------------------
    # Raw items (needed by TileSelector)
    # ------------------------------------------------------------------
    def build_scene_items(
        self,
        aoi_path: Path,
        date_start: str,
        date_end: str,
        params: StacQueryParams,
    ) -> Tuple[List[Any], Dict[str, Any]]:
        """
        Returns (items, aoi_geometry_geojson).
        """
        return self.service.search_items(
            aoi_geojson_path=aoi_path,
            date_start=date_start,
            date_end=date_end,
            params=params,
        )

    # ------------------------------------------------------------------
    # DataFrame (backward compatible)
    # ------------------------------------------------------------------
    def build_scene_catalog(
        self,
        aoi_path: Path,
        date_start: str,
        date_end: str,
        params: StacQueryParams,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with:
          id, datetime, cloud_cover, platform, constellation, collection
        """
        items, _ = self.build_scene_items(
            aoi_path=aoi_path,
            date_start=date_start,
            date_end=date_end,
            params=params,
        )
        return self.service.items_to_dataframe(items, collection=params.collection)
