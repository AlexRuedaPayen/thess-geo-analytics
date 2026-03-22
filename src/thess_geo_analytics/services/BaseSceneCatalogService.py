from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from pystac_client import Client

from thess_geo_analytics.core.constants import CDSE_CATALOG_STAC_URL
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION, VERBOSE
from thess_geo_analytics.core.params import StacQueryParams


class BaseSceneCatalogService:
    """
    Common STAC catalog service base.

    Owns:
      - loading AOI geometry
      - opening the STAC client
      - executing STAC searches with retries

    Subclasses define:
      - build_query()
      - items_to_dataframe()
    """

    def __init__(self, stac_url: str = CDSE_CATALOG_STAC_URL) -> None:
        self.stac_url = stac_url

    def load_aoi_geometry(self, aoi_geojson_path: str | Path) -> Dict[str, Any]:
        p = Path(aoi_geojson_path)
        if not p.exists():
            raise FileNotFoundError(f"AOI file not found: {p}")

        with p.open("r", encoding="utf-8") as f:
            obj = json.load(f)

        geojson_type = obj.get("type")

        if geojson_type == "Feature":
            geom = obj.get("geometry")
            if not geom:
                raise ValueError(f"AOI Feature has no geometry: {p}")
            return geom

        if geojson_type == "FeatureCollection":
            features = obj.get("features", [])
            if not features:
                raise ValueError(f"AOI FeatureCollection has no features: {p}")
            geom = features[0].get("geometry")
            if not geom:
                raise ValueError(f"First feature has no geometry: {p}")
            return geom

        if "type" in obj and "coordinates" in obj:
            return obj

        raise ValueError(f"Unsupported GeoJSON type in {p}: {geojson_type}")

    def build_query(self, params: StacQueryParams) -> Optional[Dict[str, Any]]:
        return None

    def search_items(
        self,
        aoi_geojson_path: str | Path,
        date_start: str,
        date_end: str,
        params: Optional[StacQueryParams] = None,
    ) -> Tuple[List[Any], Dict[str, Any]]:
        p = params or StacQueryParams(collection=DEFAULT_COLLECTION)

        geom = self.load_aoi_geometry(aoi_geojson_path)

        if VERBOSE:
            print("[INFO] STAC URL:", self.stac_url)
            print("[INFO] Collection:", p.collection)
            print("[INFO] Date range:", f"{date_start}/{date_end}")
            print("[INFO] Max items:", p.max_items)

        client = Client.open(self.stac_url)
        query = self.build_query(p)

        search = client.search(
            collections=[p.collection],
            intersects=geom,
            datetime=f"{date_start}/{date_end}",
            query=query,
            max_items=p.max_items,
            limit=50,
        )

        last_exc = None
        for attempt in range(3):
            try:
                items = list(search.items())
                return items, geom
            except Exception as e:
                last_exc = e
                if VERBOSE:
                    print(
                        f"[WARN] STAC search attempt {attempt + 1}/3 failed: "
                        f"{type(e).__name__}: {e}"
                    )
                if attempt < 2:
                    import time
                    time.sleep(2 * (attempt + 1))

        raise last_exc

    def items_to_dataframe(
        self,
        items: Sequence[Any],
        *,
        collection: str | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError

    def search_scenes(
        self,
        aoi_geojson_path: str | Path,
        date_start: str,
        date_end: str,
        params: Optional[StacQueryParams] = None,
    ) -> pd.DataFrame:
        p = params or StacQueryParams(collection=DEFAULT_COLLECTION)
        items, _ = self.search_items(
            aoi_geojson_path=aoi_geojson_path,
            date_start=date_start,
            date_end=date_end,
            params=p,
        )
        return self.items_to_dataframe(items, collection=p.collection)