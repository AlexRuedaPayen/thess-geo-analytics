from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from pystac_client import Client

from thess_geo_analytics.parameters.StacQueryParams import StacQueryParams


class CdseSceneCatalogService:
    CDSE_STAC_URL: str = "https://catalogue.dataspace.copernicus.eu/stac"

    def __init__(self) -> None:
        pass

    def load_aoi_geometry(self, aoi_geojson_path: str | Path) -> Dict[str, Any]:
   
        p = Path(aoi_geojson_path)
        if not p.exists():
            raise FileNotFoundError(f"AOI file not found: {p}")

        with p.open("r", encoding="utf-8") as f:
            obj = json.load(f)

        geojson_type = obj.get("type")

        if geojson_type == "Feature":
            return obj["geometry"]

        if geojson_type == "FeatureCollection":
            features = obj.get("features", [])
            if not features:
                raise ValueError("AOI FeatureCollection has no features.")
            return features[0]["geometry"]

        if "type" in obj and "coordinates" in obj:
            return obj

        raise ValueError(f"Unsupported GeoJSON type in {p}: {geojson_type}")

    def search_scenes(
        self,
        aoi_geojson_path: str | Path,
        date_start: str,
        date_end: str,
        params: Optional[StacQueryParams] = None,
    ) -> pd.DataFrame:
        
        effective_params = params or StacQueryParams()
        geom = self.load_aoi_geometry(aoi_geojson_path)

        client = Client.open(self.CDSE_STAC_URL)

        search = client.search(
            collections=[effective_params.collection],
            intersects=geom,
            datetime=f"{date_start}/{date_end}",
            query={"eo:cloud_cover": {"lt": effective_params.cloud_cover_max}},
            max_items=effective_params.max_items,
        )

        items = list(search.items())

        rows: List[Dict[str, Any]] = []
        for it in items:
            props = it.properties or {}
            rows.append(
                {
                    "id": it.id,
                    "datetime": props.get("datetime") or (it.datetime.isoformat() if it.datetime else None),
                    "cloud_cover": props.get("eo:cloud_cover"),
                    "platform": props.get("platform"),
                    "constellation": props.get("constellation"),
                    "collection": effective_params.collection,
                }
            )

        df = pd.DataFrame(rows)

        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df = df.sort_values(["datetime", "cloud_cover"], ascending=[True, True])

        return df
