# tests/mocks/FakeCdseSceneCatalogService.py

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from thess_geo_analytics.core.params import StacQueryParams


class FakeCdseSceneCatalogService:
    """
    Reusable test double for CdseSceneCatalogService.

    Behaves like a real STAC client at a high level:
      - search_items() applies date range + cloud_cover_max filter
      - items_to_dataframe() returns a realistic catalog DataFrame

    Used across unit tests of the scene catalog pipeline
    and asset-manifest pipeline.
    """

    def __init__(self, items: List[Dict[str, Any]]) -> None:
        self._items = items

    # ----------------------------------------------------------------------
    # STAC-like search filtering
    # ----------------------------------------------------------------------
    def search_items(
        self,
        aoi_geojson_path: Path,
        date_start: str,
        date_end: str,
        params: StacQueryParams,
    ) -> Tuple[List[Any], Dict[str, Any]]:
        # Load AOI geometry
        with aoi_geojson_path.open("r", encoding="utf-8") as f:
            aoi_fc = json.load(f)
        aoi_geom = aoi_fc["features"][0]["geometry"]

        start = date.fromisoformat(date_start)
        end = date.fromisoformat(date_end)
        max_cloud = getattr(params, "cloud_cover_max", None)

        filtered: List[Dict[str, Any]] = []
        for it in self._items:
            props = it.get("properties", {}) or {}

            # Timestamp
            dt_str = props.get("datetime")
            if not dt_str:
                continue

            dt = pd.to_datetime(dt_str, utc=True).date()
            if dt < start or dt > end:
                continue

            # Cloud
            cc_val = props.get("cloud_cover", None)
            try:
                cc_val = float(cc_val)
            except Exception:
                cc_val = float("inf")

            if max_cloud is not None and cc_val > max_cloud:
                continue

            filtered.append(it)

        return filtered, aoi_geom

    # ----------------------------------------------------------------------
    # Convert items to DataFrame (used by SceneCatalogBuilder)
    # ----------------------------------------------------------------------
    def items_to_dataframe(self, items: List[Any], *, collection: str) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for i, it in enumerate(items):
            props = it.get("properties", {}) or {}
            dt_str = props.get("datetime")

            rows.append(
                {
                    "id": it.get("id"),
                    "datetime": dt_str,
                    "cloud_cover": props.get("cloud_cover"),
                    "platform": "sentinel-2a" if (i % 2 == 0) else "sentinel-2b",
                    "constellation": "sentinel-2",
                    "collection": collection,
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

        return df