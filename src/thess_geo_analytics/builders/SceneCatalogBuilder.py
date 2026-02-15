# thess_geo_analytics/builders/SceneCatalogBuilder.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.services.CdseSceneCatalogService import CdseSceneCatalogService


class SceneCatalogBuilder:
    """
    Builds scene catalogs by querying STAC.

    - build_scene_items(): raw STAC items + AOI geometry (for TileSelector)
    - build_scene_catalog_df(): raw catalog DataFrame from items
    - selected_scenes_to_time_serie_df(): one row per anchor date
    - selected_scenes_to_selected_tiles_df(): tile-level rows used by the selector
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
        return self.service.search_items(
            aoi_geojson_path=aoi_path,
            date_start=date_start,
            date_end=date_end,
            params=params,
        )

    # ------------------------------------------------------------------
    # Raw catalog dataframe
    # ------------------------------------------------------------------
    def build_scene_catalog_df(self, items: List[Any], *, collection: str) -> pd.DataFrame:
        """
        Returns DataFrame with columns:
          id, datetime, cloud_cover, platform, constellation, collection
        """
        return self.service.items_to_dataframe(items, collection=collection)

    # ------------------------------------------------------------------
    # SelectedScene -> time series
    # ------------------------------------------------------------------
    def selected_scenes_to_time_serie_df(self, selected_scenes: List[Any]) -> pd.DataFrame:
        """
        One row per anchor date (fictional grid date).

        Columns:
          anchor_date, acq_datetime, tile_ids, tiles_count, cloud_score, coverage_frac
        """
        rows: List[Dict[str, Any]] = []

        for s in selected_scenes:
            tile_ids: List[str] = []
            for it in s.items:
                if hasattr(it, "id"):
                    tile_ids.append(str(it.id))
                else:
                    tile_ids.append(str(it.get("id")))

            rows.append(
                {
                    "anchor_date": s.anchor_date.isoformat(),
                    "acq_datetime": s.acq_dt.isoformat(),
                    "tile_ids": "|".join(tile_ids),
                    "tiles_count": len(tile_ids),
                    "cloud_score": float(s.cloud_score),
                    "coverage_frac": float(s.coverage_frac),
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df["anchor_date"] = pd.to_datetime(df["anchor_date"]).dt.date
            df["acq_datetime"] = pd.to_datetime(df["acq_datetime"], utc=True)

        return df

    # ------------------------------------------------------------------
    # SelectedScene -> selected tiles catalog
    # ------------------------------------------------------------------
    def selected_scenes_to_selected_tiles_df(self, selected_scenes: List[Any], *, collection: str) -> pd.DataFrame:
        """
        Tile-level rows used by the selector (duplicates allowed across anchors if same acquisition reused).

        Columns:
          anchor_date, acq_datetime, id, datetime, cloud_cover, platform, constellation, collection
        """
        rows: List[Dict[str, Any]] = []

        for s in selected_scenes:
            for it in s.items:
                # reuse service logic by extracting fields from item, but attach anchor info
                item_id = it.id if hasattr(it, "id") else it.get("id")

                # Let the service produce the standard catalog row for this one item
                one_df = self.service.items_to_dataframe([it], collection=collection)
                if one_df.empty:
                    continue
                rec = dict(one_df.iloc[0])

                rec["anchor_date"] = s.anchor_date.isoformat()
                rec["acq_datetime"] = s.acq_dt.isoformat()

                rows.append(rec)

        df = pd.DataFrame(rows)
        if not df.empty:
            df["anchor_date"] = pd.to_datetime(df["anchor_date"]).dt.date
            df["acq_datetime"] = pd.to_datetime(df["acq_datetime"], utc=True)

        # nice ordering
        cols = ["anchor_date", "acq_datetime"] + [c for c in df.columns if c not in {"anchor_date", "acq_datetime"}]
        return df[cols]
