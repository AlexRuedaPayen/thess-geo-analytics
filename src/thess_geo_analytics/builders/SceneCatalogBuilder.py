# thess_geo_analytics/builders/SceneCatalogBuilder.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.services.CdseSceneCatalogService import CdseSceneCatalogService


class SceneCatalogBuilder:
    """
    Builds scene catalogs by querying STAC and converting TileSelector outputs
    into tabular CSVs.

    Responsibilities:
      - build_scene_items():
          raw STAC items + AOI geometry (for TileSelector)
      - build_scene_catalog_df():
          raw catalog DataFrame from items
      - selected_scenes_to_time_serie_df():
          one row per anchor date (time series)
      - selected_scenes_to_selected_tiles_df():
          tile-level rows for each SelectedScene
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
        Query STAC and return:
          - items: list of STAC items (pystac.Item or dict-like)
          - aoi_geom_geojson: AOI geometry as GeoJSON dict
        """
        return self.service.search_items(
            aoi_geojson_path=aoi_path,
            date_start=date_start,
            date_end=date_end,
            params=params,
        )

    # ------------------------------------------------------------------
    # Raw catalog dataframe
    # ------------------------------------------------------------------
    def build_scene_catalog_df(
        self,
        items: List[Any],
        *,
        collection: str,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with columns:
          id, datetime, cloud_cover, platform, constellation, collection
        """
        return self.service.items_to_dataframe(items, collection=collection)

    # ------------------------------------------------------------------
    # SelectedScene -> time series (anchor-level)
    # ------------------------------------------------------------------
    def selected_scenes_to_time_serie_df(
        self,
        selected_scenes: List[Any],
    ) -> pd.DataFrame:
        """
        One row per anchor date (fictional grid date).

        Expected SelectedScene attributes:
          - anchor_date: date
          - acq_dt: datetime
          - items: list of STAC items (for tile_ids)
          - cloud_score: float
          - coverage_frac: float in [0, 1]
          - coverage_area: float (same units as TileSelector's AOI area, e.g. m²)

        Output columns:
          anchor_date        (date)
          acq_datetime       (datetime[UTC])
          tile_ids           (pipe-separated string of tile IDs)
          tiles_count        (int)
          cloud_score        (float)
          coverage_frac      (float)
          coverage_area      (float)
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
                    # may be missing on older objects, so guard with getattr
                    "coverage_area": float(getattr(s, "coverage_area", float("nan"))),
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df["anchor_date"] = pd.to_datetime(df["anchor_date"]).dt.date
            df["acq_datetime"] = pd.to_datetime(df["acq_datetime"], utc=True)

        return df

    # ------------------------------------------------------------------
    # SelectedScene -> selected tiles catalog (tile-level)
    # ------------------------------------------------------------------
    def selected_scenes_to_selected_tiles_df(
        self,
        selected_scenes: List[Any],
        *,
        collection: str,
    ) -> pd.DataFrame:
        """
        Tile-level rows used by the selector (duplicates allowed across anchors
        if same acquisition is reused).

        For each SelectedScene s and each item it in s.items, we create one row.

        Expected SelectedScene attributes:
          - anchor_date
          - acq_dt
          - items
          - coverage_frac
          - coverage_area (optional but expected in new flow)

        Base item fields come from CdseSceneCatalogService.items_to_dataframe,
        typically:
          id, datetime, cloud_cover, platform, constellation, collection

        Output columns (order):
          anchor_date
          acq_datetime
          <all item fields from items_to_dataframe(...)>
          coverage_frac_union   (same for all tiles in a SelectedScene)
          coverage_area_union   (same for all tiles in a SelectedScene)
        """
        rows: List[Dict[str, Any]] = []

        for s in selected_scenes:
            for it in s.items:
                # Let the service produce the standard catalog row for this one item
                one_df = self.service.items_to_dataframe([it], collection=collection)
                if one_df.empty:
                    continue

                rec = dict(one_df.iloc[0])

                # Attach anchor information
                rec["anchor_date"] = s.anchor_date.isoformat()
                rec["acq_datetime"] = s.acq_dt.isoformat()

                # Union coverage information for this SelectedScene
                rec["coverage_frac_union"] = float(s.coverage_frac)
                rec["coverage_area_union"] = float(getattr(s, "coverage_area", float("nan")))

                rows.append(rec)

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        # Normalise date / datetime types
        df["anchor_date"] = pd.to_datetime(df["anchor_date"]).dt.date
        df["acq_datetime"] = pd.to_datetime(df["acq_datetime"], utc=True)

        # Nice column ordering: anchor/acq first, then others
        leading = ["anchor_date", "acq_datetime"]
        # keep order but move leading to front
        other_cols = [c for c in df.columns if c not in leading]
        return df[leading + other_cols]