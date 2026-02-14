from __future__ import annotations

from pathlib import Path
from datetime import date
from typing import Any, Dict, List, Tuple

import pandas as pd

from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.geo.TileSelector import SelectedScene, TileSelector
from thess_geo_analytics.services.CdseSceneCatalogService import CdseSceneCatalogService


class SceneCatalogBuilder:
    """
    Builds a scene catalog by querying STAC.

    Responsibilities:
      - delegate STAC search to CdseSceneCatalogService
      - return raw STAC items (+ AOI geometry) when needed
      - provide helpers to materialize tabular catalogs

    Notes:
      - The pipeline decides WHICH selection strategy is used (per-date vs regular-grid).
      - This builder exposes a convenience method to build a *regular-grid selected* catalog
        using geo.TileSelector.select_regular_time_series().
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
    # Backward-compatible: all items as a DataFrame
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
        (Contains ALL returned STAC items, no selection.)
        """
        items, _ = self.build_scene_items(
            aoi_path=aoi_path,
            date_start=date_start,
            date_end=date_end,
            params=params,
        )
        return self.service.items_to_dataframe(items, collection=params.collection)


    def build_selected_time_series(
        self,
        aoi_path: Path,
        *,
        period_start: date,
        period_end: date,
        n_anchors: int,
        window_days: int,
        params: StacQueryParams,
        selector: TileSelector | None = None,
    ) -> pd.DataFrame:
        """
        Build a regular-grid "selected scenes" catalog according to TileSelector rule:
          - anchors are midpoints of n_anchors equal subdivisions of [period_start, period_end]
          - for each anchor, choose best real acquisition timestamp within ±window_days//2
          - within a timestamp, choose best union of tiles covering AOI

        Output DataFrame columns (recommended for scenes_selected.csv):
          anchor_date, acq_datetime, n_tiles, coverage_frac, cloud_score,
          scene_ids, tile_ids, platform, constellation, collection
        """
        # Query STAC for the whole period window
        items, aoi_geom_geojson = self.build_scene_items(
            aoi_path=aoi_path,
            date_start=period_start.isoformat(),
            date_end=period_end.isoformat(),
            params=params,
        )

        if not items:
            return pd.DataFrame(
                columns=[
                    "anchor_date",
                    "acq_datetime",
                    "n_tiles",
                    "coverage_frac",
                    "cloud_score",
                    "scene_ids",
                    "platform",
                    "constellation",
                    "collection",
                ]
            )

        selector = selector or TileSelector()

        # Let service parse AOI (keeps this builder free of shapely parsing details)
        # If your service returns raw geojson, we convert to shapely in the pipeline; but here we keep it simple:
        from shapely.geometry import shape as shp_shape

        aoi_shp = shp_shape(aoi_geom_geojson)

        selected: List[SelectedScene] = selector.select_regular_time_series(
            items=items,
            aoi_geom_4326=aoi_shp,
            period_start=period_start,
            period_end=period_end,
            n_anchors=n_anchors,
            window_days=window_days,
        )

        # Build rows
        rows: List[dict] = []
        for s in selected:
            df_items = self.service.items_to_dataframe(s.items, collection=params.collection)

            # derive platform/constellation from first (they should match within S2 collection)
            platform = None
            constellation = None
            if not df_items.empty:
                platform = df_items["platform"].iloc[0] if "platform" in df_items.columns else None
                constellation = df_items["constellation"].iloc[0] if "constellation" in df_items.columns else None

            scene_ids = list(df_items["id"]) if "id" in df_items.columns else []
            rows.append(
                {
                    "anchor_date": pd.to_datetime(s.anchor_date).date(),
                    "acq_datetime": pd.to_datetime(s.acq_dt, utc=True),
                    "n_tiles": len(s.items),
                    "coverage_frac": float(s.coverage_frac),
                    "cloud_score": float(s.cloud_score),
                    "scene_ids": "|".join(scene_ids),
                    "platform": platform,
                    "constellation": constellation,
                    "collection": params.collection,
                }
            )

        out = pd.DataFrame(rows)

        if not out.empty:
            out = out.sort_values(["anchor_date", "cloud_score", "coverage_frac"], ascending=[True, True, False]).reset_index(drop=True)

        return out
