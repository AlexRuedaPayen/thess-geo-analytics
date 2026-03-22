from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from thess_geo_analytics.builders.BaseSceneCatalogBuilder import BaseSceneCatalogBuilder


class NdviSceneCatalogBuilder(BaseSceneCatalogBuilder):
    """
    Sentinel-2 / NDVI-oriented scene catalog builder.
    """

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

    def selected_scenes_to_time_serie_df(
        self,
        selected_scenes: List[Any],
    ) -> pd.DataFrame:
        cols = [
            "anchor_date",
            "acq_datetime",
            "tile_ids",
            "tiles_count",
            "cloud_score",
            "coverage_frac",
            "coverage_area",
        ]

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
                    "coverage_area": float(getattr(s, "coverage_area", float("nan"))),
                }
            )

        df = pd.DataFrame(rows, columns=cols)

        if not df.empty:
            df["anchor_date"] = pd.to_datetime(df["anchor_date"]).dt.date
            df["acq_datetime"] = pd.to_datetime(df["acq_datetime"], utc=True)

        return df

    def selected_scenes_to_selected_tiles_df(
        self,
        selected_scenes: List[Any],
        *,
        collection: str,
    ) -> pd.DataFrame:
        base_cols = [
            "anchor_date",
            "acq_datetime",
            "id",
            "datetime",
            "cloud_cover",
            "platform",
            "constellation",
            "collection",
            "coverage_frac_union",
            "coverage_area_union",
        ]

        rows: List[Dict[str, Any]] = []
        for s in selected_scenes:
            for it in s.items:
                one_df = self.service.items_to_dataframe([it], collection=collection)
                if one_df.empty:
                    continue

                rec = dict(one_df.iloc[0])
                rec["anchor_date"] = s.anchor_date.isoformat()
                rec["acq_datetime"] = s.acq_dt.isoformat()
                rec["coverage_frac_union"] = float(s.coverage_frac)
                rec["coverage_area_union"] = float(getattr(s, "coverage_area", float("nan")))
                rows.append(rec)

        df = pd.DataFrame(rows)

        if df.empty:
            return pd.DataFrame(columns=base_cols)

        df["anchor_date"] = pd.to_datetime(df["anchor_date"]).dt.date
        df["acq_datetime"] = pd.to_datetime(df["acq_datetime"], utc=True)

        leading = ["anchor_date", "acq_datetime"]
        other_cols = [c for c in df.columns if c not in leading]
        return df[leading + other_cols]