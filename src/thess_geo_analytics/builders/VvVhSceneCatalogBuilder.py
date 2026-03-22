from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from thess_geo_analytics.builders.BaseSceneCatalogBuilder import BaseSceneCatalogBuilder


class VvVhSceneCatalogBuilder(BaseSceneCatalogBuilder):
    """
    Sentinel-1 / VV-VH-oriented scene catalog builder.

    Current status:
      placeholder schema until VV/VH processing is fully implemented.
    """

    def build_scene_catalog_df(
        self,
        items: List[Any],
        *,
        collection: str,
    ) -> pd.DataFrame:
        # Placeholder: keep a minimal generic schema
        rows: List[Dict[str, Any]] = []

        for it in items:
            if hasattr(it, "id"):
                item_id = str(it.id)
                props = getattr(it, "properties", {}) or {}
            else:
                item_id = str(it.get("id"))
                props = it.get("properties", {}) or {}

            rows.append(
                {
                    "id": item_id,
                    "datetime": props.get("datetime"),
                    "platform": props.get("platform"),
                    "constellation": props.get("constellation"),
                    "collection": collection,
                }
            )

        return pd.DataFrame(
            rows,
            columns=[
                "id",
                "datetime",
                "platform",
                "constellation",
                "collection",
            ],
        )

    def selected_scenes_to_time_serie_df(
        self,
        selected_scenes: List[Any],
    ) -> pd.DataFrame:
        cols = [
            "anchor_date",
            "acq_datetime",
            "tile_ids",
            "tiles_count",
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
            "platform",
            "constellation",
            "collection",
            "coverage_frac_union",
            "coverage_area_union",
        ]

        rows: List[Dict[str, Any]] = []
        for s in selected_scenes:
            for it in s.items:
                if hasattr(it, "id"):
                    item_id = str(it.id)
                    props = getattr(it, "properties", {}) or {}
                else:
                    item_id = str(it.get("id"))
                    props = it.get("properties", {}) or {}

                rows.append(
                    {
                        "anchor_date": s.anchor_date.isoformat(),
                        "acq_datetime": s.acq_dt.isoformat(),
                        "id": item_id,
                        "datetime": props.get("datetime"),
                        "platform": props.get("platform"),
                        "constellation": props.get("constellation"),
                        "collection": collection,
                        "coverage_frac_union": float(s.coverage_frac),
                        "coverage_area_union": float(getattr(s, "coverage_area", float("nan"))),
                    }
                )

        df = pd.DataFrame(rows)

        if df.empty:
            return pd.DataFrame(columns=base_cols)

        df["anchor_date"] = pd.to_datetime(df["anchor_date"]).dt.date
        df["acq_datetime"] = pd.to_datetime(df["acq_datetime"], utc=True)

        leading = ["anchor_date", "acq_datetime"]
        other_cols = [c for c in df.columns if c not in leading]
        return df[leading + other_cols]