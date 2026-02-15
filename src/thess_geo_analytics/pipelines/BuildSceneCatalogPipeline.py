from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta,datetime

from pathlib import Path
from typing import Literal

import pandas as pd
from shapely.geometry import shape

from thess_geo_analytics.builders.SceneCatalogBuilder import SceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION
from thess_geo_analytics.geo.TileSelector import TileSelector
from thess_geo_analytics.utils.RepoPaths import RepoPaths


SelectionMode = Literal["per_date", "sliding_window"]


@dataclass(frozen=True)
class BuildSceneCatalogParams:
    # --- STAC query window (relative) ---
    days: int = 90
    cloud_cover_max: float = 20.0
    max_items: int = 300
    collection: str = DEFAULT_COLLECTION

    # --- selection ---
    use_tile_selector: bool = True
    selection_mode: SelectionMode = "sliding_window"

    # coverage rules
    full_cover_threshold: float = 0.999
    allow_pair: bool = True

    # sliding-window params (used if selection_mode == "sliding_window")
    n_anchors: int = 12
    window_days: int = 15
    step_days: int = 15  # usually == window_days to keep output count manageable


class BuildSceneCatalogPipeline:
    def __init__(self, aoi_path: Path, builder: SceneCatalogBuilder | None = None) -> None:
        self.aoi_path = aoi_path
        self.builder = builder or SceneCatalogBuilder()

    def run(self, params: BuildSceneCatalogParams = BuildSceneCatalogParams()) -> Path:
        end = date.today()
        start = end - timedelta(days=params.days)

        stac_params = StacQueryParams(
            collection=params.collection,
            cloud_cover_max=params.cloud_cover_max,
            max_items=params.max_items,
        )

        # 1) Query STAC as items (needed for geometry-based selection)
        items, aoi_geom_geojson = self.builder.build_scene_items(
            aoi_path=self.aoi_path,
            date_start=start.isoformat(),
            date_end=end.isoformat(),
            params=stac_params,
        )

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)

        raw_csv = RepoPaths.table("scenes_catalog.csv")
        selected_csv = RepoPaths.table("scenes_selected.csv")
        map_csv = RepoPaths.table("scenes_selected_map.csv")

        # ------------------------------------------------------------------
        # Empty case
        # ------------------------------------------------------------------
        if not items:
            empty = pd.DataFrame(
                columns=["id", "datetime", "cloud_cover", "platform", "constellation", "collection"]
            )
            empty.to_csv(raw_csv, index=False)
            print(f"[OK] STAC catalog exported => {raw_csv}")
            print("[OK] Raw scenes found: 0")

            if params.use_tile_selector:
                empty.to_csv(selected_csv, index=False)
                pd.DataFrame(columns=["anchor_date", "scene_ids", "scene_datetimes", "n_scenes"]).to_csv(
                    map_csv, index=False
                )
                print(f"[OK] Selected scenes exported => {selected_csv}")
                print(f"[OK] Selection map exported => {map_csv}")

                return selected_csv

            return raw_csv

        # ------------------------------------------------------------------
        # 2) Always write RAW catalog first
        # ------------------------------------------------------------------
        raw_df = self.builder.service.items_to_dataframe(items, collection=params.collection)
        raw_df.to_csv(raw_csv, index=False)
        print(f"[OK] STAC catalog exported => {raw_csv}")
        print(f"[OK] Raw scenes found: {len(raw_df)}")

        # ------------------------------------------------------------------
        # 3) Optional selection -> scenes_selected.csv + scenes_selected_map.csv
        # ------------------------------------------------------------------
        if params.use_tile_selector:
            aoi_shp = shape(aoi_geom_geojson)

            selector = TileSelector(
                full_cover_threshold=params.full_cover_threshold,
                allow_union=True,
                max_union_tiles=2,
            )

            period_start = start
            period_end = end

            selected_scenes = selector.select_regular_time_series(
                items=items,
                aoi_geom_4326=aoi_shp,
                period_start=period_start,
                period_end=period_end,
                n_anchors=params.n_anchors,          # ← you must add this param
                window_days=params.window_days,      # ← and this
            )

            if not selected_scenes:
                print("[WARN] No scenes selected for regular time series.")
                return raw_csv

            # --------------------------------------------------
            # 1) Write scenes_selected.csv (real acquisition info)
            # --------------------------------------------------

            import pdb
            pdb.set_trace()
            selected_rows = []
            for s in selected_scenes:
                for it in s.items:
                    selected_rows.append({
                        "anchor_date": s.anchor_date,
                        "acq_datetime": s.acq_dt,
                        "scene_id": getattr(it, "id", it.get("id")),
                        "cloud_score": s.cloud_score,
                        "coverage_frac": s.coverage_frac,
                    })

            selected_df = pd.DataFrame(selected_rows)
            selected_df.to_csv(selected_csv, index=False)

            print(f"[OK] Selected scenes exported => {selected_csv}")
            print(f"[OK] Anchors: {len(selected_scenes)}")

            # --------------------------------------------------
            # 2) Write anchor → acquisition mapping table
            # --------------------------------------------------
            mapping_rows = [{
                "anchor_date": s.anchor_date,
                "acq_datetime": s.acq_dt,
                "n_tiles": len(s.items),
                "cloud_score": s.cloud_score,
                "coverage_frac": s.coverage_frac,
            } for s in selected_scenes]

            mapping_df = pd.DataFrame(mapping_rows)
            mapping_path = RepoPaths.table("scenes_anchor_mapping.csv")
            mapping_df.to_csv(mapping_path, index=False)

            print(f"[OK] Anchor mapping exported => {mapping_path}")

            return selected_csv


        # ------------------------------------------------------------------
        # 4) If selector disabled, ingestable file is the raw catalog
        # ------------------------------------------------------------------
        return raw_csv
