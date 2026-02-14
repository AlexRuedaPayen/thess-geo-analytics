from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
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
                allow_pair=params.allow_pair,
            )

            if params.selection_mode == "per_date":
                # anchor_date == acquisition_date
                by_anchor = selector.select_best_items_per_date(items, aoi_shp)

            elif params.selection_mode == "sliding_window":
                by_anchor = selector.select_best_items_sliding_window(
                    items,
                    aoi_shp,
                    window_days=params.window_days,
                    step_days=params.step_days,
                )
            else:
                raise ValueError(f"Unknown selection_mode: {params.selection_mode}")

            # Flatten selected items (keep unique by id)
            seen = set()
            selected_items = []
            for anchor in sorted(by_anchor):
                for it in by_anchor[anchor]:
                    it_id = getattr(it, "id", None) or it.get("id")
                    if it_id in seen:
                        continue
                    seen.add(it_id)
                    selected_items.append(it)

            selected_df = self.builder.service.items_to_dataframe(
                selected_items, collection=params.collection
            )
            selected_df.to_csv(selected_csv, index=False)

            # Build mapping table: anchor_date -> ids + real datetimes
            map_rows = []
            for anchor in sorted(by_anchor):
                anchor_items = by_anchor[anchor]

                ids = []
                dts = []
                for it in anchor_items:
                    it_id = getattr(it, "id", None) or it.get("id")
                    it_dt = None
                    if hasattr(it, "properties"):
                        it_dt = (it.properties or {}).get("datetime")
                    else:
                        it_dt = (it.get("properties", {}) or {}).get("datetime")
                    if it_dt is None and hasattr(it, "datetime") and it.datetime is not None:
                        it_dt = it.datetime.isoformat()

                    ids.append(str(it_id))
                    dts.append(str(it_dt))

                map_rows.append(
                    {
                        "anchor_date": anchor.isoformat(),
                        "scene_ids": ";".join(ids),
                        "scene_datetimes": ";".join(dts),
                        "n_scenes": len(ids),
                    }
                )

            map_df = pd.DataFrame(map_rows)
            map_df.to_csv(map_csv, index=False)

            print(f"[OK] Selected scenes exported => {selected_csv}")
            print(f"[OK] Selection map exported => {map_csv}")
            print(f"[OK] Selected scenes: {len(selected_df)}")
            print(f"[OK] Anchors kept: {len(map_df)}")

            return selected_csv

        # ------------------------------------------------------------------
        # 4) If selector disabled, ingestable file is the raw catalog
        # ------------------------------------------------------------------
        return raw_csv
