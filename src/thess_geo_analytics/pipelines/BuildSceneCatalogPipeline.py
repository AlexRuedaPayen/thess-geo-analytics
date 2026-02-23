from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from shapely.geometry import shape

from thess_geo_analytics.builders.SceneCatalogBuilder import SceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION
from thess_geo_analytics.geo.TileSelector import TileSelector
from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildSceneCatalogParams:
    # Single global temporal knob from pipeline.date_start
    date_start: str = "2021-01-01"  # overridden by entrypoint from YAML

    cloud_cover_max: float = 20.0
    max_items: int = 5000
    collection: str = DEFAULT_COLLECTION

    use_tile_selector: bool = True
    full_cover_threshold: float = 0.999
    allow_union: bool = True
    max_union_tiles: int = 2
    n_anchors: int = 24
    window_days: int = 21


class BuildSceneCatalogPipeline:
    def __init__(self, aoi_path: Path, builder: SceneCatalogBuilder | None = None) -> None:
        self.aoi_path = aoi_path
        self.builder = builder or SceneCatalogBuilder()

    def run(self, params: BuildSceneCatalogParams) -> Path:
        # Use absolute date_start instead of "days ago"
        try:
            start = date.fromisoformat(params.date_start)
        except Exception as e:
            raise ValueError(
                f"Invalid date_start={params.date_start!r} (expected YYYY-MM-DD)"
            ) from e

        end = date.today()

        stac_params = StacQueryParams(
            collection=params.collection,
            cloud_cover_max=params.cloud_cover_max,
            max_items=params.max_items,
        )

        items, aoi_geom_geojson = self.builder.build_scene_items(
            aoi_path=self.aoi_path,
            date_start=start.isoformat(),
            date_end=end.isoformat(),
            params=stac_params,
        )
        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)

        raw_csv = RepoPaths.table("scenes_catalog.csv")
        selected_csv = RepoPaths.table("scenes_selected.csv")
        ts_csv = RepoPaths.table("time_serie.csv")
        ts_cov_csv = RepoPaths.table("timestamps_coverage.csv")   # NEW

        # 2) Always write RAW catalog
        raw_df = (
            self.builder.build_scene_catalog_df(items, collection=params.collection)
            if items
            else pd.DataFrame(
                columns=[
                    "id",
                    "datetime",
                    "cloud_cover",
                    "platform",
                    "constellation",
                    "collection",
                ]
            )
        )
        raw_df.to_csv(raw_csv, index=False)

        print(f"[OK] scenes_catalog exported => {raw_csv}")
        print(f"[OK] Raw scenes found: {len(raw_df)}")

        # If no items, also write empty outputs and exit
        if not items or raw_df.empty or not params.use_tile_selector:
            if params.use_tile_selector:
                pd.DataFrame(
                    columns=[
                        "anchor_date",
                        "acq_datetime",
                        "id",
                        "datetime",
                        "cloud_cover",
                        "platform",
                        "constellation",
                        "collection",
                    ]
                ).to_csv(selected_csv, index=False)

                pd.DataFrame(
                    columns=[
                        "anchor_date",
                        "acq_datetime",
                        "tile_ids",
                        "tiles_count",
                        "cloud_score",
                        "coverage_frac",
                    ]
                ).to_csv(ts_csv, index=False)

                # also write empty coverage table
                pd.DataFrame(
                    columns=[
                        "acq_datetime",
                        "coverage_frac",
                        "tiles_count",
                        "min_cloud",
                        "max_cloud",
                        "has_full_cover",
                    ]
                ).to_csv(ts_cov_csv, index=False)

                print(f"[OK] scenes_selected exported => {selected_csv} (empty)")
                print(f"[OK] time_serie exported     => {ts_csv} (empty)")
                print(f"[OK] timestamps_coverage exported => {ts_cov_csv} (empty)")

            return raw_csv

        # 3) Selection -> selected_scenes (regular anchors)
        aoi_shp = shape(aoi_geom_geojson)

        selector = TileSelector(
            full_cover_threshold=params.full_cover_threshold,
            allow_union=params.allow_union,
            max_union_tiles=params.max_union_tiles,
        )

        # ------------------------------------------------------------------
        # 3a) per-timestamp coverage table (dumped vs kept timestamps)
        # ------------------------------------------------------------------
        infos, _, _, aoi_area_value = selector._coverage_infos(
            items, aoi_shp.buffer(0.05)
        )

        cov_rows = []
        if infos and aoi_area_value > 0:
            from collections import defaultdict

            by_ts = defaultdict(list)
            for ci in infos:
                by_ts[ci.acq_dt].append(ci)

            for dt, cis in by_ts.items():
                union_geom = cis[0].covered_geom
                for ci in cis[1:]:
                    union_geom = union_geom.union(ci.covered_geom)

                coverage_frac = float(union_geom.area) / float(aoi_area_value)
                tiles_count = len(cis)
                min_cloud = float(min(ci.cloud for ci in cis))
                max_cloud = float(max(ci.cloud for ci in cis))
                has_full_cover = coverage_frac >= params.full_cover_threshold

                cov_rows.append(
                    {
                        "acq_datetime": dt.isoformat(),
                        "coverage_frac": coverage_frac,
                        "tiles_count": tiles_count,
                        "min_cloud": min_cloud,
                        "max_cloud": max_cloud,
                        "has_full_cover": has_full_cover,
                    }
                )

        cov_df = (
            pd.DataFrame(cov_rows).sort_values("acq_datetime")
            if cov_rows
            else pd.DataFrame(
                columns=[
                    "acq_datetime",
                    "coverage_frac",
                    "tiles_count",
                    "min_cloud",
                    "max_cloud",
                    "has_full_cover",
                ]
            )
        )

        cov_df.to_csv(ts_cov_csv, index=False)
        print(f"[OK] timestamps_coverage exported => {ts_cov_csv}")

        n_full = int(cov_df["has_full_cover"].sum()) if not cov_df.empty else 0
        print(
            f"[OK] Timestamps with full cover (>= {params.full_cover_threshold:.3f}): {n_full}"
        )

        # ------------------------------------------------------------------
        # 3b) FILTER items to timestamps that *can* reach full coverage
        # ------------------------------------------------------------------
        if cov_df.empty:
            print("[WARN] No timestamps with any coverage; skipping TileSelector.")
            items_for_selector: list = []
        else:
            good_ts = set(
                pd.to_datetime(
                    cov_df.loc[cov_df["has_full_cover"], "acq_datetime"]
                ).dt.to_pydatetime()
            )
            print(f"[OK] Timestamps kept for TileSelector (has_full_cover=True): {len(good_ts)}")

            items_for_selector = []
            if good_ts:
                for it in items:
                    try:
                        dt = selector._get_datetime(it)  # reuse same logic as TileSelector
                    except Exception:
                        continue
                    if dt in good_ts:
                        items_for_selector.append(it)

        if not items_for_selector:
            # Nothing to select on → write empty selected/time_serie but keep raw & coverage
            print("[WARN] No items belong to timestamps with full coverage; outputs will be empty.")

            pd.DataFrame(
                columns=[
                    "anchor_date",
                    "acq_datetime",
                    "id",
                    "datetime",
                    "cloud_cover",
                    "platform",
                    "constellation",
                    "collection",
                ]
            ).to_csv(selected_csv, index=False)

            pd.DataFrame(
                columns=[
                    "anchor_date",
                    "acq_datetime",
                    "tile_ids",
                    "tiles_count",
                    "cloud_score",
                    "coverage_frac",
                ]
            ).to_csv(ts_csv, index=False)

            print(f"[OK] scenes_selected exported => {selected_csv} (empty)")
            print(f"[OK] time_serie exported     => {ts_csv} (empty)")
            return ts_csv

        # ------------------------------------------------------------------
        # 4) Call TileSelector on *filtered* items (no bad timestamps)
        # ------------------------------------------------------------------
        # You can keep the try/except as a safety net, but it should not trigger now.
        try:
            selected_scenes = selector.select_regular_time_series(
                items=items_for_selector,
                aoi_geom_4326=aoi_shp.buffer(0.05),
                period_start=start,
                period_end=end,
                n_anchors=params.n_anchors,
                window_days=params.window_days,
            )
        except ValueError as e:
            print(
                f"[WARN] TileSelector raised ValueError during selection despite filtering: {e}\n"
                f"       Regular time series will be empty, but timestamp coverage table "
                f"({ts_cov_csv.name}) is available for inspection."
            )
            selected_scenes = []

        # 5) Write scenes_selected (tile-level)
        selected_df = self.builder.selected_scenes_to_selected_tiles_df(
            selected_scenes,
            collection=params.collection,
        )
        selected_df.to_csv(selected_csv, index=False)

        # 6) Write time_serie (anchor-level)
        ts_df = self.builder.selected_scenes_to_time_serie_df(selected_scenes)
        ts_df.to_csv(ts_csv, index=False)

        print(f"[OK] scenes_selected exported => {selected_csv}")
        print(f"[OK] time_serie exported     => {ts_csv}")
        print(f"[OK] Anchors requested: {params.n_anchors}, anchors with selection: {len(ts_df)}")

        if not ts_df.empty:
            print(
                f"[OK] Coverage frac: min={ts_df['coverage_frac'].min():.3f} "
                f"median={ts_df['coverage_frac'].median():.3f} max={ts_df['coverage_frac'].max():.3f}"
            )
            print(
                f"[OK] Cloud score:   min={ts_df['cloud_score'].min():.2f} "
                f"median={ts_df['cloud_score'].median():.2f} max={ts_df['cloud_score'].max():.2f}"
            )

        return ts_csv