from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from shapely.geometry import shape

from thess_geo_analytics.builders.BaseSceneCatalogBuilder import BaseSceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION
from thess_geo_analytics.geo.TileSelector import TileSelector
from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildSceneCatalogParams:
    date_start: str = "2021-01-01"
    cloud_cover_max: float = 20.0
    max_items: int = 5000
    collection: str = DEFAULT_COLLECTION
    use_tile_selector: bool = True
    full_cover_threshold: float = 0.999
    allow_union: bool = True
    max_union_tiles: int = 2
    n_anchors: int = 24
    window_days: int = 21


class BaseBuildSceneCatalogPipeline:
    def __init__(
        self,
        aoi_path: Path,
        builder: BaseSceneCatalogBuilder,
    ) -> None:
        self.aoi_path = aoi_path
        self.builder = builder

    @staticmethod
    def _ensure_parent(p: Path) -> Path:
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    @staticmethod
    def _parse_start_date(date_start: str) -> date:
        try:
            return date.fromisoformat(date_start)
        except Exception as e:
            raise ValueError(
                f"Invalid date_start={date_start!r} (expected YYYY-MM-DD)"
            ) from e

    def build_stac_params(self, params: BuildSceneCatalogParams) -> StacQueryParams:
        raise NotImplementedError

    def empty_raw_columns(self) -> list[str]:
        raise NotImplementedError

    def empty_selected_columns(self) -> list[str]:
        raise NotImplementedError

    def empty_time_series_columns(self) -> list[str]:
        raise NotImplementedError

    def empty_coverage_columns(self) -> list[str]:
        raise NotImplementedError

    def scene_catalog_paths(self):
        raise NotImplementedError

    def should_write_legacy_scene_catalog_outputs(self) -> bool:
        return False

    def selector(self, params: BuildSceneCatalogParams) -> TileSelector:
        return TileSelector(
            full_cover_threshold=params.full_cover_threshold,
            allow_union=params.allow_union,
            max_union_tiles=params.max_union_tiles,
        )

    def coverage_row_from_group(self, dt, cis, aoi_area_value: float, params: BuildSceneCatalogParams) -> dict[str, Any]:
        union_geom = cis[0].covered_geom
        for ci in cis[1:]:
            union_geom = union_geom.union(ci.covered_geom)

        coverage_frac = float(union_geom.area) / float(aoi_area_value)
        tiles_count = len(cis)
        has_full_cover = coverage_frac >= params.full_cover_threshold

        return {
            "acq_datetime": dt.isoformat(),
            "coverage_frac": coverage_frac,
            "tiles_count": tiles_count,
            "has_full_cover": has_full_cover,
        }

    def print_time_series_summary(self, ts_df: pd.DataFrame) -> None:
        if not ts_df.empty and "coverage_frac" in ts_df.columns:
            print(
                f"[INFO] Coverage frac: min={ts_df['coverage_frac'].min():.3f} "
                f"median={ts_df['coverage_frac'].median():.3f} max={ts_df['coverage_frac'].max():.3f}"
            )

    def legacy_scene_catalog_paths(self) -> dict[str, Path]:
        return {
            "scenes_catalog": self._ensure_parent(RepoPaths.table("scenes_catalog.csv")),
            "scenes_selected": self._ensure_parent(RepoPaths.table("scenes_selected.csv")),
            "time_series": self._ensure_parent(RepoPaths.table("time_serie.csv")),
            "timestamps_coverage": self._ensure_parent(RepoPaths.table("timestamps_coverage.csv")),
        }

    def _write_df(self, df: pd.DataFrame, new_path: Path, legacy_path: Path | None = None) -> None:
        df.to_csv(new_path, index=False)
        if legacy_path is not None and self.should_write_legacy_scene_catalog_outputs():
            df.to_csv(legacy_path, index=False)

    def run(self, params: BuildSceneCatalogParams) -> Path:
        start = self._parse_start_date(params.date_start)
        end = date.today()

        stac_params = self.build_stac_params(params)

        items, aoi_geom_geojson = self.builder.build_scene_items(
            aoi_path=self.aoi_path,
            date_start=start.isoformat(),
            date_end=end.isoformat(),
            params=stac_params,
        )

        paths = self.scene_catalog_paths()
        legacy = self.legacy_scene_catalog_paths() if self.should_write_legacy_scene_catalog_outputs() else None

        raw_csv = self._ensure_parent(paths.scenes_catalog)
        selected_csv = self._ensure_parent(paths.scenes_selected)
        ts_csv = self._ensure_parent(paths.time_series)
        ts_cov_csv = self._ensure_parent(paths.timestamps_coverage)

        raw_df = (
            self.builder.build_scene_catalog_df(items, collection=params.collection)
            if items
            else pd.DataFrame(columns=self.empty_raw_columns())
        )
        self._write_df(
            raw_df,
            raw_csv,
            legacy["scenes_catalog"] if legacy else None,
        )

        print(f"[OUTPUT] scenes_catalog exported => {raw_csv}")
        print(f"[INFO] Raw scenes found: {len(raw_df)}")

        if not items or raw_df.empty or not params.use_tile_selector:
            if params.use_tile_selector:
                selected_empty = pd.DataFrame(columns=self.empty_selected_columns())
                ts_empty = pd.DataFrame(columns=self.empty_time_series_columns())
                cov_empty = pd.DataFrame(columns=self.empty_coverage_columns())

                self._write_df(
                    selected_empty,
                    selected_csv,
                    legacy["scenes_selected"] if legacy else None,
                )
                self._write_df(
                    ts_empty,
                    ts_csv,
                    legacy["time_series"] if legacy else None,
                )
                self._write_df(
                    cov_empty,
                    ts_cov_csv,
                    legacy["timestamps_coverage"] if legacy else None,
                )

                print(f"[OUTPUT] scenes_selected exported => {selected_csv} (empty)")
                print(f"[OUTPUT] time_series exported    => {ts_csv} (empty)")
                print(f"[OUTPUT] timestamps_coverage exported => {ts_cov_csv} (empty)")

            return raw_csv

        aoi_shp = shape(aoi_geom_geojson)
        selector = self.selector(params)

        infos, _, _, aoi_area_value = selector._coverage_infos(items, aoi_shp.buffer(0.05))

        cov_rows = []
        if infos and aoi_area_value > 0:
            from collections import defaultdict

            by_ts = defaultdict(list)
            for ci in infos:
                by_ts[ci.acq_dt].append(ci)

            for dt, cis in by_ts.items():
                cov_rows.append(self.coverage_row_from_group(dt, cis, aoi_area_value, params))

        cov_df = (
            pd.DataFrame(cov_rows).sort_values("acq_datetime")
            if cov_rows
            else pd.DataFrame(columns=self.empty_coverage_columns())
        )

        self._write_df(
            cov_df,
            ts_cov_csv,
            legacy["timestamps_coverage"] if legacy else None,
        )
        print(f"[OUTPUT] timestamps_coverage exported => {ts_cov_csv}")

        n_full = int(cov_df["has_full_cover"].sum()) if not cov_df.empty else 0
        print(
            f"[INFO] Timestamps with full cover (>= {params.full_cover_threshold:.3f}): {n_full}"
        )

        if cov_df.empty:
            print("[WARN] No timestamps with any coverage; skipping TileSelector.")
            items_for_selector = []
        else:
            good_ts = set(
                pd.to_datetime(cov_df.loc[cov_df["has_full_cover"], "acq_datetime"])
                .dt.to_pydatetime()
            )
            print(
                f"[INFO] Timestamps kept for TileSelector (has_full_cover=True): {len(good_ts)}"
            )

            items_for_selector = []
            if good_ts:
                for it in items:
                    try:
                        dt = selector._get_datetime(it)
                    except Exception:
                        continue
                    if dt in good_ts:
                        items_for_selector.append(it)

        if not items_for_selector:
            print("[WARN] No items belong to timestamps with full coverage; outputs will be empty.")

            selected_empty = pd.DataFrame(columns=self.empty_selected_columns())
            ts_empty = pd.DataFrame(columns=self.empty_time_series_columns())

            self._write_df(
                selected_empty,
                selected_csv,
                legacy["scenes_selected"] if legacy else None,
            )
            self._write_df(
                ts_empty,
                ts_csv,
                legacy["time_series"] if legacy else None,
            )

            print(f"[OUTPUT] scenes_selected exported => {selected_csv} (empty)")
            print(f"[OUTPUT] time_series exported    => {ts_csv} (empty)")
            return ts_csv

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

        selected_df = self.builder.selected_scenes_to_selected_tiles_df(
            selected_scenes,
            collection=params.collection,
        )
        self._write_df(
            selected_df,
            selected_csv,
            legacy["scenes_selected"] if legacy else None,
        )

        ts_df = self.builder.selected_scenes_to_time_serie_df(selected_scenes)

        if ts_df.empty:
            ts_df = pd.DataFrame(columns=self.empty_time_series_columns())

        self._write_df(
            ts_df,
            ts_csv,
            legacy["time_series"] if legacy else None,
        )

        print(f"[OUTPUT] scenes_selected exported => {selected_csv}")
        print(f"[OUTPUT] time_series exported    => {ts_csv}")
        print(f"[INFO] Anchors requested: {params.n_anchors}, anchors with selection: {len(ts_df)}")

        self.print_time_series_summary(ts_df)

        return ts_csv