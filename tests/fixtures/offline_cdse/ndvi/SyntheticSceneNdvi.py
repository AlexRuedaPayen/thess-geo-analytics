from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
import yaml

from tests.fixtures.offline_cdse.core.SyntheticSceneBase import (
    SyntheticSceneBase,
    SyntheticSceneBaseConfig,
)

UTC = timezone.utc


@dataclass(frozen=True)
class SyntheticSceneNdviConfig(SyntheticSceneBaseConfig):
    passes_per_day_mean: float = 1.5
    passes_per_day_std: float = 0.5
    timestamps_per_pass: tuple[int, ...] = (1, 2)

    tiles_per_timestamp_min: int = 2
    tiles_per_timestamp_max: int = 11

    cloud_alpha: float = 2.0
    cloud_beta: float = 8.0

    full_cover_probability: float = 0.85

    @staticmethod
    def from_yaml(path: str | Path) -> "SyntheticSceneNdviConfig":
        p = Path(path)
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}

        model = raw.get("model", {})
        tiles = raw.get("tiles", {})
        cloud = tiles.get("cloud_cover_distribution", {})

        start_date = date.fromisoformat(model["start_date"])
        end_raw = model.get("end_date")
        end_date = date.today() if end_raw in (None, "", "null") else date.fromisoformat(end_raw)

        return SyntheticSceneNdviConfig(
            start_date=start_date,
            end_date=end_date,
            random_seed=int(raw.get("random_seed", 1234)),
            passes_per_day_mean=float(model.get("passes_per_day_mean", 1.5)),
            passes_per_day_std=float(model.get("passes_per_day_std", 0.5)),
            timestamps_per_pass=tuple(model.get("timestamps_per_pass", [1, 2])),
            tiles_per_timestamp_min=int(tiles.get("tiles_per_timestamp_min", 2)),
            tiles_per_timestamp_max=int(tiles.get("tiles_per_timestamp_max", 11)),
            cloud_alpha=float(cloud.get("alpha", 2.0)),
            cloud_beta=float(cloud.get("beta", 8.0)),
            full_cover_probability=float(tiles.get("full_cover_probability", 0.85)),
        )


class SyntheticSceneNdvi(SyntheticSceneBase):
    def __init__(self, *, root: str | Path, config: SyntheticSceneNdviConfig) -> None:
        super().__init__(root=root, config=config)
        self.cfg = config

    def collection_name(self) -> str:
        return "sentinel-2-l2a"

    def item_id_prefix(self) -> str:
        return "S2_SYN"

    def platform_name(self) -> str:
        return self.rng.choice(["sentinel-2a", "sentinel-2b"])

    def constellation_name(self) -> str:
        return "sentinel-2"

    def generate_acquisition_dates(self) -> list[date]:
        dates: list[date] = []
        for d in self.iter_days():
            # approximate Poisson-like daily count using rounded gaussian, clipped at 0
            n = round(self.rng.gauss(self.cfg.passes_per_day_mean, self.cfg.passes_per_day_std))
            n = max(0, n)
            for _ in range(n):
                dates.append(d)
        dates.sort()
        return dates

    def timestamps_for_date(self, d: date) -> list[datetime]:
        n = self.rng.choice(self.cfg.timestamps_per_pass)
        base_hour = self.rng.choice([9, 10, 11])

        out: list[datetime] = []
        for i in range(n):
            minute = self.rng.randint(0, 59)
            second = self.rng.randint(0, 59)
            hour = min(base_hour + i, 23)
            out.append(datetime.combine(d, time(hour, minute, second), tzinfo=UTC))
        return out

    def footprints_for_date(self, d: date, tiles: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # Often keep only "major" overlapping tiles for full cover,
        # sometimes allow a random subset causing partial coverage
        major = tiles[tiles["overlap_class"] == "major"]
        minor = tiles[tiles["overlap_class"] == "minor"]

        if len(major) == 0:
            return self.random_sample_rows(
                tiles,
                n_min=self.cfg.tiles_per_timestamp_min,
                n_max=self.cfg.tiles_per_timestamp_max,
            )

        use_full_cover = self.rng.random() < self.cfg.full_cover_probability

        if use_full_cover:
            # full-cover-like behavior: include all major tiles + maybe some minors
            chosen = major.copy()
            if len(minor) > 0 and self.rng.random() < 0.5:
                extra = self.random_sample_rows(minor, n_min=1, n_max=min(3, len(minor)))
                chosen = gpd.GeoDataFrame(
                    list(chosen.to_dict("records")) + list(extra.to_dict("records")),
                    crs=tiles.crs,
                )
            return chosen

        # partial cover: random subset of all tiles
        return self.random_sample_rows(
            tiles,
            n_min=self.cfg.tiles_per_timestamp_min,
            n_max=self.cfg.tiles_per_timestamp_max,
        )

    def item_properties_for_footprint(
        self,
        *,
        d: date,
        acq_dt: datetime,
        footprint_row: dict[str, Any],
        tile_index: int,
    ) -> dict[str, Any]:
        cloud_cover = 100.0 * self.rng.betavariate(self.cfg.cloud_alpha, self.cfg.cloud_beta)

        return {
            "eo:cloud_cover": round(cloud_cover, 2),
            "instruments": ["msi"],
            "sat:orbit_state": self.rng.choice(["ascending", "descending"]),
            "synthetic:tile_id": footprint_row.get("tile_id"),
            "synthetic:overlap_class": footprint_row.get("overlap_class"),
            "synthetic:grid_i": footprint_row.get("grid_i"),
            "synthetic:grid_j": footprint_row.get("grid_j"),
        }