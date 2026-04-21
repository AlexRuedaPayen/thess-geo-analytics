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
class SyntheticSceneVvVhConfig(SyntheticSceneBaseConfig):
    revisit_days: int = 6
    revisit_jitter_days: int = 0

    tiles_per_timestamp_min: int = 1
    tiles_per_timestamp_max: int = 3
    full_cover_probability: float = 0.95

    orbit_states: tuple[str, ...] = ("ascending", "descending")
    polarizations: tuple[str, ...] = ("VV", "VH")
    instrument_mode: str = "IW"

    @staticmethod
    def from_yaml(path: str | Path) -> "SyntheticSceneVvVhConfig":
        p = Path(path)
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}

        model = raw.get("model", {})
        sar = raw.get("sar", {})
        coverage = raw.get("coverage", {})

        start_date = date.fromisoformat(model["start_date"])
        end_raw = model.get("end_date")
        end_date = date.today() if end_raw in (None, "", "null") else date.fromisoformat(end_raw)

        return SyntheticSceneVvVhConfig(
            start_date=start_date,
            end_date=end_date,
            random_seed=int(raw.get("random_seed", 1234)),
            revisit_days=int(model.get("revisit_days", 6)),
            revisit_jitter_days=int(model.get("revisit_jitter_days", 0)),
            tiles_per_timestamp_min=int(coverage.get("footprints_per_timestamp_min", 1)),
            tiles_per_timestamp_max=int(coverage.get("footprints_per_timestamp_max", 3)),
            full_cover_probability=float(coverage.get("full_cover_probability", 0.95)),
            orbit_states=tuple(sar.get("orbit_states", ["ascending", "descending"])),
            polarizations=tuple(sar.get("polarizations", ["VV", "VH"])),
            instrument_mode=str(sar.get("instrument_mode", "IW")),
        )


class SyntheticSceneVvVh(SyntheticSceneBase):
    def __init__(self, *, root: str | Path, config: SyntheticSceneVvVhConfig) -> None:
        super().__init__(root=root, config=config)
        self.cfg = config
        self._fixed_orbit_state = self.rng.choice(self.cfg.orbit_states)
        self._fixed_relative_orbit = self.rng.randint(1, 175)

    def collection_name(self) -> str:
        return "sentinel-1-grd"

    def item_id_prefix(self) -> str:
        return "S1_SYN"

    def platform_name(self) -> str:
        return self.rng.choice(["sentinel-1a", "sentinel-1b"])

    def constellation_name(self) -> str:
        return "sentinel-1"

    def generate_acquisition_dates(self) -> list[date]:
        dates: list[date] = []
        cur = self.cfg.start_date

        while cur <= self.cfg.end_date:
            jitter = 0
            if self.cfg.revisit_jitter_days > 0:
                jitter = self.rng.randint(-self.cfg.revisit_jitter_days, self.cfg.revisit_jitter_days)

            d = cur + timedelta(days=jitter)
            if self.cfg.start_date <= d <= self.cfg.end_date:
                dates.append(d)

            cur += timedelta(days=self.cfg.revisit_days)

        dates = sorted(set(dates))
        return dates

    def timestamps_for_date(self, d: date) -> list[datetime]:
        # One acquisition timestamp per pass, SAR-style
        hour = self.rng.choice([5, 6, 17, 18])
        minute = self.rng.randint(0, 59)
        second = self.rng.randint(0, 59)
        return [datetime.combine(d, time(hour, minute, second), tzinfo=UTC)]

    def footprints_for_date(self, d: date, tiles: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        major = tiles[tiles["overlap_class"] == "major"]
        minor = tiles[tiles["overlap_class"] == "minor"]

        use_full_cover = self.rng.random() < self.cfg.full_cover_probability

        if use_full_cover and len(major) > 0:
            # SAR often has broader cover; emulate with all major or a compact subset
            if len(major) <= self.cfg.tiles_per_timestamp_max:
                return major.copy()

            return self.random_sample_rows(
                major,
                n_min=min(self.cfg.tiles_per_timestamp_min, len(major)),
                n_max=min(self.cfg.tiles_per_timestamp_max, len(major)),
            )

        # partial or reduced cover
        all_tiles = gpd.GeoDataFrame(
            list(major.to_dict("records")) + list(minor.to_dict("records")),
            crs=tiles.crs,
        ) if len(minor) > 0 else tiles

        return self.random_sample_rows(
            all_tiles,
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
        return {
            "sar:polarizations": list(self.cfg.polarizations),
            "sar:instrument_mode": self.cfg.instrument_mode,
            "sat:orbit_state": self._fixed_orbit_state,
            "sat:relative_orbit": self._fixed_relative_orbit,
            "instruments": ["sar"],
            "synthetic:tile_id": footprint_row.get("tile_id"),
            "synthetic:overlap_class": footprint_row.get("overlap_class"),
            "synthetic:grid_i": footprint_row.get("grid_i"),
            "synthetic:grid_j": footprint_row.get("grid_j"),
        }