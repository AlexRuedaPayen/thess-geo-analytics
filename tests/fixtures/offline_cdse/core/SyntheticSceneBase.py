from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import geopandas as gpd
import yaml
from shapely.geometry import mapping


UTC = timezone.utc


@dataclass(frozen=True)
class SyntheticSceneBaseConfig:
    start_date: date
    end_date: date
    random_seed: int = 1234

    @staticmethod
    def from_yaml(path: str | Path) -> "SyntheticSceneBaseConfig":
        p = Path(path)
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}

        model = raw.get("model", {})
        start_date = date.fromisoformat(model["start_date"])

        end_raw = model.get("end_date")
        end_date = date.today() if end_raw in (None, "", "null") else date.fromisoformat(end_raw)

        return SyntheticSceneBaseConfig(
            start_date=start_date,
            end_date=end_date,
            random_seed=int(raw.get("random_seed", 1234)),
        )


class SyntheticSceneBase:
    """
    Shared synthetic scene generator.

    Responsibilities:
      - load AOI and synthetic footprints
      - generate acquisition dates/timestamps
      - build STAC-like items from chosen footprints
      - provide extension hooks for modality-specific metadata

    Subclasses should override:
      - generate_acquisition_dates()
      - footprints_for_date()
      - item_properties_for_footprint()
      - collection_name()
      - item_id_prefix()
      - platform_name()
      - constellation_name()
    """

    def __init__(
        self,
        *,
        root: str | Path,
        config: SyntheticSceneBaseConfig,
    ) -> None:
        self.root = Path(root)
        self.config = config

        import random
        self.rng = random.Random(config.random_seed)


    @property
    def aoi_path(self) -> Path:
        return self.root / "aoi.geojson"

    @property
    def tiles_path(self) -> Path:
        return self.root / "tiles.geojson"

    def load_aoi_geojson(self) -> dict[str, Any]:
        return json.loads(self.aoi_path.read_text(encoding="utf-8"))

    def load_tiles_gdf(self) -> gpd.GeoDataFrame:
        gdf = gpd.read_file(self.tiles_path)
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        else:
            gdf = gdf.to_crs("EPSG:4326")
        return gdf
    def collection_name(self) -> str:
        raise NotImplementedError

    def item_id_prefix(self) -> str:
        raise NotImplementedError

    def platform_name(self) -> str:
        raise NotImplementedError

    def constellation_name(self) -> str:
        raise NotImplementedError

    def generate_acquisition_dates(self) -> list[date]:
        """
        Return one acquisition date per synthetic pass.
        Override in subclasses with modality-specific timing rules.
        """
        raise NotImplementedError

    def timestamps_for_date(self, d: date) -> list[datetime]:
        """
        Default: one midday UTC timestamp per acquisition date.
        Subclasses may override if they need several timestamps.
        """
        return [datetime.combine(d, time(10, 0, 0), tzinfo=UTC)]

    def footprints_for_date(self, d: date, tiles: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Return the subset of footprints used for this acquisition date.
        Override in subclasses.
        """
        raise NotImplementedError

    def item_properties_for_footprint(
        self,
        *,
        d: date,
        acq_dt: datetime,
        footprint_row: dict[str, Any],
        tile_index: int,
    ) -> dict[str, Any]:
        """
        Return modality-specific STAC properties for one footprint.
        """
        raise NotImplementedError

    def random_sample_rows(
        self,
        tiles: gpd.GeoDataFrame,
        *,
        n_min: int,
        n_max: int,
    ) -> gpd.GeoDataFrame:
        if len(tiles) == 0:
            return tiles.iloc[0:0].copy()

        n_min = max(1, int(n_min))
        n_max = max(n_min, int(n_max))
        n = self.rng.randint(n_min, min(n_max, len(tiles)))

        idx = list(tiles.index)
        chosen = self.rng.sample(idx, n)
        return tiles.loc[chosen].copy()

    def make_item_id(
        self,
        *,
        acq_dt: datetime,
        tile_id: str,
        suffix: str = "",
    ) -> str:
        stamp = acq_dt.strftime("%Y%m%dT%H%M%SZ")
        base = f"{self.item_id_prefix()}_{stamp}_{tile_id}"
        return f"{base}_{suffix}" if suffix else base

    def base_properties(self, acq_dt: datetime) -> dict[str, Any]:
        return {
            "datetime": acq_dt.isoformat().replace("+00:00", "Z"),
            "platform": self.platform_name(),
            "constellation": self.constellation_name(),
        }

    def build_item_from_row(
        self,
        *,
        d: date,
        acq_dt: datetime,
        row: dict[str, Any],
        tile_index: int,
    ) -> dict[str, Any]:
        tile_id = str(row.get("tile_id", f"TILE_{tile_index:03d}"))
        props = self.base_properties(acq_dt)
        props.update(
            self.item_properties_for_footprint(
                d=d,
                acq_dt=acq_dt,
                footprint_row=row,
                tile_index=tile_index,
            )
        )

        return {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": self.make_item_id(acq_dt=acq_dt, tile_id=tile_id),
            "collection": self.collection_name(),
            "geometry": mapping(row["geometry"]),
            "bbox": list(row["geometry"].bounds),
            "properties": props,
            "assets": {},
        }


    def generate_items(self) -> list[dict[str, Any]]:
        tiles = self.load_tiles_gdf()
        items: list[dict[str, Any]] = []

        for d in self.generate_acquisition_dates():
            date_tiles = self.footprints_for_date(d, tiles)
            if date_tiles.empty:
                continue

            for acq_dt in self.timestamps_for_date(d):
                for i, (_, row) in enumerate(date_tiles.iterrows()):
                    items.append(
                        self.build_item_from_row(
                            d=d,
                            acq_dt=acq_dt,
                            row=row.to_dict(),
                            tile_index=i,
                        )
                    )

        items.sort(key=lambda it: (it["properties"]["datetime"], it["id"]))
        return items

    def generate_feature_collection(self) -> dict[str, Any]:
        return {
            "type": "FeatureCollection",
            "features": self.generate_items(),
        }

    def write_feature_collection(self, out_path: str | Path) -> Path:
        out = Path(out_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        fc = self.generate_feature_collection()
        out.write_text(json.dumps(fc, indent=2), encoding="utf-8")
        return out


    def iter_days(self) -> Iterable[date]:
        cur = self.config.start_date
        while cur <= self.config.end_date:
            yield cur
            cur += timedelta(days=1)