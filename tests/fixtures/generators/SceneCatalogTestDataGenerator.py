from __future__ import annotations

import json
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from shapely.geometry import box, mapping
from shapely.ops import unary_union

LOG = logging.getLogger(__name__)


@dataclass
class SceneCatalogTestDataConfig:
    """
    Configuration for scene catalog test data generator.

    Attributes
    ----------
    output_dir:
        Directory where AOI + fake items + previews will be written.
    start_datetime:
        First acquisition datetime (UTC, ISO8601).
    n_timestamps:
        Number of distinct acquisition timestamps.
    tiles_per_timestamp:
        Max number of tiles per timestamp (up to 15 supported).
    cloud_min:
        Minimum cloud cover (%) for random generation.
    cloud_max:
        Maximum cloud cover (%) for random generation.
    rng_seed:
        Seed for the random number generator (for CI reproducibility).
    preview_geojson:
        Whether to write a combined GeoJSON with AOI + all tiles.
    preview_csv:
        Whether to write a CSV with per-tile coverage stats.
    """
    output_dir: Path
    start_datetime: str = "2021-01-05T10:00:00Z"
    n_timestamps: int = 24
    tiles_per_timestamp: int = 15
    cloud_min: float = 0.0
    cloud_max: float = 80.0
    rng_seed: int = 42
    preview_geojson: bool = True
    preview_csv: bool = True


class SceneCatalogTestDataGenerator:
    """
    Generates a complex AOI + a STAC-like scene catalog for tests.

    Geometry design
    ---------------
    AOI: slightly non-convex L-ish polygon.

    Tiles per timestamp:
      - we create a small grid of overlapping tiles around the AOI.
      - tiles_per_timestamp controls how many of those patterns we keep.

    Cloud cover:
      - random in [cloud_min, cloud_max], but deterministic via rng_seed.

    Artifacts:
      - <output_dir>/aoi_scene_catalog.geojson
      - <output_dir>/scene_catalog_items.json
      - <output_dir>/scene_catalog_preview.geojson  (if preview_geojson)
      - <output_dir>/scene_catalog_coverage_preview.csv  (if preview_csv)
    """

    def __init__(self, config: SceneCatalogTestDataConfig) -> None:
        self.config = config

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def run(self) -> Dict[str, Any]:
        """
        Generate AOI + fake items, persist them, and return both disk paths
        and in-memory objects.
        """
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        aoi_geom, aoi_path = self._write_complex_aoi()
        items, items_path, coverage_rows = self._write_items(aoi_geom)

        preview_geojson_path = None
        preview_csv_path = None

        if self.config.preview_geojson:
            preview_geojson_path = self._write_preview_geojson(aoi_geom, items)
        if self.config.preview_csv:
            preview_csv_path = self._write_coverage_preview_csv(coverage_rows)

        LOG.info(
            "SceneCatalogTestDataGenerator: wrote AOI=%s, items=%s, preview_geojson=%s, preview_csv=%s",
            aoi_path,
            items_path,
            preview_geojson_path,
            preview_csv_path,
        )

        return {
            "aoi_geom": aoi_geom,
            "items": items,
            "aoi_path": aoi_path,
            "items_path": items_path,
            "preview_geojson_path": preview_geojson_path,
            "preview_csv_path": preview_csv_path,
        }

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _parse_start_datetime(self) -> datetime:
        s = self.config.start_datetime.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _write_complex_aoi(self) -> Tuple[Any, Path]:
        """
        Build a slightly more complex non-convex AOI:

          - main body      : box(23.0, 40.0, 23.12, 40.12)
          - NE 'arm'       : box(23.08, 40.12, 23.18, 40.20)
          - small notch SW : box(23.02, 39.96, 23.10, 40.02) subtracted

        AOI = union(main_body, arm) minus notch
        """
        main_body = box(23.0, 40.0, 23.12, 40.12)
        arm = box(23.08, 40.12, 23.18, 40.20)

        # Extra shape to create a non-convex outline
        extra = box(22.98, 40.06, 23.05, 40.16)

        raw_geom = unary_union([main_body, arm, extra])
        # Optional: carve a notch (simple difference)
        notch = box(23.02, 39.96, 23.10, 40.02)
        aoi_geom = raw_geom.difference(notch)

        aoi_fc = {
            "type": "FeatureCollection",
            "name": "aoi_scene_catalog",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"id": "AOI_SCENE_CATALOG"},
                    "geometry": mapping(aoi_geom),
                }
            ],
        }

        aoi_path = self.config.output_dir / "aoi_scene_catalog.geojson"
        with aoi_path.open("w", encoding="utf-8") as f:
            json.dump(aoi_fc, f, indent=2)

        return aoi_geom, aoi_path

    def _tile_geometries_for_timestamp(self, idx: int) -> List[Tuple[str, Any]]:
        """
        Define a grid of overlapping tiles around the AOI.

        We create up to ~15 tiles with small shifts, then truncate
        to tiles_per_timestamp.
        """
        tiles: List[Tuple[str, Any]] = []

        # Base bounding box around Thessaloniki-ish region
        # (same general area as AOI, but slightly larger).
        base_minx, base_miny = 22.9, 39.95
        base_maxx, base_maxy = 23.25, 40.25

        # Simple 3x5 grid of tiles with overlap
        nx = 3
        ny = 5
        dx = (base_maxx - base_minx) / (nx + 1)
        dy = (base_maxy - base_miny) / (ny + 1)

        for iy in range(ny):
            for ix in range(nx):
                # Some deterministic “jitter” per timestamp so geometry
                # slightly shifts but remains reproducible.
                shift_x = (idx % 3) * dx * 0.15
                shift_y = (idx % 5) * dy * 0.15

                minx = base_minx + ix * dx + shift_x
                maxx = minx + dx * 1.6  # overlapped tiles
                miny = base_miny + iy * dy + shift_y
                maxy = miny + dy * 1.6

                tile_id = f"tile_{iy:02d}_{ix:02d}"
                tiles.append((tile_id, box(minx, miny, maxx, maxy)))

        max_tiles = max(1, min(self.config.tiles_per_timestamp, len(tiles)))
        return tiles[:max_tiles]

    def _write_items(self, aoi_geom) -> Tuple[List[Dict[str, Any]], Path, List[Dict[str, Any]]]:
        """
        Create a set of STAC-like items with:

          - n_timestamps acquisition dates spaced every 5 days
          - up to tiles_per_timestamp tiles per timestamp
          - random cloud_cover in [cloud_min, cloud_max] (deterministic via rng_seed)
          - ONE guaranteed AOI-cover, low-cloud tile per timestamp so that
            coverage >= full_cover_threshold is always achievable in tests.
        """
        items: List[Dict[str, Any]] = []
        coverage_rows: List[Dict[str, Any]] = []

        start_dt = self._parse_start_datetime()
        aoi_area = float(aoi_geom.area) if aoi_geom.area else 0.0

        rng = random.Random(self.config.rng_seed)

        for i in range(self.config.n_timestamps):
            dt = start_dt + i * timedelta(days=5)

            # -------------------------------------------------
            # 1) Guaranteed AOI-cover, LOW-CLOUD tile
            # -------------------------------------------------
            cover_geom = aoi_geom.buffer(0.01).envelope
            cover_geom_json = mapping(cover_geom)

            # Ensure this tile ALWAYS passes cloud_cover_max=20 used in tests
            low_cloud_max = min(self.config.cloud_max, 19.0)
            low_cloud_min = self.config.cloud_min
            cover_cloud = rng.uniform(low_cloud_min, low_cloud_max)

            inter = cover_geom.intersection(aoi_geom)
            inter_area = float(inter.area)
            inter_frac = (inter_area / aoi_area) if aoi_area > 0 else 0.0

            cover_id = f"FAKE_TILE_{i:02d}_AOI_COVER"

            cover_item = {
                "id": cover_id,
                "type": "Feature",
                "geometry": cover_geom_json,
                "properties": {
                    "datetime": dt.isoformat().replace("+00:00", "Z"),
                    "cloud_cover": cover_cloud,
                    "platform": "sentinel-2",
                    "constellation": "sentinel-2",
                    "proj:geometry": cover_geom_json,
                    "test:tile_name": "aoi_cover",
                    "test:intersection_frac": inter_frac,
                },
            }
            items.append(cover_item)

            coverage_rows.append(
                {
                    "id": cover_id,
                    "datetime": dt.isoformat().replace("+00:00", "Z"),
                    "tile_name": "aoi_cover",
                    "cloud_cover": cover_cloud,
                    "intersection_area": inter_area,
                    "intersection_frac": inter_frac,
                }
            )

            # -------------------------------------------------
            # 2) Additional grid tiles (up to tiles_per_timestamp - 1)
            # -------------------------------------------------
            # We reuse the grid generator but truncate so total per timestamp
            # does not exceed tiles_per_timestamp.
            grid_tiles = self._tile_geometries_for_timestamp(i)

            # We already added 1 tile (AOI cover) above:
            remaining_slots = max(self.config.tiles_per_timestamp - 1, 0)
            grid_tiles = grid_tiles[:remaining_slots]

            for tile_name, tile_geom in grid_tiles:
                geom_json = mapping(tile_geom)

                cloud = rng.uniform(self.config.cloud_min, self.config.cloud_max)

                inter = tile_geom.intersection(aoi_geom)
                inter_area = float(inter.area)
                inter_frac = (inter_area / aoi_area) if aoi_area > 0 else 0.0

                item_id = f"FAKE_TILE_{i:02d}_{tile_name.upper()}"

                item = {
                    "id": item_id,
                    "type": "Feature",
                    "geometry": geom_json,
                    "properties": {
                        "datetime": dt.isoformat().replace("+00:00", "Z"),
                        "cloud_cover": cloud,
                        "platform": "sentinel-2",
                        "constellation": "sentinel-2",
                        "proj:geometry": geom_json,
                        "test:tile_name": tile_name,
                        "test:intersection_frac": inter_frac,
                    },
                }
                items.append(item)

                coverage_rows.append(
                    {
                        "id": item_id,
                        "datetime": dt.isoformat().replace("+00:00", "Z"),
                        "tile_name": tile_name,
                        "cloud_cover": cloud,
                        "intersection_area": inter_area,
                        "intersection_frac": inter_frac,
                    }
                )

        items_path = self.config.output_dir / "scene_catalog_items.json"
        with items_path.open("w", encoding="utf-8") as f:
            json.dump(items, f, indent=2)

        return items, items_path, coverage_rows

    def _write_preview_geojson(self, aoi_geom, items: List[Dict[str, Any]]) -> Path:
        features: List[Dict[str, Any]] = []

        # AOI feature
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "id": "AOI_SCENE_CATALOG",
                    "layer": "aoi",
                },
                "geometry": mapping(aoi_geom),
            }
        )

        # Tile features
        for it in items:
            props = it.get("properties", {}) or {}
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "id": it["id"],
                        "layer": "tile",
                        "tile_name": props.get("test:tile_name"),
                        "cloud_cover": props.get("cloud_cover"),
                        "intersection_frac": props.get("test:intersection_frac"),
                    },
                    "geometry": it["geometry"],
                }
            )

        fc = {
            "type": "FeatureCollection",
            "name": "scene_catalog_preview",
            "features": features,
        }

        out_path = self.config.output_dir / "scene_catalog_preview.geojson"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(fc, f, indent=2)

        return out_path

    def _write_coverage_preview_csv(self, coverage_rows: List[Dict[str, Any]]) -> Path:
        df = pd.DataFrame(coverage_rows)
        out_path = self.config.output_dir / "scene_catalog_coverage_preview.csv"
        df.to_csv(out_path, index=False)
        return out_path


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    cfg = SceneCatalogTestDataConfig(
        output_dir=Path("tests/fixtures/generated/scene_catalog"),
        n_timestamps=24,
        tiles_per_timestamp=15,
        cloud_min=0.0,
        cloud_max=80.0,
        rng_seed=42,
    )
    gen = SceneCatalogTestDataGenerator(cfg)
    gen.run()