from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from shapely.geometry import box, mapping, shape
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
        Max number of tiles per timestamp. We will generate up to 3 patterns,
        so values > 3 behave like 3.
    base_cloud:
        Cloud cover for the first timestamp / first tile.
    cloud_step:
        Increment in cloud cover between timestamps.
    preview_geojson:
        Whether to write a combined GeoJSON with AOI + all tiles.
    preview_csv:
        Whether to write a CSV with per-tile coverage stats.
    """
    output_dir: Path
    start_datetime: str = "2021-01-05T10:00:00Z"
    n_timestamps: int = 3
    tiles_per_timestamp: int = 3
    base_cloud: float = 10.0
    cloud_step: float = 10.0
    preview_geojson: bool = True
    preview_csv: bool = True


class SceneCatalogTestDataGenerator:
    """
    Generates a complex AOI + a small STAC-like catalog for tests.

    Geometry design
    ---------------
    AOI: an "L-shaped" polygon, union of:
      - main body: rectangular box
      - a 'north-east arm': small extension to test partial tiles

    Tiles per timestamp:
      - central tile: covers most of the main AOI
      - west tile: overlaps AOI partially and extends to the west
      - north tile: overlaps the 'arm' more than the main body

    Artifacts (persisted on disk for manual inspection):
      - <output_dir>/aoi_scene_catalog.geojson
      - <output_dir>/scene_catalog_items.json
      - <output_dir>/scene_catalog_preview.geojson  (if preview_geojson=True)
      - <output_dir>/scene_catalog_coverage_preview.csv  (if preview_csv=True)

    Returned from run():
      {
        "aoi_geom": shapely geometry,
        "items": list[dict],
        "aoi_path": Path,
        "items_path": Path,
        "preview_geojson_path": Optional[Path],
        "preview_csv_path": Optional[Path],
      }
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
        Build a slightly more complex AOI:

        - main body:   box(23.0, 40.0, 23.12, 40.12)
        - NE 'arm':    box(23.08, 40.12, 23.18, 40.20)

        AOI = union(main_body, arm)
        """
        main_body = box(23.0, 40.0, 23.12, 40.12)
        arm = box(23.08, 40.12, 23.18, 40.20)

        aoi_geom = unary_union([main_body, arm])

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
        Define a few tile patterns for a given timestamp index.

        Returns a list of (tile_type, geometry) pairs.
        """
        tiles: List[Tuple[str, Any]] = []

        # 1) Central tile: mostly covers the main body
        central = box(23.0, 40.0, 23.12, 40.12)
        tiles.append(("central", central))

        # 2) West tile: overlaps main AOI partially, extends west
        west = box(22.96, 40.0, 23.06, 40.12)
        tiles.append(("west", west))

        # 3) North tile: overlaps both main + arm more toward the north-east
        north = box(23.06, 40.10, 23.20, 40.22)
        tiles.append(("north", north))

        # Respect tiles_per_timestamp (up to 3)
        max_tiles = max(1, min(self.config.tiles_per_timestamp, 3))
        return tiles[:max_tiles]

    def _write_items(self, aoi_geom) -> Tuple[List[Dict[str, Any]], Path, List[Dict[str, Any]]]:
        """
        Create a small set of STAC-like items with intersecting tiles.

        For each timestamp i:
          - dt = start_datetime + i * 5 days
          - generate up to tiles_per_timestamp tiles with patterns (central, west, north)

        For each tile we compute:
          - intersection area with AOI
          - intersection_frac = inter_area / aoi_area
        """
        items: List[Dict[str, Any]] = []
        coverage_rows: List[Dict[str, Any]] = []

        start_dt = self._parse_start_datetime()
        aoi_area = float(aoi_geom.area) if aoi_geom.area else 0.0

        for i in range(self.config.n_timestamps):
            dt = start_dt + i * timedelta(days=5)

            tile_geoms = self._tile_geometries_for_timestamp(i)

            for j, (tile_type, tile_geom) in enumerate(tile_geoms):
                geom_json = mapping(tile_geom)

                # Cloud: base + per-timestamp increment + small per-tile bump
                cloud = self.config.base_cloud + i * self.config.cloud_step + j * 3.0

                inter = tile_geom.intersection(aoi_geom)
                inter_area = float(inter.area)
                inter_frac = (inter_area / aoi_area) if aoi_area > 0 else 0.0

                item_id = f"FAKE_TILE_{i:02d}_{tile_type.upper()}"

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
                        # extra test/QA metadata
                        "test:tile_type": tile_type,
                        "test:intersection_frac": inter_frac,
                    },
                }
                items.append(item)

                coverage_rows.append(
                    {
                        "id": item_id,
                        "datetime": dt.isoformat().replace("+00:00", "Z"),
                        "tile_type": tile_type,
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
        """
        Combined GeoJSON with:
          - AOI as one feature
          - each tile as its own feature (with id, tile_type, cloud)
        Great for opening in QGIS or geojson.io.
        """
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
            props = it.get("properties", {})
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "id": it["id"],
                        "layer": "tile",
                        "tile_type": props.get("test:tile_type"),
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
        """
        Small CSV summarising coverage per tile; easy to open in any viewer.
        """
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
        n_timestamps=3,
        tiles_per_timestamp=3,
    )
    gen = SceneCatalogTestDataGenerator(cfg)
    gen.run()