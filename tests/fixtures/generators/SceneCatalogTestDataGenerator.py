from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from shapely.geometry import box, mapping, shape

LOG = logging.getLogger(__name__)


@dataclass
class SceneCatalogTestDataConfig:
    """
    Configuration for scene catalog test data generator.

    Attributes
    ----------
    output_dir:
        Directory where AOI + fake items will be written.
    start_datetime:
        First acquisition datetime (UTC, ISO8601).
    n_timestamps:
        Number of distinct acquisition timestamps.
    tiles_per_timestamp:
        Number of tiles per timestamp (we'll slightly shift them).
    base_cloud:
        Cloud cover for the first tile; others get increments.
    cloud_step:
        Increment in cloud cover between tiles.
    """
    output_dir: Path
    start_datetime: str = "2021-01-05T10:00:00Z"
    n_timestamps: int = 3
    tiles_per_timestamp: int = 1
    base_cloud: float = 10.0
    cloud_step: float = 10.0


class SceneCatalogTestDataGenerator:
    """
    Generates a simple AOI + a small STAC-like catalog for tests.

    Artifacts (persisted on disk so user can inspect them):
      - <output_dir>/aoi_scene_catalog.geojson
      - <output_dir>/scene_catalog_items.json

    Additionally returns in-memory objects so tests can use them directly.
    """

    def __init__(self, config: SceneCatalogTestDataConfig) -> None:
        self.config = config

    def run(self) -> Dict[str, Any]:
        """
        Generate AOI + fake items, persist them, and return:

        {
          "aoi_geom": shapely geometry,
          "items": list[dict],
          "aoi_path": Path,
          "items_path": Path,
        }
        """
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        aoi_geom, aoi_path = self._write_aoi()
        items, items_path = self._write_items(aoi_geom)

        LOG.info(
            "SceneCatalogTestDataGenerator: wrote AOI=%s, items=%s",
            aoi_path,
            items_path,
        )

        return {
            "aoi_geom": aoi_geom,
            "items": items,
            "aoi_path": aoi_path,
            "items_path": items_path,
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

    def _write_aoi(self):
        """
        AOI: simple box (23.0, 40.0, 23.1, 40.1) in EPSG:4326.
        """
        aoi_geom = box(23.0, 40.0, 23.1, 40.1)
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

    def _write_items(self, aoi_geom) -> tuple[list[dict], Path]:
        """
        Create a small set of STAC-like items:

          - n_timestamps timestamps, spaced by 5 days
          - tiles_per_timestamp tiles, slightly shifted in lon
          - geometry = AOI-ish box
        """
        items: List[Dict[str, Any]] = []
        start_dt = self._parse_start_datetime()

        for i in range(self.config.n_timestamps):
            dt = start_dt + i * timedelta(days=5)

            for j in range(self.config.tiles_per_timestamp):
                dx = 0.01 * j
                geom = box(
                    23.0 + dx,
                    40.0,
                    23.1 + dx,
                    40.1,
                )
                geom_json = mapping(geom)

                cloud = self.config.base_cloud + (i * self.config.cloud_step) + (j * 2.0)

                item = {
                    "id": f"FAKE_TILE_{i:02d}_{j:02d}",
                    "type": "Feature",
                    "geometry": geom_json,
                    "properties": {
                        "datetime": dt.isoformat().replace("+00:00", "Z"),
                        "cloud_cover": cloud,
                        "platform": "sentinel-2",
                        "constellation": "sentinel-2",
                        "proj:geometry": geom_json,
                    },
                }
                items.append(item)

        items_path = self.config.output_dir / "scene_catalog_items.json"
        with items_path.open("w", encoding="utf-8") as f:
            json.dump(items, f, indent=2)

        return items, items_path


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    cfg = SceneCatalogTestDataConfig(output_dir=Path("tests/fixtures/generated/scene_catalog"))
    gen = SceneCatalogTestDataGenerator(cfg)
    gen.run()