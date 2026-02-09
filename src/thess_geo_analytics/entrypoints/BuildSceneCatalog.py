from __future__ import annotations

import sys
from pathlib import Path

from thess_geo_analytics.pipelines.BuildSceneCatalogPipeline import (
    BuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths


def main() -> None:
    # Minimal CLI:
    # python -m thess_geo_analytics.entrypoints.build_scene_catalog aoi/EL522_Thessaloniki.geojson 90 20 300
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python -m thess_geo_analytics.entrypoints.BuildSceneCatalog <aoi_geojson> [days] [cloud_max] [max_items]"
        )

    aoi_path = Path(sys.argv[1])
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
    cloud_max = float(sys.argv[3]) if len(sys.argv) > 3 else 20.0
    max_items = int(sys.argv[4]) if len(sys.argv) > 4 else 300

    pipeline = BuildSceneCatalogPipeline(aoi_path=aoi_path)
    pipeline.run(BuildSceneCatalogParams(days=days, cloud_cover_max=cloud_max, max_items=max_items))


if __name__ == "__main__":
    main()
