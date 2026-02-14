from __future__ import annotations

import sys
from pathlib import Path

from thess_geo_analytics.pipelines.BuildSceneCatalogPipeline import (
    BuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)


def main() -> None:
    # Usage:
    # python -m thess_geo_analytics.entrypoints.build_scene_catalog \
    #   aoi/EL522_Thessaloniki.geojson 90 20 300 sentinel-2-l2a 1 0.999 1
    #
    # Args:
    #   1) aoi_geojson (required)
    #   2) days (default 90)
    #   3) cloud_max (default 20.0)
    #   4) max_items (default 300)
    #   5) collection (default DEFAULT_COLLECTION)
    #   6) use_tile_selector (0/1, default 1)
    #   7) full_cover_threshold (default 0.999)
    #   8) allow_pair (0/1, default 1)

    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python -m thess_geo_analytics.entrypoints.build_scene_catalog "
            "<aoi_geojson> [days] [cloud_max] [max_items] [collection] "
            "[use_tile_selector 0|1] [full_cover_threshold] [allow_pair 0|1]"
        )

    aoi_path = Path(sys.argv[1])

    days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
    cloud_max = float(sys.argv[3]) if len(sys.argv) > 3 else 20.0
    max_items = int(sys.argv[4]) if len(sys.argv) > 4 else 300

    collection = str(sys.argv[5]) if len(sys.argv) > 5 else None

    use_tile_selector = bool(int(sys.argv[6])) if len(sys.argv) > 6 else True
    full_cover_threshold = float(sys.argv[7]) if len(sys.argv) > 7 else 0.999
    allow_pair = bool(int(sys.argv[8])) if len(sys.argv) > 8 else True

    pipeline = BuildSceneCatalogPipeline(aoi_path=aoi_path)

    params = BuildSceneCatalogParams(
        days=days,
        cloud_cover_max=cloud_max,
        max_items=max_items,
        collection=collection or BuildSceneCatalogParams().collection,
        use_tile_selector=use_tile_selector,
        full_cover_threshold=full_cover_threshold,
        allow_pair=allow_pair,
    )

    pipeline.run(params)


if __name__ == "__main__":
    main()