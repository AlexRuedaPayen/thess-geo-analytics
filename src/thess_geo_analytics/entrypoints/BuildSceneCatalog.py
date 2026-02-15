from __future__ import annotations

import sys
from pathlib import Path

from thess_geo_analytics.pipelines.BuildSceneCatalogPipeline import (
    BuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)


def main() -> None:
    # Usage (new):
    # python -m thess_geo_analytics.entrypoints.build_scene_catalog \
    #   aoi/EL522_Thessaloniki.geojson 90 20 300 sentinel-2-l2a 1 sliding_window 0.999 1 15 15
    #
    # Args:
    #   1) aoi_geojson (required)
    #   2) days (default 90)
    #   3) cloud_max (default 20.0)
    #   4) max_items (default 300)
    #   5) collection (default DEFAULT_COLLECTION)
    #
    #   6) use_tile_selector (0/1, default 1)
    #   7) selection_mode ("per_date" | "sliding_window", default "sliding_window")
    #   8) full_cover_threshold (default 0.999)
    #   9) allow_pair (0/1, default 1)
    #
    #  10) window_days (default 15)           [only used if selection_mode == "sliding_window"]
    #  11) step_days (default 15)             [only used if selection_mode == "sliding_window"]

    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python -m thess_geo_analytics.entrypoints.build_scene_catalog "
            "<aoi_geojson> [days] [cloud_max] [max_items] [collection] "
            "[use_tile_selector 0|1] [selection_mode per_date|sliding_window] "
            "[full_cover_threshold] [allow_pair 0|1] [window_days] [step_days]"
        )

    aoi_path = Path(sys.argv[1])

    days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
    cloud_max = float(sys.argv[3]) if len(sys.argv) > 3 else 20.0
    max_items = int(sys.argv[4]) if len(sys.argv) > 4 else 300
    collection = str(sys.argv[5]) if len(sys.argv) > 5 else None

    use_tile_selector = bool(int(sys.argv[6])) if len(sys.argv) > 6 else True
    selection_mode = str(sys.argv[7]) if len(sys.argv) > 7 else "sliding_window"
    full_cover_threshold = float(sys.argv[8]) if len(sys.argv) > 8 else 0.999
    allow_pair = bool(int(sys.argv[9])) if len(sys.argv) > 9 else True
    window_days = int(sys.argv[10]) if len(sys.argv) > 10 else 15
    step_days = int(sys.argv[11]) if len(sys.argv) > 11 else 15

    pipeline = BuildSceneCatalogPipeline(aoi_path=aoi_path)

    params = BuildSceneCatalogParams(
        days=days,
        cloud_cover_max=cloud_max,
        max_items=max_items,
        collection=collection or BuildSceneCatalogParams().collection,
        use_tile_selector=use_tile_selector,
        selection_mode=selection_mode,  # type: ignore[arg-type]
        full_cover_threshold=full_cover_threshold,
        allow_pair=allow_pair,
        window_days=window_days,
        step_days=step_days,
    )

    pipeline.run(params)


if __name__ == "__main__":
    main()
