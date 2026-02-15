from __future__ import annotations

import sys
from pathlib import Path

from thess_geo_analytics.pipelines.BuildSceneCatalogPipeline import (
    BuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)


def _as_bool01(x: str) -> bool:
    # accept "0/1", "true/false", "yes/no"
    x = x.strip().lower()
    if x in {"1", "true", "yes", "y"}:
        return True
    if x in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Expected boolean 0/1 or true/false, got: {x}")


def main() -> None:
    # Backward compatible usage (old):
    # python -m thess_geo_analytics.entrypoints.BuildSceneCatalog \
    #   aoi/EL522_Thessaloniki.geojson 90 20 300 sentinel-2-l2a 1 0.999 1
    #
    # New (extended) positional tail:
    #   ... [n_anchors] [window_days] [allow_union 0|1] [max_union_tiles]
    #
    # Args:
    #   1) aoi_geojson (required)
    #   2) days (default 365)
    #   3) cloud_max (default 20.0)
    #   4) max_items (default 5000)
    #   5) collection (default DEFAULT_COLLECTION)
    #   6) use_tile_selector (0/1, default 1)
    #   7) full_cover_threshold (default 0.999)
    #   8) allow_union (0/1, default 1)           [NEW meaning, used to be allow_pair]
    #   9) n_anchors (default 24)
    #  10) window_days (default 21)
    #  11) max_union_tiles (default 2)

    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage:\n"
            "  python -m thess_geo_analytics.entrypoints.BuildSceneCatalog "
            "<aoi_geojson> [days] [cloud_max] [max_items] [collection] "
            "[use_tile_selector 0|1] [full_cover_threshold] [allow_union 0|1] "
            "[n_anchors] [window_days] [max_union_tiles]\n\n"
            "Examples:\n"
            "  python -m thess_geo_analytics.entrypoints.BuildSceneCatalog aoi/EL522_Thessaloniki.geojson 30 30 100 sentinel-2-l2a 1\n"
            "  python -m thess_geo_analytics.entrypoints.BuildSceneCatalog aoi/EL522_Thessaloniki.geojson 2190 20 5000 sentinel-2-l2a 1 0.999 1 72 21 2\n"
        )

    aoi_path = Path(sys.argv[1])

    days = int(sys.argv[2]) if len(sys.argv) > 2 else 365
    cloud_max = float(sys.argv[3]) if len(sys.argv) > 3 else 20.0
    max_items = int(sys.argv[4]) if len(sys.argv) > 4 else 5000
    collection = str(sys.argv[5]) if len(sys.argv) > 5 else None

    use_tile_selector = _as_bool01(sys.argv[6]) if len(sys.argv) > 6 else True
    full_cover_threshold = float(sys.argv[7]) if len(sys.argv) > 7 else 0.999

    # IMPORTANT: this used to be allow_pair in your older version,
    # but your TileSelector now uses allow_union/max_union_tiles.
    allow_union = _as_bool01(sys.argv[8]) if len(sys.argv) > 8 else True

    n_anchors = int(sys.argv[9]) if len(sys.argv) > 9 else 24
    window_days = int(sys.argv[10]) if len(sys.argv) > 10 else 21
    max_union_tiles = int(sys.argv[11]) if len(sys.argv) > 11 else 2

    pipeline = BuildSceneCatalogPipeline(aoi_path=aoi_path)

    params = BuildSceneCatalogParams(
        days=days,
        cloud_cover_max=cloud_max,
        max_items=max_items,
        collection=collection or BuildSceneCatalogParams().collection,
        use_tile_selector=use_tile_selector,
        full_cover_threshold=full_cover_threshold,
        allow_union=allow_union,
        max_union_tiles=max_union_tiles,
        n_anchors=n_anchors,
        window_days=window_days,
    )

    out = pipeline.run(params)
    print(f"[OK] Pipeline returned: {out}")


if __name__ == "__main__":
    main()
