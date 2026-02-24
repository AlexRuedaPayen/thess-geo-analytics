from __future__ import annotations

import argparse
from pathlib import Path

from thess_geo_analytics.pipelines.BuildSceneCatalogPipeline import (
    BuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)
from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.utils.log_parameters import log_parameters


PARAMETER_DOCS = {
    "date_start": "Earliest acquisition date (YYYY-MM-DD).",
    "cloud_cover_max": "Maximum allowed cloud cover percentage.",
    "max_items": "Maximum number of STAC items (scenes) to retrieve.",
    "collection": "STAC collection ID (e.g. sentinel-2-l2a).",
    "use_tile_selector": "Whether to apply tile selector based on AOI coverage.",
    "full_cover_threshold": "Minimum fraction of AOI covered by a single tile.",
    "allow_union": "Allow merging multiple tiles to cover the AOI.",
    "n_anchors": "Number of temporal anchors in the selection period.",
    "window_days": "Half-window size (days) around each anchor.",
    "max_union_tiles": "Maximum number of tiles allowed in a union.",
}


def _as_bool01(x: str) -> bool:
    x = x.strip().lower()
    if x in {"1", "true", "yes", "y"}:
        return True
    if x in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Expected boolean 0/1 or true/false, got: {x}")


def main() -> None:
    cfg = load_pipeline_config()

    # scene_catalog-specific knobs (cloud_cover_max, max_items, etc.)
    sc_cfg = cfg.scene_catalog_params

    # Single global temporal knob, from YAML: pipeline.date_start
    pipeline_date_start = cfg.raw["pipeline"]["date_start"]

    aoi_default_path = cfg.aoi_path  # <--- this is important

    p = argparse.ArgumentParser(
        description=(
            "Build Sentinel-2 scene catalog "
            "(scenes_s2_all.csv + scenes_selected.csv)."
        )
    )

    p.add_argument(
        "--aoi",
        default=str(aoi_default_path),
        help="Path to AOI GeoJSON (default from YAML aoi.file or derived).",
    )

    # Core temporal & quality params
    p.add_argument(
        "--date-start",
        default=pipeline_date_start,
        help="Earliest acquisition date (YYYY-MM-DD, default from pipeline.date_start).",
    )
    p.add_argument(
        "--cloud-max",
        type=float,
        default=sc_cfg.get("cloud_cover_max", 20.0),
        help="Maximum allowed cloud cover percentage.",
    )
    p.add_argument(
        "--max-items",
        type=int,
        default=sc_cfg.get("max_items", 5000),
        help="Max STAC items to retrieve.",
    )
    p.add_argument(
        "--collection",
        default=sc_cfg.get("collection", "sentinel-2-l2a"),
        help="STAC collection id.",
    )

    # Tile selector / coverage
    p.add_argument(
        "--use-tile-selector",
        default=str(sc_cfg.get("use_tile_selector", True)),
        help="Whether to apply tile selector (0/1, true/false).",
    )
    p.add_argument(
        "--full-cover-threshold",
        type=float,
        default=sc_cfg.get("full_cover_threshold", 0.999),
        help="AOI coverage threshold for a tile.",
    )
    p.add_argument(
        "--allow-union",
        default=str(sc_cfg.get("allow_union", True)),
        help="Allow merging multiple tiles (0/1, true/false).",
    )
    p.add_argument(
        "--n-anchors",
        type=int,
        default=sc_cfg.get("n_anchors", 24),
        help="Number of temporal anchors.",
    )
    p.add_argument(
        "--window-days",
        type=int,
        default=sc_cfg.get("window_days", 21),
        help="Temporal window per anchor (days).",
    )
    p.add_argument(
        "--max-union-tiles",
        type=int,
        default=sc_cfg.get("max_union_tiles", 20),
        help="Maximum number of tiles to union.",
    )

    args = p.parse_args()

    use_tile_selector = _as_bool01(args.use_tile_selector)
    allow_union = _as_bool01(args.allow_union)

    aoi_path = Path(args.aoi)

    params = BuildSceneCatalogParams(
        date_start=args.date_start,
        cloud_cover_max=args.cloud_max,
        max_items=args.max_items,
        collection=args.collection,
        use_tile_selector=use_tile_selector,
        full_cover_threshold=args.full_cover_threshold,
        allow_union=allow_union,
        max_union_tiles=args.max_union_tiles,
        n_anchors=args.n_anchors,
        window_days=args.window_days,
    )

    # Log parameters with meanings
    extra = {
        "mode": cfg.mode,
        "region": cfg.region_name,
        "aoi_id": cfg.aoi_id,
        "aoi_path": str(aoi_path),
    }
    log_parameters("BuildSceneCatalog", params, PARAMETER_DOCS, extra)

    pipeline = BuildSceneCatalogPipeline(aoi_path=aoi_path)
    out = pipeline.run(params)
    print(f"[OK] Pipeline returned: {out}")


if __name__ == "__main__":
    main()