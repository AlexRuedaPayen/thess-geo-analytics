from __future__ import annotations

import argparse
from pathlib import Path

from thess_geo_analytics.pipelines.BuildSceneCatalogPipeline import (
    BuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)
from thess_geo_analytics.core.pipeline_config import load_pipeline_config


def _as_bool01(x: str) -> bool:
    x = x.strip().lower()
    if x in {"1", "true", "yes", "y"}:
        return True
    if x in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Expected boolean 0/1 or true/false, got: {x}")


def main() -> None:
    cfg = load_pipeline_config()
    sc_cfg = cfg.scene_catalog_params
    aoi_default_path = cfg.aoi_path  # <--- this is important

    p = argparse.ArgumentParser(
        description="Build Sentinel-2 scene catalog (scenes_s2_all.csv + scenes_selected.csv)."
    )

    p.add_argument(
        "--aoi",
        default=str(aoi_default_path),
        help="Path to AOI GeoJSON (default from YAML aoi.file).",
    )

    # Core temporal & quality params
    p.add_argument(
        "--days",
        type=int,
        default=sc_cfg.get("days", 365),
        help="Look-back window (days).",
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
        default=sc_cfg.get("max_union_tiles", 2),
        help="Maximum number of tiles to union.",
    )

    args = p.parse_args()

    use_tile_selector = _as_bool01(args.use_tile_selector)
    allow_union = _as_bool01(args.allow_union)

    aoi_path = Path(args.aoi)

    pipeline = BuildSceneCatalogPipeline(aoi_path=aoi_path)

    params = BuildSceneCatalogParams(
        days=args.days,
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

    out = pipeline.run(params)
    print(f"[OK] Pipeline returned: {out}")


if __name__ == "__main__":
    main()