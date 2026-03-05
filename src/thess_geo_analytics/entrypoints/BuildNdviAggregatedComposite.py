from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Sequence

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.core.settings import DATA_LAKE
from thess_geo_analytics.pipelines.BuildNdviAggregatedCompositePipeline import (
    BuildNdviAggregatedCompositePipeline,
    BuildNdviAggregatedCompositeParams,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.utils.log_parameters import log_parameters


PARAMETER_DOCS = {
    "aoi_path": "AOI polygon GeoJSON used to define the target grid.",
    "aoi_id": "AOI identifier used in filenames (e.g. el522). Comes from pipeline.thess.yaml.",
    "aggregated_root": "Root folder containing pre-aggregated timestamp rasters: <root>/<timestamp>/B04,B08,SCL.tif",
    "strategy": "Aggregation strategy: 'monthly' (default) or 'timestamp'.",
    "max_scenes_per_period": "Optional cap on number of scenes per period (None = no cap).",
    "min_scenes_per_month": "Minimum scenes required to form a monthly composite (for 'monthly' strategy).",
    "fallback_to_quarterly": "If True, months with too few scenes fall back to quarterly composites.",
    "enable_cloud_masking": "If True, use SCL to mask clouds/invalid pixels when available.",
    "verbose": "If True, print per-scene / composite diagnostics.",
    "max_workers": "Maximum parallel workers for NDVI composites (env THESS_NDVI_MAX_WORKERS).",
    "debug": "If True, run sequentially and re-raise exceptions (env THESS_NDVI_DEBUG).",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build NDVI composites from aggregated timestamp rasters."
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Run in debug mode (sequential, re-raise exceptions). Overrides THESS_NDVI_DEBUG.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging for per-scene / composite diagnostics.",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum number of parallel workers. Overrides THESS_NDVI_MAX_WORKERS.",
    )
    return p.parse_args(argv)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)

    cfg = load_pipeline_config()
    ndvi_cfg = cfg.ndvi_composite_params

    # AOI from config (e.g. EL522_Thessaloniki.geojson)
    aoi_filename = f"{cfg.aoi_id.upper()}_{cfg.region_name.replace(' ', '_')}.geojson"
    aoi_path = RepoPaths.AOI / aoi_filename
    aoi_id = cfg.aoi_id

    aggregated_root = Path(DATA_LAKE) / "data_raw" / "aggregated"


    # Verbose: CLI flag wins, otherwise config
    if args.verbose:
        verbose = True
    else:
        verbose = getattr(ndvi_cfg, "verbose", False)

    # max_workers: CLI > env > default
    if args.max_workers is not None:
        max_workers = args.max_workers
    else:
        max_workers = int(os.getenv("THESS_NDVI_MAX_WORKERS", "4"))
    max_workers = max(1, max_workers)

    # debug: CLI flag wins, otherwise env
    if args.debug:
        debug = True
    else:
        debug = _env_bool("THESS_NDVI_DEBUG", False)

    params = BuildNdviAggregatedCompositeParams(
        aoi_path=aoi_path,
        aoi_id=aoi_id,
        aggregated_root=aggregated_root,
        strategy=getattr(ndvi_cfg, "strategy", "monthly"),
        max_scenes_per_period=getattr(ndvi_cfg, "max_scenes_per_period", None),
        min_scenes_per_month=getattr(ndvi_cfg, "min_scenes_per_month", 2),
        fallback_to_quarterly=getattr(ndvi_cfg, "fallback_to_quarterly", True),
        enable_cloud_masking=getattr(ndvi_cfg, "enable_cloud_masking", True),
        verbose=verbose,
    )

    log_parameters(
        "ndvi_aggregated_composites",
        params={**params.__dict__, "max_workers": max_workers, "debug": debug},
        extra={"region": cfg.region_name, "mode": cfg.mode},
        docs=PARAMETER_DOCS,
    )

    pipe = BuildNdviAggregatedCompositePipeline()
    pipe.run(params, max_workers=max_workers, debug=debug)


if __name__ == "__main__":
    main()