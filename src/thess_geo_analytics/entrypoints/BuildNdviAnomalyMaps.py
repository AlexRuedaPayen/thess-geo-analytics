from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.utils.log_parameters import log_parameters
from thess_geo_analytics.pipelines.BuildNdviAnomalyMapsPipeline import (
    BuildNdviAnomalyMapsPipeline,
    BuildNdviAnomalyMapsParams,
)

PARAMETER_DOCS = {
    "aoi_id": "AOI identifier used in NDVI COG filenames (ndvi_YYYY-MM_<aoi_id>.tif).",
    "cogs_dir": "Directory containing monthly NDVI composites.",
    "year_start": "Lowest year of monthly composites to include (inclusive; optional).",
    "year_end": "Highest year of monthly composites to include (inclusive; optional).",
    "min_years_for_climatology": "Minimum distinct years required for a robust per-month climatology.",
    "recompute_climatology": (
        "If true, recompute per-pixel monthly climatology even when "
        "ndvi_climatology_median_MM_<aoi_id>.tif already exist."
    ),
    "verbose": "Enable verbose logging.",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Build NDVI pixel-wise anomaly rasters:\n"
            "  anomaly(YYYY-MM) = NDVI(YYYY-MM) - median_monthly_climatology(month_of_year)\n"
            "using existing monthly NDVI composites (ndvi_YYYY-MM_<aoi_id>.tif)."
        )
    )

    p.add_argument(
        "--aoi-id",
        default=None,
        help="AOI identifier used in NDVI COG filenames (default: from pipeline config).",
    )

    p.add_argument(
        "--cogs-dir",
        default=None,
        help="Directory with monthly NDVI COGs (default: outputs/cogs).",
    )

    p.add_argument(
        "--year-start",
        type=int,
        default=None,
        help="Optional: lowest year of monthly composites to use (inclusive).",
    )
    p.add_argument(
        "--year-end",
        type=int,
        default=None,
        help="Optional: highest year of monthly composites to use (inclusive).",
    )

    p.add_argument(
        "--recompute-climatology",
        action="store_true",
        help=(
            "If set, recompute per-pixel monthly median climatology even if "
            "climatology GeoTIFFs already exist."
        ),
    )

    p.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging.",
    )

    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)

    cfg = load_pipeline_config()

    aoi_id = args.aoi_id or cfg.aoi_id
    cogs_dir = Path(args.cogs_dir) if args.cogs_dir else RepoPaths.OUTPUTS / "cogs"

    params = BuildNdviAnomalyMapsParams(
        aoi_id=aoi_id,
        cogs_dir=cogs_dir,
        year_start=args.year_start,
        year_end=args.year_end,
        recompute_climatology=bool(args.recompute_climatology),
        verbose=bool(args.verbose or cfg.debug),
    )

    extra = {
        "mode": cfg.mode,
        "region": cfg.region_name,
        "aoi_id": cfg.aoi_id,
    }

    log_parameters(
        "ndvi_anomaly_maps",
        params,
        extra=extra,
        docs=PARAMETER_DOCS,
    )

    pipe = BuildNdviAnomalyMapsPipeline()
    results = pipe.run(params)

    print("\n=== OUTPUTS (NDVI anomaly maps) ===")
    for label, tif_path, png_path in results:
        print(f"[OK] {label} anomaly GeoTIFF → {tif_path}")
        print(f"[OK] {label} anomaly preview → {png_path}")


if __name__ == "__main__":
    main()