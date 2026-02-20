from __future__ import annotations

import argparse
from pathlib import Path

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.pipelines.BuildNdviAnomalyMapsPipeline import (
    BuildNdviAnomalyMapsPipeline,
    BuildNdviAnomalyMapsParams,
)


def main() -> None:
    p = argparse.ArgumentParser(
        description=(
            "Build NDVI pixel-wise anomaly rasters:\n"
            "  pixel_anomaly = pixel_value - pixel_climatology_monthly_median\n"
            "using existing monthly NDVI composites (ndvi_YYYY-MM_<aoi>.tif)."
        )
    )

    p.add_argument(
        "--aoi-id",
        default="el522",
        help="AOI identifier used in NDVI COG filenames (e.g. ndvi_YYYY-MM_el522.tif).",
    )

    p.add_argument(
        "--cogs-dir",
        default=str(RepoPaths.OUTPUTS / "cogs"),
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

    args = p.parse_args()

    pipe = BuildNdviAnomalyMapsPipeline()
    results = pipe.run(
        BuildNdviAnomalyMapsParams(
            aoi_id=args.aoi_id,
            cogs_dir=Path(args.cogs_dir),
            year_start=args.year_start,
            year_end=args.year_end,
            recompute_climatology=args.recompute_climatology,
            verbose=args.verbose,
        )
    )

    for label, tif_path, png_path in results:
        print(f"[OK] anomaly {label}: GeoTIFF → {tif_path}")
        print(f"[OK] anomaly {label}: Preview → {png_path}")


if __name__ == "__main__":
    main()