from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from thess_geo_analytics.core.settings import DATA_LAKE
from thess_geo_analytics.pipelines.BuildDownsampledAggregatedTimestampsPipeline import (
    BuildDownsampledAggregatedTimestampsPipeline,
    BuildDownsampledAggregatedTimestampsParams,
)
from thess_geo_analytics.utils.log_parameters import log_parameters


PARAMETER_DOCS = {
    "src_root": "Root folder containing aggregated timestamp mosaics (B04/B08/SCL).",
    "dst_root": "Destination folder where downsampled mosaics will be written.",
    "factor": (
        "Downsample factor relative to source resolution. "
        "1 = no downsampling, 10 = 100m if source is 10m, 20 = 200m."
    ),
    "continuous_method": (
        "Downsampling method for continuous rasters (B04/B08 reflectance). "
        "Options: nanmean (default) or nanmedian."
    ),
    "categorical_method": (
        "Downsampling method for categorical rasters (SCL). "
        "Options: mode (default) or nearest."
    ),
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Downsample aggregated timestamp mosaics (B04/B08/SCL) to reduce RAM usage in NDVI computation."
    )

    p.add_argument(
        "--factor",
        type=int,
        default=1,
        help="Downsample factor (1=no downsampling, 10=100m, 20=200m if input is 10m).",
    )

    p.add_argument(
        "--src-root",
        type=str,
        default=None,
        help="Source root folder (default: DATA_LAKE/data_raw/aggregated).",
    )

    p.add_argument(
        "--dst-root",
        type=str,
        default=None,
        help="Destination root folder (default: DATA_LAKE/data_raw/aggregated_100m).",
    )

    p.add_argument(
        "--continuous-method",
        type=str,
        default="nanmean",
        choices=["nanmean", "nanmedian"],
        help="Method used to downsample continuous rasters (B04/B08).",
    )

    p.add_argument(
        "--categorical-method",
        type=str,
        default="mode",
        choices=["mode", "nearest"],
        help="Method used to downsample categorical rasters (SCL).",
    )

    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)

    src_root = (
        Path(args.src_root)
        if args.src_root
        else Path(DATA_LAKE) / "data_raw" / "aggregated"
    )

    dst_root = (
        Path(args.dst_root)
        if args.dst_root
        else Path(DATA_LAKE) / "data_raw" / "aggregated_100m"
    )

    params = BuildDownsampledAggregatedTimestampsParams(
        src_root=src_root,
        dst_root=dst_root,
        factor=int(args.factor),
        continuous_method=str(args.continuous_method),
        categorical_method=str(args.categorical_method),
    )

    extra = {
        "target_resolution_m": 10 * params.factor if params.factor > 0 else 10,
    }

    log_parameters(
        "downsample_aggregated_timestamps",
        params,
        PARAMETER_DOCS,
        extra,
    )

    pipe = BuildDownsampledAggregatedTimestampsPipeline()
    outputs = pipe.run(params)

    print(f"[OK] Downsampled {len(outputs)} band rasters")
    print(f"[OUTPUT] {dst_root}")


if __name__ == "__main__":
    main()