from __future__ import annotations

import argparse
from pathlib import Path

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.pipelines.BuildSuperpixelFeaturesPipeline import (
    BuildSuperpixelFeaturesPipeline,
    BuildSuperpixelFeaturesParams,
)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Aggregate 7-band pixel features over SLIC superpixels."
    )

    p.add_argument(
        "--features",
        default=str(RepoPaths.OUTPUTS / "cogs" / "pixel_features_7d.tif"),
        help="Path to 7-band pixel feature raster (from BuildPixelFeatures).",
    )
    p.add_argument(
        "--labels",
        default=str(RepoPaths.OUTPUTS / "cogs" / "superpixels.tif"),
        help="Path to superpixel label raster. "
             "If missing, it will be created from the features.",
    )
    p.add_argument(
        "--out-csv",
        default=str(RepoPaths.OUTPUTS / "cogs" / "superpixel_features.csv"),
        help="Output CSV file with superpixel-level features.",
    )
    p.add_argument("--n-segments", type=int, default=1500, help="Number of SLIC superpixels.")
    p.add_argument("--compactness", type=float, default=10.0, help="SLIC compactness parameter.")
    p.add_argument("--max-iter", type=int, default=10, help="SLIC max iterations.")

    args = p.parse_args()

    params = BuildSuperpixelFeaturesParams(
        pixel_features_raster=Path(args.features),
        superpixel_raster=Path(args.labels),
        n_segments=args.n_segments,
        compactness=args.compactness,
        max_iter=args.max_iter,
        out_csv=Path(args.out_csv),
    )

    BuildSuperpixelFeaturesPipeline().run(params)


if __name__ == "__main__":
    main()