from __future__ import annotations

from pathlib import Path
import argparse

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.pipelines.BuildSuperpixelFeaturesPipeline import (
    BuildSuperpixelFeaturesPipeline,
    BuildSuperpixelFeaturesParams,
)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Aggregate pixel-level 7D features into superpixel-level features."
    )
    p.add_argument(
        "--labels",
        type=str,
        default=str(RepoPaths.OUTPUTS / "cogs" / "superpixels.tif"),
        help="Path to superpixel labels raster",
    )
    p.add_argument(
        "--features",
        type=str,
        default=str(RepoPaths.OUTPUTS / "cogs" / "pixel_features_7d.tif"),
        help="Path to pixel feature raster (7 bands)",
    )
    p.add_argument(
        "--out-csv",
        type=str,
        default=str(RepoPaths.OUTPUTS / "cogs" / "superpixel_features.csv"),
        help="Output CSV path",
    )

    args = p.parse_args()

    params = BuildSuperpixelFeaturesParams(
        superpixel_raster=Path(args.labels),
        pixel_features_raster=Path(args.features),
        out_csv=Path(args.out_csv),
    )

    BuildSuperpixelFeaturesPipeline().run(params)


if __name__ == "__main__":
    main()