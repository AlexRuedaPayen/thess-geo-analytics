from __future__ import annotations
import argparse
from pathlib import Path

from thess_geo_analytics.pipelines.BuildPixelFeaturesPipeline import (
    BuildPixelFeaturesPipeline,
    BuildPixelFeaturesParams,
)

def main():
    p = argparse.ArgumentParser(description="Compute 7D pixelwise NDVI temporal features.")
    p.add_argument("--ndvi-dir", default=None)
    p.add_argument("--out", default=None)

    args = p.parse_args()

    params = BuildPixelFeaturesParams(
        ndvi_dir=Path(args.ndvi_dir) if args.ndvi_dir else BuildPixelFeaturesParams.ndvi_dir,
        out_path=Path(args.out) if args.out else BuildPixelFeaturesParams.out_path,
    )

    BuildPixelFeaturesPipeline().run(params)


if __name__ == "__main__":
    main()