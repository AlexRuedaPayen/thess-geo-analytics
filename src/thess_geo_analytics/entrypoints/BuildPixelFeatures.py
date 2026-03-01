from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.utils.log_parameters import log_parameters
from thess_geo_analytics.pipelines.BuildPixelFeaturesPipeline import (
    BuildPixelFeaturesPipeline,
    BuildPixelFeaturesParams,
)

PARAMETER_DOCS = {
    "ndvi_dir": "Directory containing NDVI anomaly COGs (ndvi_anomaly_YYYY-MM_<aoi_id>.tif).",
    "pattern": "Glob pattern used to discover anomaly COGs.",
    "aoi_id": "AOI identifier; only anomaly COGs ending with _<aoi_id>.tif are used (if set).",
    "out_path": "Output 7-band GeoTIFF with pixelwise temporal features.",
    "tile_height": "Tile height (rows) used to process rasters in blocks.",
    "tile_width": "Tile width (cols) used to process rasters in blocks.",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Compute 7D pixelwise NDVI temporal features from anomaly COGs "
            "(ndvi_anomaly_YYYY-MM_<aoi_id>.tif)."
        )
    )

    p.add_argument(
        "--out",
        default=None,
        help="Optional: override output path for pixel_features_7d.tif",
    )

    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    # 1) Runtime args
    args = parse_args(argv)

    # 2) Config
    cfg = load_pipeline_config()
    aoi_id = cfg.aoi_id

    ndvi_dir = RepoPaths.OUTPUTS / "cogs"
    default_out = RepoPaths.OUTPUTS / "cogs" / f"pixel_features_7d_{aoi_id}.tif"
    out_path = Path(args.out) if args.out is not None else default_out

    params = BuildPixelFeaturesParams(
        ndvi_dir=ndvi_dir,
        pattern="ndvi_anomaly_*.tif",
        aoi_id=aoi_id,
        out_path=out_path,
        # tile sizes: keep defaults, or override here if you want
    )

    extra = {
        "mode": cfg.mode,
        "region": cfg.region_name,
        "aoi_id": aoi_id,
    }

    log_parameters(
        "ndvi_pixel_features",
        params,
        extra=extra,
        docs=PARAMETER_DOCS,
    )
    pipe = BuildPixelFeaturesPipeline()
    out = pipe.run(params)


if __name__ == "__main__":
    main()