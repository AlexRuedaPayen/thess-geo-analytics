from __future__ import annotations

import argparse
from typing import Sequence

from thess_geo_analytics.pipelines.BuildNdviAggregatedCompositePipeline import (
    BuildNdviAggregatedCompositePipeline,
    BuildNdviAggregatedCompositeParams,
)
from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.utils.log_parameters import log_parameters
from thess_geo_analytics.utils.RepoPaths import RepoPaths


PARAMETER_DOCS = {
    "strategy": "timestamp = 1 composite per aggregated folder; monthly = group by month with optional quarter fallback.",
    "aggregated_root": "Root folder for aggregated timestamp rasters (DATA_LAKE/data_raw/aggregated).",
    "max_scenes_per_period": "Maximum number of timestamps used per composite (None = all).",
    "min_scenes_per_month": "Minimum timestamps required to build a monthly composite.",
    "fallback_to_quarterly": "If True, build quarterly composite for sparse months.",
    "enable_cloud_masking": "If True, apply SCL-based cloud masking before NDVI.",
    "verbose": "Enable verbose logging.",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """
    Runtime overrides only.
    Everything else comes from pipeline.thess.yaml via PipelineConfig.
    """
    p = argparse.ArgumentParser(
        description="Build NDVI composites from aggregated timestamp rasters (config-driven)."
    )

    # For now, only runtime verbosity override; all other knobs are config-driven.
    p.add_argument("--verbose", action="store_true", help="Enable verbose logging.")

    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    # 1) Runtime args (only verbose)
    args = parse_args(argv)

    # 2) Load pipeline config
    cfg = load_pipeline_config()

    # AOI / region come from config
    aoi_path = cfg.aoi_path
    aoi_id = cfg.aoi_id

    # NDVI composite parameters (mode-aware)
    ndvi_cfg = cfg.effective_ndvi_composite_params

    # 3) Resolve NDVI params with sensible defaults
    strategy = ndvi_cfg.get("strategy", "monthly")
    max_scenes_per_period = ndvi_cfg.get("max_scenes_per_period", None)
    min_scenes_per_month = int(ndvi_cfg.get("min_scenes_per_month", 2))
    fallback_to_quarterly = bool(ndvi_cfg.get("fallback_to_quarterly", True))
    enable_cloud_masking = bool(ndvi_cfg.get("cloud_masking", True))

    # Root of aggregated timestamp rasters
    aggregated_root = RepoPaths.DATA_LAKE / "data_raw" / "aggregated"

    params = BuildNdviAggregatedCompositeParams(
        aoi_path=aoi_path,
        aoi_id=aoi_id,
        aggregated_root=aggregated_root,
        strategy=strategy,
        max_scenes_per_period=max_scenes_per_period,
        min_scenes_per_month=min_scenes_per_month,
        fallback_to_quarterly=fallback_to_quarterly,
        enable_cloud_masking=enable_cloud_masking,
        verbose=bool(args.verbose or cfg.debug),
    )

    # Extra context for logging
    extra = {
        "mode": cfg.mode,
        "region": cfg.region_name,
        "aoi_id": cfg.aoi_id,
        "strategy": strategy,
    }

    log_parameters(
        "ndvi_aggregated_composites",
        params,
        extra=extra,
        docs=PARAMETER_DOCS,
    )

    pipe = BuildNdviAggregatedCompositePipeline()
    outputs = pipe.run(params)

    print("\n=== OUTPUTS (NDVI aggregated composites) ===")
    for label, out_tif, meta_path in outputs:
        print(f"[OK] {label} NDVI → {out_tif}")
        print(f"[META] {meta_path}")


if __name__ == "__main__":
    main()