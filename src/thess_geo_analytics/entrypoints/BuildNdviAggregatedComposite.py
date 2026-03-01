from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Sequence

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.pipelines.BuildNdviAggregatedCompositePipeline import (
    BuildNdviAggregatedCompositePipeline,
    BuildNdviAggregatedCompositeParams,
)
from thess_geo_analytics.utils.log_parameters import log_parameters
from thess_geo_analytics.utils.RepoPaths import RepoPaths


PARAMETER_DOCS = {
    "aoi_path": "AOI polygon GeoJSON used to define the target grid.",
    "aoi_id": "AOI identifier used in filenames (e.g. el522). Comes from pipeline.thess.yaml.",
    "aggregated_root": "Root folder containing pre-aggregated timestamp rasters: <root>/<timestamp>/B04,B08,SCL.tif",
    "strategy": "Aggregation strategy: 'monthly' (default) or 'timestamp'.",
    "max_scenes_per_period": "Optional cap on number of scenes per period (None = no cap).",
    "min_scenes_per_month": "Minimum scenes required to form a monthly composite (for 'monthly' strategy).",
    "fallback_to_quarterly": "If True, months with too few scenes fall back to quarterly composites.",
    "enable_cloud_masking": "If True, use SCL to mask clouds/invalid pixels when available.",
    "verbose": "If True, print per-scene warnings.",
    "max_workers": "Maximum parallel workers for NDVI composites.",
    "debug": "If True, run sequentially and re-raise exceptions.",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """
    Currently the pipeline is fully config-driven.
    CLI args are kept for future extensions (e.g. --debug override).
    """
    p = argparse.ArgumentParser(
        description="Build NDVI composites from pre-aggregated Sentinel-2 timestamps."
    )
    # Example for future:
    # p.add_argument("--debug", action="store_true", help="Force debug (sequential) mode.")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    _args = parse_args(argv)

    # 1) Load high-level pipeline config
    cfg = load_pipeline_config()
    ms = cfg.mode_settings

    # Raw NDVI config from YAML
    ndvi_raw = cfg.ndvi_composite_params
    # Apply dev/deep clamping rules
    ndvi_cfg = ms.effective_ndvi_composites(ndvi_raw)

    # 2) Resolve paths
    # aggregated_root from YAML, with ${DATA_LAKE} etc. expanded
    aggregated_root_str = ndvi_cfg.get("aggregated_root", "${DATA_LAKE}/data_raw/aggregated")
    aggregated_root = Path(os.path.expandvars(aggregated_root_str))

    # AOI path comes from config (already resolved into a Path)
    aoi_path: Path = cfg.aoi_path
    aoi_id: str = cfg.aoi_id

    # 3) Build params dataclass
    params = BuildNdviAggregatedCompositeParams(
        aoi_path=aoi_path,
        aoi_id=aoi_id,
        aggregated_root=aggregated_root,
        strategy=ndvi_cfg.get("strategy", "monthly"),
        max_scenes_per_period=ndvi_cfg.get("max_scenes_per_period"),
        min_scenes_per_month=ndvi_cfg.get("min_scenes_per_month", 2),
        fallback_to_quarterly=ndvi_cfg.get("fallback_to_quarterly", True),
        enable_cloud_masking=ndvi_cfg.get("enable_cloud_masking", True),
        verbose=ndvi_cfg.get("verbose", False),
        max_workers=ndvi_cfg.get("max_workers", 4),
        debug=ndvi_cfg.get("debug", False),
    )

    # 4) Log parameters for traceability
    log_parameters(
        "ndvi_aggregated_composites",
        params=params,
        extra={
            "region": cfg.region_name,
            "mode": cfg.mode,
        },
        docs=PARAMETER_DOCS,
    )

    # 5) Run pipeline
    pipe = BuildNdviAggregatedCompositePipeline()
    results = pipe.run(params)

    print("\n=== OUTPUTS (NDVI aggregated composites) ===")
    print(f"[OK] Composites built: {len(results)}")
    if results:
        # Show a small sample of outputs
        for label, tif_path, meta_path in results[:5]:
            print(f" - {label}: {tif_path.name} (meta: {meta_path.name})")


if __name__ == "__main__":
    main()