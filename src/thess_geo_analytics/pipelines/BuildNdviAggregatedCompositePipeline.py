from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple

from thess_geo_analytics.builders.NdviAggregatedCompositeBuilder import (
    NdviAggregatedCompositeBuilder,
)


@dataclass(frozen=True)
class BuildNdviAggregatedCompositeParams:
    """
    High-level parameters for building NDVI composites from pre-aggregated
    Sentinel-2 timestamp folders under aggregated_root.

    The actual heavy lifting (reprojection, masking, COG writing) is done by
    NdviAggregatedCompositeBuilder.
    """

    # AOI
    aoi_path: Path
    aoi_id: str

    # Where pre-aggregated rasters live: <aggregated_root>/<timestamp>/B04,B08,SCL.tif
    aggregated_root: Path

    # Strategy:
    #   - "monthly": build ndvi_<YYYY-MM>_<aoi>.tif (with quarterly fallback)
    #   - "timestamp": one NDVI per timestamp label
    strategy: str = "monthly"

    # Scene selection
    max_scenes_per_period: Optional[int] = None
    min_scenes_per_month: int = 2
    fallback_to_quarterly: bool = True

    # Processing flags
    enable_cloud_masking: bool = True
    verbose: bool = False

    # Parallelization / debugging
    max_workers: int = 4
    debug: bool = False


class BuildNdviAggregatedCompositePipeline:
    """
    Thin orchestration wrapper around NdviAggregatedCompositeBuilder.

    It converts the high-level params into the appropriate builder calls and
    returns a list of successful composites:

        [(label, out_tif_path, metadata_json_path), ...]
    """

    def run(
        self,
        params: BuildNdviAggregatedCompositeParams,
    ) -> List[Tuple[str, Path, Path]]:
        builder = NdviAggregatedCompositeBuilder(
            aoi_path=params.aoi_path,
            aoi_id=params.aoi_id,
        )

        if params.strategy == "timestamp":
            # One NDVI per timestamp
            return builder.run_all_timestamps(
                aggregated_root=params.aggregated_root,
                max_scenes=params.max_scenes_per_period,
                enable_cloud_masking=params.enable_cloud_masking,
                verbose=params.verbose,
                max_workers=params.max_workers,
                debug=params.debug,
            )

        # Default: monthly series with optional quarterly fallback
        return builder.run_monthly_with_fallback(
            aggregated_root=params.aggregated_root,
            max_scenes=params.max_scenes_per_period,
            min_scenes=params.min_scenes_per_month,
            fallback=params.fallback_to_quarterly,
            enable_cloud_masking=params.enable_cloud_masking,
            verbose=params.verbose,
            max_workers=params.max_workers,
            debug=params.debug,
        )