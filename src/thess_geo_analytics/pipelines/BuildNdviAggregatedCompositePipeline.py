from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple

from thess_geo_analytics.builders.NdviAggregatedCompositeBuilder import (
    NdviAggregatedCompositeBuilder,
)


@dataclass(frozen=True)
class BuildNdviAggregatedCompositeParams:
    aoi_path: Path
    aoi_id: str

    aggregated_root: Path

    strategy: str = "monthly"

    max_scenes_per_period: Optional[int] = None
    min_scenes_per_month: int = 2
    fallback_to_quarterly: bool = True

    enable_cloud_masking: bool = True

    verbose: bool = False


class BuildNdviAggregatedCompositePipeline:
    """
    Pipeline orchestrating NDVI composite building.
    Parameters are resolved *here*, not inside the builder.
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
            return builder.run_all_timestamps(
                aggregated_root=params.aggregated_root,
                max_scenes=params.max_scenes_per_period,
                enable_cloud_masking=params.enable_cloud_masking,
                verbose=params.verbose,
            )

        return builder.run_monthly_with_fallback(
            aggregated_root=params.aggregated_root,
            max_scenes=params.max_scenes_per_period,
            min_scenes=params.min_scenes_per_month,
            fallback=params.fallback_to_quarterly,
            enable_cloud_masking=params.enable_cloud_masking,
            verbose=params.verbose,
        )