from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple
import os

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
        *,
        max_workers: int | None = None,
        debug: bool | None = None,
    ) -> List[Tuple[str, Path, Path]]:
        """
        max_workers / debug can be overridden via env:

          - THESS_NDVI_MAX_WORKERS
          - THESS_NDVI_DEBUG
        """

        # --- resolve parallelism knobs ---------------------------------
        if max_workers is None:
            # default: be conservative so we don't get OOM-killed in Docker
            env_val = os.environ.get("THESS_NDVI_MAX_WORKERS")
            try:
                max_workers = int(env_val) if env_val is not None else 1
            except ValueError:
                max_workers = 1

        if debug is None:
            debug_env = os.environ.get("THESS_NDVI_DEBUG", "").strip().lower()
            debug = debug_env in {"1", "true", "yes", "y", "on"}

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
                max_workers=max_workers,
                debug=debug,
            )

        # default: monthly with quarterly fallback
        return builder.run_monthly_with_fallback(
            aggregated_root=params.aggregated_root,
            max_scenes=params.max_scenes_per_period,
            min_scenes=params.min_scenes_per_month,
            fallback=params.fallback_to_quarterly,
            enable_cloud_masking=params.enable_cloud_masking,
            verbose=params.verbose,
            max_workers=max_workers,
            debug=debug,
        )