from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

from thess_geo_analytics.builders.DownsampleAggregatedTimestampsBuilder import (
    DownsampleAggregatedTimestampsBuilder,
    DownsampleAggregatedTimestampsParams,
)


@dataclass(frozen=True)
class BuildDownsampledAggregatedTimestampsParams:
    src_root: Path
    dst_root: Path
    factor: int = 10
    bands: tuple[str, ...] = ("B04", "B08", "SCL")
    scl_nodata: int = 0
    continuous_method: str = "nanmean"
    categorical_method: str = "mode"


class BuildDownsampledAggregatedTimestampsPipeline:
    def run(self, params: BuildDownsampledAggregatedTimestampsParams) -> List[Path]:
        builder = DownsampleAggregatedTimestampsBuilder(
            DownsampleAggregatedTimestampsParams(
                src_root=params.src_root,
                dst_root=params.dst_root,
                factor=params.factor,
                bands=params.bands,
                scl_nodata=params.scl_nodata,
                continuous_method=params.continuous_method,
                categorical_method=params.categorical_method,
            )
        )
        return builder.run()