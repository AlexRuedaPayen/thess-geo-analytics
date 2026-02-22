from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from thess_geo_analytics.builders.NdviAnomalyMapsBuilder import (
    NdviAnomalyMapsBuilder,
    NdviAnomalyMapsConfig,
)


@dataclass(frozen=True)
class BuildNdviAnomalyMapsParams:
    """
    Parameters for NDVI pixel-wise anomaly computation.
    """

    aoi_id: str = "el522"
    cogs_dir: Path = Path("outputs") / "cogs"

    # Limit which years to include in climatology + anomaly
    year_start: Optional[int] = None
    year_end: Optional[int] = None

    # If False and climatology tifs already exist, they can be reused
    recompute_climatology: bool = False

    verbose: bool = False


class BuildNdviAnomalyMapsPipeline:
    """
    Orchestrates NdviAnomalyBuilder.

    Returns:
      list of (label, out_tif, out_png)
      where label is YYYY-MM.
    """

    def __init__(self) -> None:
        self._builder: Optional[NdviAnomalyMapsBuilder] = None

    def _get_builder(self, params: BuildNdviAnomalyMapsParams) -> NdviAnomalyMapsBuilder:
        if self._builder is None:
            cfg = NdviAnomalyMapsConfig(
                cogs_dir=params.cogs_dir,
                aoi_id=params.aoi_id,
                year_start=params.year_start,
                year_end=params.year_end,
                recompute_climatology=params.recompute_climatology,
                verbose=params.verbose,
            )
            self._builder = NdviAnomalyMapsBuilder(cfg=cfg)
        return self._builder

    def run(self, params: BuildNdviAnomalyMapsParams) -> list[tuple[str, Path, Path]]:
        builder = self._get_builder(params)
        return builder.run_all()


# Optional smoke test
if __name__ == "__main__":
    print("=== BuildNdviAnomalyPipeline Smoke Test ===")
    pipe = BuildNdviAnomalyMapsPipeline()
    outs = pipe.run(BuildNdviAnomalyMapsParams(recompute_climatology=False))
    print(f"[OK] anomalies produced: {len(outs)}")
    print("✓ Smoke test OK")