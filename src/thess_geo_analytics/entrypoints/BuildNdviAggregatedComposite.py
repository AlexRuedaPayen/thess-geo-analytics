from __future__ import annotations

import argparse
from typing import Sequence
from pathlib import Path

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.pipelines.BuildNdviPeriodStatsPipeline import (
    BuildNdviPeriodStatsPipeline,
)
from thess_geo_analytics.utils.log_parameters import log_parameters
from thess_geo_analytics.utils.RepoPaths import RepoPaths


PARAMETER_DOCS = {
    "aoi_id": "AOI identifier used in filenames (e.g. el522). Comes from pipeline.thess.yaml",
    "cogs_dir": "Directory containing ndvi_<period>_<aoi>.tif composites (usually outputs/cogs).",
    "out_csv": "Output CSV file containing period-level NDVI statistics.",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """
    No runtime parameters are needed.
    NDVI period stats are fully config-driven.
    """
    p = argparse.ArgumentParser(
        description="Compute NDVI stats for ALL existing ndvi_<period>_<aoi>.tif composites."
    )
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    # 1) CLI (none)
    _args = parse_args(argv)

    # 2) Load configuration
    cfg = load_pipeline_config()
    aoi_id = cfg.aoi_id  # ← exactly like the NDVI composite entrypoint

    cogs_dir = RepoPaths.OUTPUTS / "cogs"
    out_csv = RepoPaths.table("ndvi_period_stats.csv")

    # 3) Log parameters
    params_dict = {
        "aoi_id": aoi_id,
        "cogs_dir": str(cogs_dir),
        "out_csv": str(out_csv),
    }

    log_parameters(
        "ndvi_period_stats",
        params=params_dict,
        extra={"region": cfg.region_name, "mode": cfg.mode},
        docs=PARAMETER_DOCS,
    )

    # 4) Run pipeline
    pipe = BuildNdviPeriodStatsPipeline()
    pipe.run_all_existing(aoi_id=aoi_id, out_csv=out_csv)

    print("\n=== OUTPUT (NDVI period stats) ===")
    print(f"[OK] Written → {out_csv}")


if __name__ == "__main__":
    main()