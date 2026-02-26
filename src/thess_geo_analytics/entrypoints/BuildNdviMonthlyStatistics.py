from __future__ import annotations

import argparse
from typing import Sequence

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.pipelines.BuildNdviMonthlyStatisticsPipeline import (
    BuildNdviMonthlyStatisticsPipeline,
    BuildNdviMonthlyStatisticsParams,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.utils.log_parameters import log_parameters


PARAMETER_DOCS = {
    "aoi_id": "AOI identifier used in filenames (e.g. el522). Comes from pipeline.thess.yaml.",
    "stats_csv": "Output/input CSV for per-period NDVI stats (ndvi_period_stats.csv).",
    "out_parquet": "Main NDVI time series Parquet file (legacy spelling 'nvdi_timeseries').",
    "out_parquet_canonical": "Canonical NDVI time series Parquet file (ndvi_timeseries.parquet).",
    "out_fig": "PNG figure for NDVI time series plot.",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """
    No runtime overrides for now; everything is config-driven.
    Kept for future extensibility (e.g. --verbose, date filters, etc.).
    """
    p = argparse.ArgumentParser(
        description="Build NDVI monthly statistics: per-period stats + time series + plot."
    )
    # Example for future:
    # p.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    _args = parse_args(argv)

    # 1) Load config and resolve AOI/paths
    cfg = load_pipeline_config()
    aoi_id = cfg.aoi_id

    params = BuildNdviMonthlyStatisticsParams(
        aoi_id=aoi_id,
        stats_csv=RepoPaths.table("ndvi_period_stats.csv"),
        out_parquet=RepoPaths.table("nvdi_timeseries.parquet"),
        out_parquet_canonical=RepoPaths.table("ndvi_timeseries.parquet"),
        out_fig=RepoPaths.figure("ndvi_timeseries.png"),
    )

    # 2) Log parameters
    log_parameters(
        "ndvi_monthly_statistics",
        params=params,
        extra={
            "region": cfg.region_name,
            "mode": cfg.mode,
        },
        docs=PARAMETER_DOCS,
    )

    # 3) Run orchestration pipeline
    pipe = BuildNdviMonthlyStatisticsPipeline()
    out_parquet, out_fig = pipe.run(params)

    print("\n=== OUTPUTS (NDVI monthly statistics) ===")
    print(f"[OK] Period stats CSV → {params.stats_csv}")
    print(f"[OK] Time series Parquet → {params.out_parquet}")
    print(f"[OK] Time series Parquet (canonical) → {params.out_parquet_canonical}")
    print(f"[OK] Time series figure → {params.out_fig}")


if __name__ == "__main__":
    main()