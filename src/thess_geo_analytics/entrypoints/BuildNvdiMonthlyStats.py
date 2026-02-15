from __future__ import annotations

import argparse
from pathlib import Path

from thess_geo_analytics.pipelines.BuildNdviPeriodStatsPipeline import (
    BuildNdviPeriodStatsPipeline,
    BuildNdviPeriodStatsParams,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths


def main() -> None:
    p = argparse.ArgumentParser(
        description="Compute NDVI stats for a period composite (monthly or quarterly)."
    )
    p.add_argument(
        "--period",
        default=None,
        help="YYYY-MM or YYYY-Qn (e.g., 2025-08 or 2025-Q3). If omitted, compute for all existing composites.",
    )
    p.add_argument("--aoi-id", default="el522")
    p.add_argument(
        "--out",
        default=str(RepoPaths.table("ndvi_period_stats.csv")),
        help="Output CSV path (default: outputs/tables/ndvi_period_stats.csv)",
    )
    args = p.parse_args()

    out_csv = Path(args.out)

    pipe = BuildNdviPeriodStatsPipeline()

    if args.period:
        pipe.run(BuildNdviPeriodStatsParams(period=args.period, aoi_id=args.aoi_id, out_csv=out_csv))
    else:
        pipe.run_all_existing(aoi_id=args.aoi_id, out_csv=out_csv)


if __name__ == "__main__":
    main()
