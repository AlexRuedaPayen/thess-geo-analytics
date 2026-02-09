from __future__ import annotations

import argparse

from thess_geo_analytics.pipelines.BuildNdviMonthlyStatsPipeline import (
    BuildNdviMonthlyStatsPipeline,
    BuildNdviMonthlyStatsParams,
)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("month", help="YYYY-MM")
    p.add_argument("--aoi-id", default="el522")
    args = p.parse_args()

    pipe = BuildNdviMonthlyStatsPipeline()
    pipe.run(BuildNdviMonthlyStatsParams(month=args.month, aoi_id=args.aoi_id))


if __name__ == "__main__":
    main()
