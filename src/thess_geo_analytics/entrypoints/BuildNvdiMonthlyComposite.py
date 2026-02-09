from __future__ import annotations

import argparse

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.pipelines.BuildNdviMonthlyCompositePipeline import (
    BuildNdviMonthlyCompositePipeline,
    BuildNdviMonthlyCompositeParams,
)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("month", help="YYYY-MM")
    p.add_argument("--aoi", default="EL522_thessaloniki.geojson", help="AOI geojson filename in /aoi")
    p.add_argument("--aoi-id", default="el522")
    args = p.parse_args()

    aoi_path = RepoPaths.aoi(args.aoi)

    pipe = BuildNdviMonthlyCompositePipeline()
    out_tif, out_png = pipe.run(BuildNdviMonthlyCompositeParams(month=args.month, aoi_path=aoi_path, aoi_id=args.aoi_id))

    print("[OK] Monthly NDVI written:", out_tif)
    print("[OK] Preview written:     ", out_png)


if __name__ == "__main__":
    main()
