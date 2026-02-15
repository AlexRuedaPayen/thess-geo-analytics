from __future__ import annotations

import argparse

from thess_geo_analytics.pipelines.BuildNdviTimeSeriesPipeline import (
    BuildNdviTimeSeriesPipeline,
    BuildNdviTimeSeriesParams,
)


def main() -> None:
    p = argparse.ArgumentParser(description="Build NDVI monthly time series + plot")
    p.add_argument("--aoi-id", default="el522", help="AOI identifier (default: el522)")
    args = p.parse_args()

    pipe = BuildNdviTimeSeriesPipeline()
    pipe.run(BuildNdviTimeSeriesParams(aoi_id=args.aoi_id))


if __name__ == "__main__":
    main()
