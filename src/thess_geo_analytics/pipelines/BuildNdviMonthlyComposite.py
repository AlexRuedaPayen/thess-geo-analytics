from __future__ import annotations

import sys
from dotenv import load_dotenv

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.geo.MonthlyCompositeBuilder import MonthlyCompositeBuilder


class BuildNdviMonthlyComposite:
    def run(self) -> None:
        load_dotenv()

        if len(sys.argv) < 2:
            raise SystemExit("Usage: python -m thess_geo_analytics.pipelines.BuildNdviMonthlyComposite YYYY-MM")

        month = sys.argv[1]
        aoi_path = RepoPaths.aoi("EL522_thessaloniki.geojson")

        builder = MonthlyCompositeBuilder(aoi_path=aoi_path, aoi_id="el522")
        out_tif, out_png = builder.run(month)

        print(f"Monthly NDVI written: {out_tif}")
        print(f"Preview written:      {out_png}")


if __name__ == "__main__":
    BuildNdviMonthlyComposite().run()

