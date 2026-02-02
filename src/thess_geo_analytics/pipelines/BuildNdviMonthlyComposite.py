from __future__ import annotations

import sys
from dotenv import load_dotenv

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.pipelines.NdviMonthlyCompositePipeline import NdviMonthlyCompositePipeline, NdviMonthlyParams

class BuildNdviMonthlyComposite:
    def init(self) -> None:
        pass

    def run(self) -> None:
        load_dotenv()

        if len(sys.argv) < 2:
            raise SystemExit("Usage: python -m thess_geo_analytics.pipelines.BuildNdviMonthlyComposite YYYY-MM")

        month = sys.argv[1]
        aoi_path = RepoPaths.aoi("EL522_thessaloniki.geojson")  

        NdviMonthlyCompositePipeline(aoi_path, NdviMonthlyParams(month=month)).run()

if __name__ == "__main__":
    BuildNdviMonthlyComposite().run()