from __future__ import annotations

import sys
from thess_geo_analytics.pipelines.ExtractAoiPipeline import ExtractAoiPipeline


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m thess_geo_analytics.entrypoints.ExtractAoi"
        " <RegionName>")

    region_name = sys.argv[1]
    ExtractAoiPipeline().run(region_name)


if __name__ == "__main__":
    main()
