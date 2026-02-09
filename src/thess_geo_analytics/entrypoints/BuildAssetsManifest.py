from __future__ import annotations

import sys

from thess_geo_analytics.pipelines.BuildAssetsManifestPipeline import (
    BuildAssetsManifestPipeline,
    BuildAssetsManifestParams,
)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python -m thess_geo_analytics.entrypoints.BuildAssetsManifest YYYY-MM [max_scenes] [download_n]"
        )

    month = sys.argv[1]
    max_scenes = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    download_n = int(sys.argv[3]) if len(sys.argv) > 3 else 3

    pipe = BuildAssetsManifestPipeline()
    pipe.run(
        BuildAssetsManifestParams(
            month=month,
            max_scenes=max_scenes,
            download_n=download_n,
            download_missing=True,
            validate_rasterio=True,
        )
    )


if __name__ == "__main__":
    main()
