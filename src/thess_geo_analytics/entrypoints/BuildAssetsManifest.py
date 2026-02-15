from __future__ import annotations

import sys

from thess_geo_analytics.pipelines.BuildAssetsManifestPipeline import (
    BuildAssetsManifestPipeline,
    BuildAssetsManifestParams,
)


def main() -> None:
    # Usage examples:
    #   python -m thess_geo_analytics.entrypoints.BuildAssetsManifest
    #   python -m thess_geo_analytics.entrypoints.BuildAssetsManifest 200 10
    #   python -m thess_geo_analytics.entrypoints.BuildAssetsManifest 200 10 2020-01-01 2026-02-01 cloud_then_time assets_manifest.csv
    #
    # Args:
    #   1) max_scenes (optional, default None -> all)
    #   2) download_n (optional, default 3)
    #   3) date_start YYYY-MM-DD (optional)
    #   4) date_end   YYYY-MM-DD (optional)
    #   5) sort_mode  (optional: as_is | cloud_then_time | time) default as_is
    #   6) out_name   (optional) default assets_manifest_selected.csv

    max_scenes = int(sys.argv[1]) if len(sys.argv) > 1 else None
    download_n = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    date_start = str(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] != "None" else None
    date_end = str(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] != "None" else None
    sort_mode = str(sys.argv[5]) if len(sys.argv) > 5 else "as_is"
    out_name = str(sys.argv[6]) if len(sys.argv) > 6 else "assets_manifest_selected.csv"

    if sort_mode not in {"as_is", "cloud_then_time", "time"}:
        raise SystemExit("sort_mode must be one of: as_is | cloud_then_time | time")

    pipe = BuildAssetsManifestPipeline()
    pipe.run(
        BuildAssetsManifestParams(
            max_scenes=max_scenes,
            date_start=date_start,
            date_end=date_end,
            sort_mode=sort_mode,  # type: ignore[arg-type]
            download_n=download_n,
            download_missing=True,
            validate_rasterio=True,
            out_name=out_name,
        )
    )


if __name__ == "__main__":
    main()
