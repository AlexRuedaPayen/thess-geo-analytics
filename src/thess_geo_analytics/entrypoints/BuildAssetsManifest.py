from __future__ import annotations

import sys

from thess_geo_analytics.pipelines.BuildAssetsManifestPipeline import (
    BuildAssetsManifestPipeline,
    BuildAssetsManifestParams,
)


def _as_bool01(x: str) -> bool:
    """
    Accepts: "0/1", "true/false", "yes/no", "y/n" (case-insensitive).
    """
    x = x.strip().lower()
    if x in {"1", "true", "yes", "y"}:
        return True
    if x in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Expected boolean 0/1 or true/false, got: {x}")


def main() -> None:
    # Usage examples:
    #   python -m thess_geo_analytics.entrypoints.BuildAssetsManifest
    #   python -m thess_geo_analytics.entrypoints.BuildAssetsManifest 200 10
    #   python -m thess_geo_analytics.entrypoints.BuildAssetsManifest 200 10 2020-01-01 2026-02-01 cloud_then_time assets_manifest.csv
    #
    # With GCP options:
    #   python -m thess_geo_analytics.entrypoints.BuildAssetsManifest 999999 999999 None None as_is assets_manifest_selected.csv 1 thess-geo-analytics raw_s2 C:/Users/alexr/.gcp/thess-geo-analytics-nvdi.json 0
    #
    # Args:
    #   1) max_scenes (optional, default None -> all)
    #   2) download_n (optional, default 3)
    #   3) date_start YYYY-MM-DD or "None" (optional)
    #   4) date_end   YYYY-MM-DD or "None" (optional)
    #   5) sort_mode  (optional: as_is | cloud_then_time | time) default as_is
    #   6) out_name   (optional) default assets_manifest_selected.csv
    #   7) upload_to_gcs (optional: 0/1, true/false, yes/no) default 0
    #   8) gcs_bucket (optional, required if upload_to_gcs=1)
    #   9) gcs_prefix (optional, default "raw_s2")
    #  10) gcs_credentials (optional path or "None" for default auth)
    #  11) delete_local_after_upload (optional: 0/1, default 0)
    #  12) raw_storage_mode  validate value in {"url_to_local", "url_to_gcs_keep_local", "url_to_gcs_drop_local", "gcs_to_local"}

    max_scenes = int(sys.argv[1]) if len(sys.argv) > 1 else None
    download_n = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    date_start = str(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] != "None" else None
    date_end = str(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] != "None" else None
    sort_mode = str(sys.argv[5]) if len(sys.argv) > 5 else "as_is"
    out_name = str(sys.argv[6]) if len(sys.argv) > 6 else "assets_manifest_selected.csv"

    upload_to_gcs = _as_bool01(sys.argv[7]) if len(sys.argv) > 7 else False
    gcs_bucket = str(sys.argv[8]) if len(sys.argv) > 8 and sys.argv[8] != "None" else None
    gcs_prefix = str(sys.argv[9]) if len(sys.argv) > 9 and sys.argv[9] != "None" else "raw_s2"
    gcs_credentials = str(sys.argv[10]) if len(sys.argv) > 10 and sys.argv[10] != "None" else None
    delete_local_after_upload = _as_bool01(sys.argv[11]) if len(sys.argv) > 11 else False
    raw_storage_mode = str(sys.argv[12]) if len(sys.argv) > 12 else "url_to_local"

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
            upload_to_gcs=upload_to_gcs,
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix,
            gcs_credentials=gcs_credentials,
            delete_local_after_upload=delete_local_after_upload,
            raw_storage_mode=raw_storage_mode,
        )
    )


if __name__ == "__main__":
    main()
