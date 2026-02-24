from __future__ import annotations

import argparse

from thess_geo_analytics.pipelines.BuildAssetsManifestPipeline import (
    BuildAssetsManifestPipeline,
    BuildAssetsManifestParams,
)
from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.utils.log_parameters import log_parameters

ALLOWED_RAW_STORAGE_MODES = {
    "url_to_local",
    "url_to_gcs_keep_local",
    "url_to_gcs_drop_local",
    "gcs_to_local",
}

PARAMETER_DOCS = {
    "max_scenes": "Max scenes in manifest (None = all scenes).",
    "date_start": "Earliest acquisition date YYYY-MM-DD (global pipeline date).",
    "sort_mode": "Ordering before capping max_scenes.",
    "download_n": "Number of scenes to download & validate (0 = none).",
    "download_missing": "If False, skip all downloads (manifest only).",
    "validate_rasterio": "Open each download with rasterio to validate.",
    "out_name": "Output CSV filename for assets manifest.",
    "upload_to_gcs": "Whether to upload raw bands to GCS.",
    "gcs_bucket": "GCS bucket for raw bands.",
    "gcs_prefix": "Prefix inside bucket for raw bands.",
    "gcs_credentials": "Path to GCS credentials JSON (optional).",
    "delete_local_after_upload": "Delete local files after upload if True.",
    "raw_storage_mode": "How raw bands are handled (local/GCS).",
    "band_resolution": "Target band resolution in metres (e.g. 10 or 20).",
    "max_download_workers": "Max concurrent downloads.",
}


def _as_bool01(x: str) -> bool:
    x = x.strip().lower()
    if x in {"1", "true", "yes", "y"}:
        return True
    if x in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Expected boolean 0/1 or true/false, got: {x}")


def main() -> None:
    """
    Simplified CLI for BuildAssetsManifest.

    Defaults are read from config/pipeline.thess.yaml
    and can be overridden via optional flags.

    - Temporal extent comes from pipeline.date_start (single global knob).
    - Other defaults come from assets_manifest section (possibly mode-adjusted).
    """
    cfg = load_pipeline_config()
    ms = cfg.mode_settings

    am_raw = cfg.assets_manifest_params
    am_cfg = cfg.effective_assets_manifest_params

    # global date_start (single source of truth)
    pipeline_date_start = cfg.raw["pipeline"]["date_start"]

    band_res = ms.effective_band_resolution(am_raw)
    max_workers_default = ms.effective_max_download_workers()

    p = argparse.ArgumentParser(
        description="Build assets_manifest_selected.csv from scenes_selected.csv"
    )

    # ---- high-value knobs ----
    p.add_argument(
        "--max-scenes",
        type=int,
        default=am_cfg.get("max_scenes"),
        help="Max scenes in manifest (default from YAML+mode, None=all).",
    )
    p.add_argument(
        "--date-start",
        default=pipeline_date_start,
        help="Earliest acquisition date YYYY-MM-DD (default from pipeline.date_start).",
    )
    p.add_argument(
        "--sort-mode",
        default=am_cfg.get("sort_mode", "cloud_then_time"),
        choices=["as_is", "cloud_then_time", "time"],
        help="Ordering before capping max_scenes.",
    )
    p.add_argument(
        "--download-n",
        type=int,
        default=am_cfg.get("download_n", 9999),
        help="Number of scenes to download & validate (0 = none).",
    )
    p.add_argument(
        "--no-download",
        action="store_true",
        help="Skip all downloads (manifest only).",
    )
    p.add_argument(
        "--band-resolution",
        type=int,
        default=band_res,
        help="Target NDVI resolution in metres (10 or 20).",
    )
    p.add_argument(
        "--max-download-workers",
        type=int,
        default=max_workers_default,
        help=(
            "Max concurrent downloads "
            "(default mode-dependent; env THESS_MAX_DOWNLOAD_WORKERS overrides)."
        ),
    )
    p.add_argument(
        "--out-name",
        default=am_cfg.get("out_table", "assets_manifest_selected.csv"),
        help="Output CSV filename (under outputs/tables).",
    )
    p.add_argument(
        "--raw-storage-mode",
        default=am_cfg.get("raw_storage_mode", "url_to_local"),
        choices=sorted(ALLOWED_RAW_STORAGE_MODES),
        help="How raw bands are handled (local/GCS).",
    )
    p.add_argument(
        "--upload-to-gcs",
        default=str(am_cfg.get("upload_to_gcs", False)),
        help="Upload raw bands to GCS (0/1, true/false).",
    )
    p.add_argument(
        "--gcs-bucket",
        default=am_cfg.get("gcs_bucket"),
        help="GCS bucket for raw bands.",
    )
    p.add_argument(
        "--gcs-prefix",
        default=am_cfg.get("gcs_prefix", "raw_s2"),
        help="Prefix in bucket for raw bands.",
    )
    p.add_argument(
        "--gcs-credentials",
        default=am_cfg.get("gcs_credentials"),
        help="Path to service account JSON (optional).",
    )
    p.add_argument(
        "--delete-local-after-upload",
        default=str(am_cfg.get("delete_local_after_upload", False)),
        help="Delete local files after upload (0/1, true/false).",
    )

    args = p.parse_args()

    upload_to_gcs = _as_bool01(args.upload_to_gcs)
    delete_local_after_upload = _as_bool01(args.delete_local_after_upload)

    if upload_to_gcs and not args.gcs_bucket:
        raise SystemExit("upload_to_gcs=True but no --gcs-bucket set (nor in YAML).")

    # effective download_n: CLI still wins
    effective_download_n = 0 if args.no_download else args.download_n

    params = BuildAssetsManifestParams(
        max_scenes=args.max_scenes,
        date_start=args.date_start,
        sort_mode=args.sort_mode,  # type: ignore[arg-type]
        download_n=effective_download_n,
        download_missing=not args.no_download,
        validate_rasterio=True,
        out_name=args.out_name,
        upload_to_gcs=upload_to_gcs,
        gcs_bucket=args.gcs_bucket,
        gcs_prefix=args.gcs_prefix,
        gcs_credentials=args.gcs_credentials,
        delete_local_after_upload=delete_local_after_upload,
        raw_storage_mode=args.raw_storage_mode,  # type: ignore[arg-type]
        band_resolution=args.band_resolution,
        max_download_workers=args.max_download_workers,
    )

    extra = {
        "mode": ms.mode,
        "region": cfg.region_name,
        "aoi_id": cfg.aoi_id,
    }
    log_parameters("BuildAssetsManifest", params, PARAMETER_DOCS, extra)

    pipe = BuildAssetsManifestPipeline()
    pipe.run(params)


if __name__ == "__main__":
    main()