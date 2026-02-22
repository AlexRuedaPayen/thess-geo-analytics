from __future__ import annotations

import argparse

from thess_geo_analytics.pipelines.BuildAssetsManifestPipeline import (
    BuildAssetsManifestPipeline,
    BuildAssetsManifestParams,
)
from thess_geo_analytics.core.pipeline_config import load_pipeline_config

ALLOWED_RAW_STORAGE_MODES = {
    "url_to_local",
    "url_to_gcs_keep_local",
    "url_to_gcs_drop_local",
    "gcs_to_local",
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

    Defaults are read from config/pipeline.thess.yaml (assets_manifest section),
    and can be overridden via optional flags.
    """
    cfg = load_pipeline_config()
    am_cfg = cfg.assets_manifest_params  # dict from your PipelineConfig

    p = argparse.ArgumentParser(
        description="Build assets_manifest_selected.csv from scenes_selected.csv"
    )

    # ---- high-value knobs (others from YAML) ----
    p.add_argument(
        "--max-scenes",
        type=int,
        default=am_cfg.get("max_scenes"),
        help="Max scenes in manifest (default from YAML, None=all).",
    )
    p.add_argument(
        "--date-start",
        default=am_cfg.get("date_start"),
        help='Start date YYYY-MM-DD (default from YAML).',
    )
    p.add_argument(
        "--date-end",
        default=am_cfg.get("date_end"),
        help='End date YYYY-MM-DD (default from YAML).',
    )
    p.add_argument(
        "--sort-mode",
        default=am_cfg.get("sort_mode", "as_is"),
        choices=["as_is", "cloud_then_time", "time"],
        help="Ordering before capping max_scenes.",
    )
    p.add_argument(
        "--download-n",
        type=int,
        default=am_cfg.get("download_n", 3),
        help="Number of scenes to download & validate (0 = none).",
    )
    p.add_argument(
        "--no-download",
        action="store_true",
        help="Skip all downloads (manifest only).",
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

    # effective download_n
    effective_download_n = 0 if args.no_download else args.download_n

    pipe = BuildAssetsManifestPipeline()
    pipe.run(
        BuildAssetsManifestParams(
            max_scenes=args.max_scenes,
            date_start=args.date_start,
            date_end=args.date_end,
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
        )
    )


if __name__ == "__main__":
    main()