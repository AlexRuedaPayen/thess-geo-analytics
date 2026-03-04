from __future__ import annotations

import argparse

from thess_geo_analytics.pipelines.BuildAssetsManifestPipeline import (
    BuildAssetsManifestPipeline,
    BuildAssetsManifestParams,
)

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.utils.log_parameters import log_parameters

# Optional types for injection
from thess_geo_analytics.services.CdseStacService import CdseStacService
from thess_geo_analytics.services.CdseAssetDownloader import CdseAssetDownloader
from thess_geo_analytics.builders.AssetsManifestBuilder import AssetsManifestBuilder


ALLOWED_RAW_STORAGE_MODES = {
    "url_to_local",
}


PARAMETER_DOCS = {
    "max_scenes": "Max scenes in manifest (None = all scenes).",
    "date_start": "Earliest acquisition date YYYY-MM-DD.",
    "sort_mode": "Ordering before capping max_scenes.",
    "download_n": "Number of scenes to download & validate (0 = none).",
    "download_missing": "If False, skip downloads (manifest only).",
    "validate_rasterio": "Open each download with rasterio to validate.",
    "out_name": "Output CSV filename for assets manifest.",
    "raw_storage_mode": "How raw bands are retrieved.",
    "band_resolution": "Target band resolution in metres (10 or 20).",
    "max_download_workers": "Max concurrent downloads.",
}


# --------------------------------------------------------------
# Injectable runner (used by tests)
# --------------------------------------------------------------
def run(
    *,
    builder: AssetsManifestBuilder | None = None,
    stac_service: CdseStacService | None = None,
    asset_downloader: CdseAssetDownloader | None = None,
) -> None:

    cfg = load_pipeline_config()
    ms = cfg.mode_settings

    am_raw = cfg.assets_manifest_params
    am_cfg = cfg.effective_assets_manifest_params

    pipeline_date_start = cfg.raw["pipeline"]["date_start"]

    band_res = ms.effective_band_resolution(am_raw)
    max_workers_default = ms.effective_max_download_workers()

    effective_download_n = am_cfg.get("download_n", 9999)

    params = BuildAssetsManifestParams(
        max_scenes=am_cfg.get("max_scenes"),
        date_start=pipeline_date_start,
        sort_mode=am_cfg.get("sort_mode", "cloud_then_time"),
        download_n=effective_download_n,
        download_missing=True,
        validate_rasterio=True,
        out_name=am_cfg.get("out_table", "assets_manifest_selected.csv"),
        raw_storage_mode=am_cfg.get("raw_storage_mode", "url_to_local"),
        band_resolution=band_res,
        max_download_workers=max_workers_default,
    )

    extra = {
        "mode": ms.mode,
        "region": cfg.region_name,
        "aoi_id": cfg.aoi_id,
    }

    log_parameters("BuildAssetsManifest", params, PARAMETER_DOCS, extra)

    pipe = BuildAssetsManifestPipeline(
        builder=builder,
        downloader=asset_downloader,
        stac_service=stac_service,
    )

    pipe.run(params)


# --------------------------------------------------------------
# CLI
# --------------------------------------------------------------
def main() -> None:

    cfg = load_pipeline_config()
    ms = cfg.mode_settings

    am_raw = cfg.assets_manifest_params
    am_cfg = cfg.effective_assets_manifest_params

    pipeline_date_start = cfg.raw["pipeline"]["date_start"]

    band_res = ms.effective_band_resolution(am_raw)
    max_workers_default = ms.effective_max_download_workers()

    p = argparse.ArgumentParser(
        description="Build assets_manifest_selected.csv from scenes_selected.csv"
    )

    p.add_argument(
        "--max-scenes",
        type=int,
        default=am_cfg.get("max_scenes"),
    )

    p.add_argument(
        "--date-start",
        default=pipeline_date_start,
    )

    p.add_argument(
        "--sort-mode",
        default=am_cfg.get("sort_mode", "cloud_then_time"),
        choices=["as_is", "cloud_then_time", "time"],
    )

    p.add_argument(
        "--download-n",
        type=int,
        default=am_cfg.get("download_n", 9999),
    )

    p.add_argument(
        "--no-download",
        action="store_true",
    )

    p.add_argument(
        "--band-resolution",
        type=int,
        default=band_res,
    )

    p.add_argument(
        "--max-download-workers",
        type=int,
        default=max_workers_default,
    )

    p.add_argument(
        "--out-name",
        default=am_cfg.get("out_table", "assets_manifest_selected.csv"),
    )

    p.add_argument(
        "--raw-storage-mode",
        default=am_cfg.get("raw_storage_mode", "url_to_local"),
        choices=sorted(ALLOWED_RAW_STORAGE_MODES),
    )

    args = p.parse_args()

    effective_download_n = 0 if args.no_download else args.download_n

    params = BuildAssetsManifestParams(
        max_scenes=args.max_scenes,
        date_start=args.date_start,
        sort_mode=args.sort_mode,
        download_n=effective_download_n,
        download_missing=not args.no_download,
        validate_rasterio=True,
        out_name=args.out_name,
        raw_storage_mode=args.raw_storage_mode,
        band_resolution=args.band_resolution,
        max_download_workers=args.max_download_workers,
    )

    log_parameters("BuildAssetsManifest", params, PARAMETER_DOCS)

    pipe = BuildAssetsManifestPipeline()

    pipe.run(params)


if __name__ == "__main__":
    main()