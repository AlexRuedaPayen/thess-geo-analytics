from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from thess_geo_analytics.builders.MonthlyCompositeBuilder import (
    MonthlyCompositeBuilder,
    MonthlyCompositeConfig,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildNdviMonthlyCompositeParams:
    aoi_path: Path
    aoi_id: str = "el522"

    month: Optional[str] = None  # if provided, run only that month (no fallback)
    time_serie_csv: Path = RepoPaths.table("scenes_selected.csv")
    assets_manifest_csv: Path = RepoPaths.table("assets_manifest_selected.csv")

    max_scenes_per_period: Optional[int] = None
    download_missing: bool = True
    verbose: bool = False

    min_scenes_per_month: int = 2
    fallback_to_quarterly: bool = True

    # ------------------------
    # RAW STORAGE (input side)
    # ------------------------
    # Same modes as RawAssetStorageManager:
    #   - "url_to_local"
    #   - "url_to_gcs_keep_local"
    #   - "url_to_gcs_drop_local"
    #   - "gcs_to_local"
    raw_storage_mode: str = "url_to_local"

    # Shared GCS config (for raw + composites)
    gcs_bucket: Optional[str] = None
    gcs_credentials: Optional[str] = None
    gcs_prefix_raw: str = "raw_s2"

    # ------------------------
    # COMPOSITE STORAGE (output side)
    # ------------------------
    upload_composites_to_gcs: bool = False
    gcs_prefix_composites: str = "ndvi/composites"


class BuildNdviMonthlyCompositePipeline:
    """
    Orchestrates MonthlyCompositeBuilder.

    Returns:
      list of (label, out_tif, out_png)
      where label is YYYY-MM or YYYY-Qn.
    """

    def run(self, params: BuildNdviMonthlyCompositeParams) -> list[tuple[str, Path, Path]]:
        cfg = MonthlyCompositeConfig(
            max_scenes=params.max_scenes_per_period,
            verbose=params.verbose,
            download_missing=params.download_missing,
            min_scenes_per_month=params.min_scenes_per_month,
            fallback_to_quarterly=params.fallback_to_quarterly,
            # raw storage / GCS
            raw_storage_mode=params.raw_storage_mode,          # type: ignore[arg-type]
            gcs_bucket=params.gcs_bucket,
            gcs_credentials=params.gcs_credentials,
            gcs_prefix_raw=params.gcs_prefix_raw,
            # composite uploads
            upload_composites_to_gcs=params.upload_composites_to_gcs,
            gcs_prefix_composites=params.gcs_prefix_composites,
        )

        builder = MonthlyCompositeBuilder(
            aoi_path=params.aoi_path,
            aoi_id=params.aoi_id,
            cfg=cfg,
            time_serie_csv=params.time_serie_csv,
            assets_manifest_csv=params.assets_manifest_csv,
        )

        if params.month:
            out_tif, out_png = builder.run_month(params.month)
            return [(params.month, out_tif, out_png)]

        results = builder.run_all_periods()
        # builder returns list[(label, tif, png)] already
        return results

    @staticmethod
    def smoke_test() -> None:
        print("=== BuildNdviMonthlyCompositePipeline Smoke Test ===")
        print("[SKIP] Run via entrypoint with real AOI + existing time_serie.csv + assets_manifest_selected.csv.")
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildNdviMonthlyCompositePipeline.smoke_test()
