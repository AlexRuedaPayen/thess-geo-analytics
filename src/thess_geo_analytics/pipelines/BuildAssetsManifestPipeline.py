from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal

import pandas as pd
from tqdm import tqdm

from thess_geo_analytics.builders.AssetsManifestBuilder import (
    AssetsManifestBuilder,
    AssetsManifestBuildParams,
)
from thess_geo_analytics.services.CdseTokenService import CdseTokenService
from thess_geo_analytics.services.CdseAssetDownloader import CdseAssetDownloader
from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.utils.GcsClient import GcsClient

SortMode = Literal["as_is", "cloud_then_time", "time"]


@dataclass(frozen=True)
class BuildAssetsManifestParams:
    # Selection scope (applies on scenes_selected.csv)
    max_scenes: Optional[int] = None
    date_start: Optional[str] = None  # "YYYY-MM-DD"
    date_end: Optional[str] = None    # "YYYY-MM-DD"
    sort_mode: SortMode = "as_is"

    # Downloading
    download_n: int = 3                 # how many rows to actually download
    download_missing: bool = True       # whether to trigger download at all
    validate_rasterio: bool = True      # open with rasterio after download

    # Output naming
    out_name: str = "assets_manifest_selected.csv"

    # Optional GCP upload of raw bands
    upload_to_gcs: bool = False
    gcs_bucket: Optional[str] = None
    gcs_prefix: str = "raw_s2"          # e.g. raw_s2/<scene_id>/B04.tif
    gcs_credentials: Optional[str] = None
    delete_local_after_upload: bool = False


class BuildAssetsManifestPipeline:
    def __init__(self, builder: AssetsManifestBuilder | None = None) -> None:
        self.builder = builder or AssetsManifestBuilder()

        # Created lazily only if needed
        self._downloader: Optional[CdseAssetDownloader] = None
        self._gcs: Optional[GcsClient] = None

    # -------------------
    # Public API
    # -------------------
    def run(self, params: BuildAssetsManifestParams) -> Path:
        scenes_csv = RepoPaths.table("scenes_selected.csv")
        if not scenes_csv.exists():
            raise FileNotFoundError(f"Missing scenes_selected: {scenes_csv}")

        scenes_df = pd.read_csv(scenes_csv)

        build_params = AssetsManifestBuildParams(
            max_scenes=params.max_scenes,
            date_start=params.date_start,
            date_end=params.date_end,
            sort_mode=params.sort_mode,
        )

        manifest_df = self.builder.build_assets_manifest_df(scenes_df, build_params)

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)
        out_path = RepoPaths.table(params.out_name)

        print(f"[OK] Assets manifest created in memory — rows: {len(manifest_df)}")

        missing_hrefs = manifest_df[["href_b04", "href_b08", "href_scl"]].isna().any(axis=1).sum()
        if missing_hrefs:
            print(f"[WARN] {missing_hrefs} row(s) have missing hrefs (resolver mismatch)")

        # Download + validate + optional GCS upload
        if params.download_missing and len(manifest_df) > 0:
            manifest_df = self._download_and_validate(
                manifest_df,
                params=params,
            )

        # Save final manifest (possibly with gcs_* columns added)
        manifest_df.to_csv(out_path, index=False)
        print(f"[OK] Assets manifest exported → {out_path}")
        print(f"[OK] Scenes in manifest: {len(manifest_df)}")

        return out_path

    # -------------------
    # Internals
    # -------------------
    def _get_downloader(self) -> CdseAssetDownloader:
        if self._downloader is None:
            token = CdseTokenService()
            self._downloader = CdseAssetDownloader(token)
        return self._downloader

    def _get_gcs(self, params: BuildAssetsManifestParams) -> GcsClient:
        if self._gcs is None:
            if not params.gcs_bucket:
                raise ValueError("upload_to_gcs=True but gcs_bucket is not set")
            self._gcs = GcsClient(
                bucket=params.gcs_bucket,
                credentials=params.gcs_credentials,
            )
        return self._gcs

    def _download_and_validate(
        self,
        df: pd.DataFrame,
        *,
        params: BuildAssetsManifestParams,
    ) -> pd.DataFrame:
        import rasterio

        downloader = self._get_downloader()
        gcs: Optional[GcsClient] = None
        if params.upload_to_gcs:
            gcs = self._get_gcs(params)

        df = df.copy()

        n = min(int(params.download_n), len(df))
        if n <= 0:
            print("[INFO] download_n <= 0 or empty manifest — skipping downloads.")
            return df

        print(f"[INFO] Download+validate for first {n} scene(s)…")

        for i in tqdm(range(n), desc="Downloading S2 assets", unit="scene"):
            row = df.iloc[i]
            scene_id = row["scene_id"]

            # Skip rows with missing hrefs
            if pd.isna(row["href_b04"]) or pd.isna(row["href_b08"]) or pd.isna(row["href_scl"]):
                print(f"[SKIP] {scene_id} missing href(s)")
                continue

            p_b04 = Path(row["local_b04"])
            p_b08 = Path(row["local_b08"])
            p_scl = Path(row["local_scl"])

            # Download if missing; CdseAssetDownloader itself should avoid re-downloading
            try:
                if not p_b04.exists():
                    #print(f"[DL] {scene_id} → B04.tif")
                    downloader.download(str(row["href_b04"]), p_b04)
                if not p_b08.exists():
                    #print(f"[DL] {scene_id} → B08.tif")
                    downloader.download(str(row["href_b08"]), p_b08)
                if not p_scl.exists():
                    #print(f"[DL] {scene_id} → SCL.tif")
                    downloader.download(str(row["href_scl"]), p_scl)
            except Exception as e:
                # Only fatal if the very first download fails (handled inside downloader);
                # here we treat any raised exception as fatal for the whole run.
                print(f"[ERROR] Fatal error downloading {scene_id}: {e}")
                raise

            # If validation requested but some files still missing, skip this scene
            if params.validate_rasterio:
                if not (p_b04.exists() and p_b08.exists() and p_scl.exists()):
                    print(f"[WARN] {scene_id} missing one or more local files after download — skipping validation.")
                else:
                    try:
                        for p in (p_b04, p_b08, p_scl):
                            with rasterio.open(p) as ds:
                                _ = (ds.width, ds.height, ds.crs)
                    except Exception as e:
                        print(f"[WARN] Validation failed for {scene_id} ({e}) — skipping this scene.")
                        # We do NOT raise; continue with next scene

            # Optional GCS upload (only if all three files exist)
            if gcs is not None and p_b04.exists() and p_b08.exists() and p_scl.exists():
                base_prefix = f"{params.gcs_prefix}/{scene_id}"
                remote_b04 = f"{base_prefix}/B04.tif"
                remote_b08 = f"{base_prefix}/B08.tif"
                remote_scl = f"{base_prefix}/SCL.tif"

                try:
                    url_b04 = gcs.upload(p_b04, remote_b04)
                    url_b08 = gcs.upload(p_b08, remote_b08)
                    url_scl = gcs.upload(p_scl, remote_scl)

                    df.at[row.name, "gcs_b04"] = url_b04
                    df.at[row.name, "gcs_b08"] = url_b08
                    df.at[row.name, "gcs_scl"] = url_scl

                    print(f"[OK] GCS upload {scene_id} B04 → {url_b04}")
                    print(f"[OK] GCS upload {scene_id} B08 → {url_b08}")
                    print(f"[OK] GCS upload {scene_id} SCL → {url_scl}")

                    if params.delete_local_after_upload:
                        for p in (p_b04, p_b08, p_scl):
                            if p.exists():
                                p.unlink()
                        print(f"[INFO] Deleted local copies for {scene_id} after successful GCS upload.")
                except Exception as e:
                    print(f"[WARN] GCS upload failed for {scene_id} ({e}) — keeping local files.")

        print(f"[OK] Download+validate done for first {n} scene(s).")
        return df

    # -------------------
    # Smoke test
    # -------------------
    @staticmethod
    def smoke_test() -> None:
        print("=== BuildAssetsManifestPipeline Smoke Test ===")
        pipe = BuildAssetsManifestPipeline()
        out = pipe.run(
            BuildAssetsManifestParams(
                max_scenes=None,
                sort_mode="as_is",
                download_missing=False,  # no downloads in smoke
                out_name="assets_manifest_selected_smoke.csv",
            )
        )
        print("[OK] wrote:", out)
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildAssetsManifestPipeline.smoke_test()
