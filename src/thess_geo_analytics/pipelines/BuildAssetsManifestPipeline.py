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


class BuildAssetsManifestPipeline:
    def __init__(self, builder: AssetsManifestBuilder | None = None) -> None:
        self.builder = builder or AssetsManifestBuilder()
        # Downloader created lazily only if needed
        self._downloader: Optional[CdseAssetDownloader] = None

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
        manifest_df.to_csv(out_path, index=False)

        print(f"[OK] Assets manifest exported → {out_path}")
        print(f"[OK] Scenes in manifest: {len(manifest_df)}")

        missing_hrefs = manifest_df[["href_b04", "href_b08", "href_scl"]].isna().any(axis=1).sum()
        if missing_hrefs:
            print(f"[WARN] {missing_hrefs} row(s) have missing hrefs (resolver mismatch)")

        if params.download_missing:
            self._download_and_validate(
                manifest_df,
                download_n=params.download_n,
                validate=params.validate_rasterio,
            )

        return out_path

    def _get_downloader(self) -> CdseAssetDownloader:
        if self._downloader is None:
            token = CdseTokenService()
            self._downloader = CdseAssetDownloader(token)
        return self._downloader

    def _download_and_validate(self, df: pd.DataFrame, *, download_n: int, validate: bool) -> None:
        import rasterio

        downloader = self._get_downloader()
        n = min(int(download_n), len(df))
        if n <= 0:
            print("[INFO] download_n <= 0 or empty manifest — skipping downloads.")
            return

        print(f"[INFO] Download+validate for first {n} scene(s)…")

        for i in tqdm(range(n), desc="Downloading S2 assets", unit="scene"):
            row = df.iloc[i]
            scene_id = row["scene_id"]

            if pd.isna(row["href_b04"]) or pd.isna(row["href_b08"]) or pd.isna(row["href_scl"]):
                print(f"[SKIP] {scene_id} missing href(s)")
                continue

            p_b04 = Path(row["local_b04"])
            p_b08 = Path(row["local_b08"])
            p_scl = Path(row["local_scl"])

            if not p_b04.exists():
                print(f"[DL] {scene_id} → B04.tif")
                downloader.download(str(row["href_b04"]), p_b04)
            if not p_b08.exists():
                print(f"[DL] {scene_id} → B08.tif")
                downloader.download(str(row["href_b08"]), p_b08)
            if not p_scl.exists():
                print(f"[DL] {scene_id} → SCL.tif")
                downloader.download(str(row["href_scl"]), p_scl)

            if validate:
                for p in (p_b04, p_b08, p_scl):
                    with rasterio.open(p) as ds:
                        _ = (ds.width, ds.height, ds.crs)  # simple sanity check

        print(f"[OK] Download+validate done for first {n} scene(s).")

    @staticmethod
    def smoke_test() -> None:
        print("=== BuildAssetsManifestPipeline Smoke Test ===")
        pipe = BuildAssetsManifestPipeline()
        out = pipe.run(
            BuildAssetsManifestParams(
                max_scenes=None,
                sort_mode="as_is",
                download_missing=False,
                out_name="assets_manifest_selected_smoke.csv",
            )
        )
        print("[OK] wrote:", out)
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildAssetsManifestPipeline.smoke_test()
