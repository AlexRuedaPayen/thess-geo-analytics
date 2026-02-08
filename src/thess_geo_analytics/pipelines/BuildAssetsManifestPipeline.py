from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from thess_geo_analytics.builders.AssetsManifestBuilder import (
    AssetsManifestBuilder,
    AssetsManifestBuildParams,
)
from thess_geo_analytics.services.CdseTokenService import CdseTokenService
from thess_geo_analytics.services.CdseAssetDownloader import CdseAssetDownloader
from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildAssetsManifestParams:
    month: str
    max_scenes: int = 10
    download_n: int = 3
    download_missing: bool = True
    validate_rasterio: bool = True


class BuildAssetsManifestPipeline:
    def __init__(self, builder: AssetsManifestBuilder | None = None) -> None:
        self.builder = builder or AssetsManifestBuilder()

        # downloader created lazily only if needed
        self._downloader: Optional[CdseAssetDownloader] = None

    def run(self, params: BuildAssetsManifestParams) -> Path:
        scenes_csv = RepoPaths.table("scenes_catalog.csv")
        if not scenes_csv.exists():
            raise FileNotFoundError(f"Missing scenes catalog: {scenes_csv}")

        scenes_df = pd.read_csv(scenes_csv)

        build_params = AssetsManifestBuildParams(
            month=params.month,
            max_scenes=params.max_scenes,
        )

        manifest_df = self.builder.build_assets_manifest_df(scenes_df, build_params)

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)
        out_path = RepoPaths.table(f"assets_manifest_{params.month}.csv")
        manifest_df.to_csv(out_path, index=False)

        print(f"[OK] Assets manifest exported => {out_path}")
        print(f"[OK] Scenes in manifest: {len(manifest_df)}")

        missing_hrefs = manifest_df[["href_b04", "href_b08", "href_scl"]].isna().any(axis=1).sum()
        if missing_hrefs:
            print(f"[WARN] {missing_hrefs} rows have missing hrefs (resolver mismatch)")

        if params.download_missing:
            self._download_and_validate(manifest_df, download_n=params.download_n, validate=params.validate_rasterio)

        return out_path

    def _get_downloader(self) -> CdseAssetDownloader:
        if self._downloader is None:
            token = CdseTokenService()
            self._downloader = CdseAssetDownloader(token)
        return self._downloader

    def _download_and_validate(self, df: pd.DataFrame, *, download_n: int, validate: bool) -> None:
        import rasterio
        from pathlib import Path

        downloader = self._get_downloader()
        n = min(download_n, len(df))

        for i in range(n):
            row = df.iloc[i]
            scene_id = row["scene_id"]

            if pd.isna(row["href_b04"]) or pd.isna(row["href_b08"]) or pd.isna(row["href_scl"]):
                print(f"[SKIP] {scene_id} missing href(s)")
                continue

            p_b04 = Path(row["local_b04"])
            p_b08 = Path(row["local_b08"])
            p_scl = Path(row["local_scl"])

            if not p_b04.exists():
                print(f"[DL] {scene_id} -> B04.tif")
                downloader.download(str(row["href_b04"]), p_b04)
            if not p_b08.exists():
                print(f"[DL] {scene_id} -> B08.tif")
                downloader.download(str(row["href_b08"]), p_b08)
            if not p_scl.exists():
                print(f"[DL] {scene_id} -> SCL.tif")
                downloader.download(str(row["href_scl"]), p_scl)

            if validate:
                for p in [p_b04, p_b08, p_scl]:
                    with rasterio.open(p) as ds:
                        _ = (ds.width, ds.height, ds.crs)

        print(f"[OK] Download+validate done for first {n} scene(s).")

    @staticmethod
    def smoke_test() -> None:
        print("=== BuildAssetsManifestPipeline Smoke Test ===")
        pipe = BuildAssetsManifestPipeline()
        out = pipe.run(BuildAssetsManifestParams(month="2026-01", max_scenes=3, download_missing=False))
        print("[OK] wrote:", out)
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildAssetsManifestPipeline.smoke_test()
