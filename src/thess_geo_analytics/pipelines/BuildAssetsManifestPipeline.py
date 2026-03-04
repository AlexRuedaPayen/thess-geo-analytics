from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal
from threading import Lock
import os

import pandas as pd
import rasterio
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from thess_geo_analytics.builders.AssetsManifestBuilder import (
    AssetsManifestBuilder,
    AssetsManifestBuildParams,
)

from thess_geo_analytics.services.RawAssetStorageManager import (
    RawAssetStorageManager,
    StorageMode as RawStorageMode,
)

from thess_geo_analytics.services.CdseTokenService import CdseTokenService
from thess_geo_analytics.services.CdseAssetDownloader import CdseAssetDownloader
from thess_geo_analytics.utils.RepoPaths import RepoPaths

from thess_geo_analytics.services.CdseStacService import CdseStacService
from thess_geo_analytics.services.StacAssetResolver import StacAssetResolver


SortMode = Literal["as_is", "cloud_then_time", "time"]


@dataclass(frozen=True)
class BuildAssetsManifestParams:
    # Scene selection
    max_scenes: Optional[int] = None
    date_start: Optional[str] = None
    sort_mode: SortMode = "as_is"

    # Download behaviour
    download_n: int = 3
    download_missing: bool = True
    validate_rasterio: bool = True

    # Output
    out_name: str = "assets_manifest_selected.csv"

    # Storage
    raw_storage_mode: RawStorageMode = "url_to_local"

    # Mode knobs
    band_resolution: int = 10
    max_download_workers: Optional[int] = None


class BuildAssetsManifestPipeline:

    def __init__(
        self,
        *,
        builder: AssetsManifestBuilder | None = None,
        stac_service=None,
        downloader: CdseAssetDownloader | None = None,
    ) -> None:

        self._builder_override = builder
        self._stac_service_override = stac_service
        self._downloader_override = downloader

        self._downloader: Optional[CdseAssetDownloader] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, params: BuildAssetsManifestParams) -> Path:

        scenes_csv = RepoPaths.table("scenes_selected.csv")

        if not scenes_csv.exists():
            raise FileNotFoundError(f"Missing scenes_selected: {scenes_csv}")

        scenes_df = pd.read_csv(scenes_csv)

        build_params = AssetsManifestBuildParams(
            max_scenes=params.max_scenes,
            date_start=params.date_start,
            sort_mode=params.sort_mode,
        )

        builder = self._get_builder(params)

        manifest_df = builder.build_assets_manifest_df(
            scenes_df,
            build_params,
        )

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)

        out_path = RepoPaths.table(params.out_name)

        print(f"[OUTPUT] Assets manifest created — rows: {len(manifest_df)}")
        print(f"[INFO] band_resolution={params.band_resolution} m")

        missing = (
            manifest_df[["href_b04", "href_b08", "href_scl"]]
            .isna()
            .any(axis=1)
            .sum()
        )

        if missing:
            print(f"[WARN] {missing} row(s) have missing asset hrefs")

        if params.download_missing and len(manifest_df) > 0:

            manifest_df = self._download_and_validate(
                manifest_df,
                params=params,
            )

        manifest_df.to_csv(out_path, index=False)

        print(f"[OUTPUT] Assets manifest exported → {out_path}")
        print(f"[INFO] Scenes in manifest: {len(manifest_df)}")

        return out_path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_builder(self, params: BuildAssetsManifestParams) -> AssetsManifestBuilder:

        if self._builder_override is not None:
            return self._builder_override

        return AssetsManifestBuilder(
            band_resolution=params.band_resolution,
            stac_service=self._stac_service_override,
        )

    def _get_downloader(self) -> CdseAssetDownloader:

        if self._downloader_override is not None:
            return self._downloader_override

        if self._downloader is None:
            token = CdseTokenService()
            self._downloader = CdseAssetDownloader(token)

        return self._downloader

    # ------------------------------------------------------------------
    # Download + validation
    # ------------------------------------------------------------------

    def _download_and_validate(
        self,
        df: pd.DataFrame,
        *,
        params: BuildAssetsManifestParams,
    ) -> pd.DataFrame:

        downloader = self._get_downloader()

        storage_mgr = RawAssetStorageManager(
            mode=params.raw_storage_mode,
            downloader=downloader,
        )

        df = df.copy()

        n = min(int(params.download_n), len(df))

        if n <= 0:
            print("[INFO] download_n <= 0 — skipping downloads")
            return df

        max_workers = params.max_download_workers

        if max_workers is None or max_workers <= 0:
            max_workers = 4

        env_override = os.getenv("THESS_MAX_DOWNLOAD_WORKERS")

        if env_override:
            try:
                max_workers = max(1, int(env_override))
            except ValueError:
                pass

        print(f"[INFO] Downloading first {n} scenes (workers={max_workers})")

        log_rows: list[dict] = []
        log_lock = Lock()

        def _log(scene_id, status, ok_b04=None, ok_b08=None, ok_scl=None, message=""):

            row = {
                "scene_id": scene_id,
                "status": status,
                "ok_b04": ok_b04,
                "ok_b08": ok_b08,
                "ok_scl": ok_scl,
                "message": message,
            }

            with log_lock:
                log_rows.append(row)

        def _process_scene(idx: int):

            row = df.iloc[idx]

            scene_id = row["scene_id"]

            try:

                if pd.isna(row["href_b04"]) or pd.isna(row["href_b08"]) or pd.isna(row["href_scl"]):

                    msg = "missing asset href"

                    print(f"[SKIP] {scene_id}: {msg}")

                    _log(scene_id, "missing_href", False, False, False, msg)

                    return

                p_b04 = Path(row["local_b04"])
                p_b08 = Path(row["local_b08"])
                p_scl = Path(row["local_scl"])

                ok_b04, _ = storage_mgr.ensure_local(
                    url=str(row["href_b04"]),
                    local_path=p_b04,
                    scene_id=scene_id,
                    band="B04",
                )

                ok_b08, _ = storage_mgr.ensure_local(
                    url=str(row["href_b08"]),
                    local_path=p_b08,
                    scene_id=scene_id,
                    band="B08",
                )

                ok_scl, _ = storage_mgr.ensure_local(
                    url=str(row["href_scl"]),
                    local_path=p_scl,
                    scene_id=scene_id,
                    band="SCL",
                )

                all_ok = bool(ok_b04 and ok_b08 and ok_scl)

                if params.validate_rasterio and all_ok:

                    try:

                        for p in (p_b04, p_b08, p_scl):

                            with rasterio.open(p) as ds:
                                _ = (ds.width, ds.height, ds.crs)

                    except Exception as e:

                        msg = f"validation failed: {e}"

                        print(f"[WARN] {scene_id}: {msg}")

                        _log(scene_id, "validation_failed", ok_b04, ok_b08, ok_scl, msg)

                        return

                status = "success" if all_ok else "download_incomplete"

                _log(scene_id, status, ok_b04, ok_b08, ok_scl)

            except Exception as e:

                msg = f"unexpected error: {e}"

                print(f"[WARN] {scene_id}: {msg}")

                _log(scene_id, "exception", False, False, False, msg)

        indices = list(range(n))

        with ThreadPoolExecutor(max_workers=max_workers) as ex:

            futures = [ex.submit(_process_scene, i) for i in indices]

            for f in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Downloading S2 assets",
                unit="scene",
            ):

                try:
                    f.result()
                except Exception as e:
                    print(f"[WARN] worker exception: {e}")

        print("[OK] Download stage completed")

        log_df = pd.DataFrame(log_rows)

        log_csv = RepoPaths.table("assets_download_status.csv")

        log_df.to_csv(log_csv, index=False)

        if not log_df.empty:
            print("[INFO] Download status:", log_df["status"].value_counts().to_dict())

        print(f"[OK] Log exported → {log_csv}")

        return df

    # ------------------------------------------------------------------
    # Smoke test
    # ------------------------------------------------------------------

    @staticmethod
    def smoke_test():

        print("=== BuildAssetsManifestPipeline Smoke Test ===")

        pipe = BuildAssetsManifestPipeline()

        out = pipe.run(
            BuildAssetsManifestParams(
                download_missing=False,
                sort_mode="as_is",
                band_resolution=10,
                out_name="assets_manifest_selected_smoke.csv",
            )
        )

        print("[OK] wrote:", out)
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildAssetsManifestPipeline.smoke_test()