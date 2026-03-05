from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal
from threading import Lock

import pandas as pd
import rasterio
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

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
from thess_geo_analytics.services.CdseStacService import CdseStacService
from thess_geo_analytics.services.StacAssetResolver import StacAssetResolver
from thess_geo_analytics.utils.RepoPaths import RepoPaths


SortMode = Literal["as_is", "cloud_then_time", "time"]


@dataclass(frozen=True)
class BuildAssetsManifestParams:
    # Selection scope (applies on scenes_selected.csv)
    max_scenes: Optional[int] = None
    date_start: Optional[str] = None  # "YYYY-MM-DD"
    sort_mode: SortMode = "as_is"

    # Downloading
    download_n: int = 3
    download_missing: bool = True
    validate_rasterio: bool = True

    # Output naming
    out_name: str = "assets_manifest_selected.csv"

    # Storage policy
    raw_storage_mode: RawStorageMode = "url_to_local"

    # Mode-aware knobs
    band_resolution: int = 10
    max_download_workers: Optional[int] = None


class BuildAssetsManifestPipeline:
    """
    Step 3 of the pipeline:
      - reads outputs/tables/scenes_selected.csv
      - builds assets_manifest_selected.csv by resolving STAC assets (B04/B08/SCL)
      - optionally downloads N scenes and validates GeoTIFFs with rasterio

    Designed for tests:
      - you can inject stac_service and downloader (mocks)
      - everything respects THESS_RUN_ROOT via RepoPaths helpers
    """

    from thess_geo_analytics.services.StacAssetResolver import StacAssetResolver

    def __init__(
        self,
        *,
        builder: AssetsManifestBuilder | None = None,
        stac_service: CdseStacService | None = None,
        downloader: CdseAssetDownloader | None = None,
        resolver: StacAssetResolver | None = None,  
    ) -> None:
        self._builder_override = builder
        self._stac_service_override = stac_service
        self._downloader_override = downloader
        self._resolver_override = resolver        
        # Lazy defaults (prod)
        self._downloader: Optional[CdseAssetDownloader] = None

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
            sort_mode=params.sort_mode,
            cache_root=RepoPaths.run_root() / "cache" / "s2",  # runtime cache under run root
        )

        builder = self._get_builder(params)
        manifest_df = builder.build_assets_manifest_df(scenes_df, build_params)

        out_tables_dir = RepoPaths.outputs("tables")
        out_tables_dir.mkdir(parents=True, exist_ok=True)

        out_path = RepoPaths.table(params.out_name)

        print(f"[OUTPUT] Assets manifest created in memory — rows: {len(manifest_df)}")
        print(f"[INFO] band_resolution={params.band_resolution} m")

        missing_hrefs = (
            manifest_df[["href_b04", "href_b08", "href_scl"]].isna().any(axis=1).sum()
            if len(manifest_df) > 0
            else 0
        )
        if missing_hrefs:
            print(f"[WARN] {missing_hrefs} row(s) have missing hrefs (resolver mismatch)")

        # Download + validate
        if params.download_missing and len(manifest_df) > 0:
            manifest_df = self._download_and_validate(manifest_df, params=params)

        manifest_df.to_csv(out_path, index=False)
        print(f"[OUTPUT] Assets manifest exported → {out_path}")
        print(f"[INFO] Scenes in manifest: {len(manifest_df)}")

        return out_path

    # -------------------
    # Internals
    # -------------------
    def _get_builder(self, params: BuildAssetsManifestParams) -> AssetsManifestBuilder:
        if self._builder_override is not None:
            return self._builder_override

        stac_service = self._stac_service_override or CdseStacService()
        resolver = self._resolver_override or StacAssetResolver(
            band_resolution=params.band_resolution
        )

        return AssetsManifestBuilder(
            stac_service=stac_service,
            band_resolution=params.band_resolution,
            resolver=resolver,
        )

    def _get_downloader(self) -> CdseAssetDownloader:
        if self._downloader_override is not None:
            return self._downloader_override

        if self._downloader is None:
            token = CdseTokenService()
            self._downloader = CdseAssetDownloader(token)

        return self._downloader

    def _resolve_max_workers(self, params: BuildAssetsManifestParams) -> int:
        max_workers: Optional[int] = params.max_download_workers

        env_override = (
            os.getenv("THESS_MAX_DOWNLOAD_WORKERS")
            or os.getenv("THESS_AMX_DOWNLOAD_WORKERS")  # if you really want this alias
        )
        if env_override:
            try:
                max_workers = max(1, int(env_override))
            except ValueError:
                print(f"[WARN] Ignoring invalid THESS_MAX_DOWNLOAD_WORKERS={env_override!r}")

        if max_workers is None or max_workers <= 0:
            return 4
        return int(max_workers)

    def _download_and_validate(
        self,
        df: pd.DataFrame,
        *,
        params: BuildAssetsManifestParams,
    ) -> pd.DataFrame:
        df = df.copy()

        n = min(int(params.download_n), len(df))
        if n <= 0:
            print("[INFO] download_n <= 0 or empty manifest — skipping downloads.")
            return df

        storage_mode: RawStorageMode = params.raw_storage_mode
        max_workers = self._resolve_max_workers(params)

        print(f"[INFO] Using raw_storage_mode={storage_mode}")
        print(f"[INFO] max_download_workers={max_workers}")
        print(f"[INFO] Download+validate for first {n} scene(s)…")

        downloader = self._get_downloader()

        storage_mgr = RawAssetStorageManager(
            mode=storage_mode,
            downloader=downloader,
        )

        # -------- logging state (thread-safe) --------
        log_rows: list[dict] = []
        log_lock = Lock()

        def _log(
            scene_id: str,
            status: str,
            *,
            ok_b04=None,
            ok_b08=None,
            ok_scl=None,
            message: str = "",
        ) -> None:
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

        # -------- per-scene worker --------
        def _process_scene(idx: int) -> None:
            row = df.iloc[idx]
            scene_id = str(row["scene_id"])

            try:
                # If we're in URL modes, we need hrefs
                if storage_mode.startswith("url_to_"):
                    if (
                        pd.isna(row.get("href_b04"))
                        or pd.isna(row.get("href_b08"))
                        or pd.isna(row.get("href_scl"))
                    ):
                        msg = "missing one or more hrefs; skipping download"
                        print(f"[SKIP] {scene_id}: {msg}")
                        _log(scene_id, "skipped_missing_href", ok_b04=False, ok_b08=False, ok_scl=False, message=msg)
                        return

                p_b04 = Path(row["local_b04"])
                p_b08 = Path(row["local_b08"])
                p_scl = Path(row["local_scl"])

                ok_b04, _ = storage_mgr.ensure_local(
                    url=str(row["href_b04"]),
                    local_path=p_b04,
                    scene_id=scene_id,
                    band="B04",
                    gcs_url=None,
                )
                ok_b08, _ = storage_mgr.ensure_local(
                    url=str(row["href_b08"]),
                    local_path=p_b08,
                    scene_id=scene_id,
                    band="B08",
                    gcs_url=None,
                )
                ok_scl, _ = storage_mgr.ensure_local(
                    url=str(row["href_scl"]),
                    local_path=p_scl,
                    scene_id=scene_id,
                    band="SCL",
                    gcs_url=None,
                )

                all_ok = bool(ok_b04 and ok_b08 and ok_scl)

                if params.validate_rasterio:
                    if not all_ok:
                        msg = "one or more bands missing/failed — skipping validation"
                        print(f"[WARN] {scene_id}: {msg}")
                        _log(scene_id, "download_incomplete", ok_b04=bool(ok_b04), ok_b08=bool(ok_b08), ok_scl=bool(ok_scl), message=msg)
                        return

                    if not (p_b04.exists() and p_b08.exists() and p_scl.exists()):
                        msg = "local files not present for validation"
                        print(f"[WARN] {scene_id}: {msg}")
                        _log(scene_id, "skipped_no_local_for_validation", ok_b04=True, ok_b08=True, ok_scl=True, message=msg)
                        return

                    try:
                        for p in (p_b04, p_b08, p_scl):
                            with rasterio.open(p) as ds:
                                _ = (ds.width, ds.height, ds.crs)
                    except Exception as e:
                        msg = f"validation failed: {e}"
                        print(f"[WARN] {scene_id}: {msg}")
                        _log(scene_id, "validation_failed", ok_b04=True, ok_b08=True, ok_scl=True, message=msg)
                        return

                    _log(scene_id, "success", ok_b04=True, ok_b08=True, ok_scl=True, message="downloaded and validated")
                else:
                    status = "success" if all_ok else "download_incomplete"
                    msg = "downloaded (no validation)" if all_ok else "one or more bands failed download"
                    if not all_ok:
                        print(f"[WARN] {scene_id}: {msg}")
                    _log(scene_id, status, ok_b04=bool(ok_b04), ok_b08=bool(ok_b08), ok_scl=bool(ok_scl), message=msg)

            except Exception as e:
                msg = f"unexpected error: {e}"
                print(f"[WARN] {scene_id}: {msg}")
                _log(scene_id, "exception", ok_b04=False, ok_b08=False, ok_scl=False, message=msg)

        # -------- parallel execution --------
        indices = list(range(n))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_process_scene, idx) for idx in indices]

            for fut in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Downloading S2 assets",
                unit="scene",
            ):
                try:
                    fut.result()
                except Exception as e:
                    print(f"[WARN] Worker raised unhandled exception: {e}")

        print(f"[OK] Download+validate done for first {n} scene(s).")

        # -------- write per-scene download status CSV --------
        log_df = pd.DataFrame(log_rows) if log_rows else pd.DataFrame(
            columns=["scene_id", "status", "ok_b04", "ok_b08", "ok_scl", "message"]
        )

        log_csv = RepoPaths.table("assets_download_status.csv")
        log_df.to_csv(log_csv, index=False)

        if not log_df.empty:
            status_counts = log_df["status"].value_counts().to_dict()
            print(f"[OK] Download status log exported → {log_csv}")
            print("[INFO] Download status counts:", status_counts)
        else:
            print(f"[OK] No scenes processed; empty status log → {log_csv}")

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
                max_scenes=5,
                sort_mode="as_is",
                download_missing=False,
                out_name="assets_manifest_selected_smoke.csv",
                band_resolution=10,
            )
        )
        print("[OK] wrote:", out)
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildAssetsManifestPipeline.smoke_test()