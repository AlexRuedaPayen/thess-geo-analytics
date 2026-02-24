from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal
from threading import Lock
import rasterio

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
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
from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.utils.GcsClient import GcsClient

SortMode = Literal["as_is", "cloud_then_time", "time"]


@dataclass(frozen=True)
class BuildAssetsManifestParams:
    # Selection scope (applies on scenes_selected.csv)
    max_scenes: Optional[int] = None
    date_start: Optional[str] = None  # "YYYY-MM-DD"
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

    raw_storage_mode: RawStorageMode = "url_to_local"

    # Mode-aware knobs
    band_resolution: int = 10
    max_download_workers: Optional[int] = None


class BuildAssetsManifestPipeline:
    def __init__(self, builder: AssetsManifestBuilder | None = None) -> None:
        self._builder_override = builder
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
            sort_mode=params.sort_mode,
        )

        builder = self._get_builder(params)
        manifest_df = builder.build_assets_manifest_df(scenes_df, build_params)

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)
        out_path = RepoPaths.table(params.out_name)

        print(f"[OUTPUT] Assets manifest created in memory — rows: {len(manifest_df)}")
        print(f"[INFO] band_resolution={params.band_resolution} m")

        missing_hrefs = (
            manifest_df[["href_b04", "href_b08", "href_scl"]]
            .isna()
            .any(axis=1)
            .sum()
        )
        if missing_hrefs:
            print(f"[WARN] {missing_hrefs} row(s) have missing hrefs (resolver mismatch)")

        # Download + validate + optional GCS upload
        if params.download_missing and len(manifest_df) > 0:
            manifest_df = self._download_and_validate(
                manifest_df,
                params=params,
            )

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

        return AssetsManifestBuilder(
            band_resolution=params.band_resolution,
        )

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

        downloader = self._get_downloader()
        gcs: Optional[GcsClient] = None
        if params.upload_to_gcs:
            gcs = self._get_gcs(params)

        df = df.copy()

        n = min(int(params.download_n), len(df))
        if n <= 0:
            print("[INFO] download_n <= 0 or empty manifest — skipping downloads.")
            return df

        # Decide storage mode from params
        storage_mode: RawStorageMode = params.raw_storage_mode

        # If user set upload_to_gcs=True but still left raw_storage_mode at url_to_local,
        # default to url_to_gcs_keep_local for convenience:
        if params.upload_to_gcs and storage_mode == "url_to_local":
            storage_mode = "url_to_gcs_keep_local"

        # -------- max_workers resolution --------
        max_workers: Optional[int] = params.max_download_workers

        # Env override: THESS_AMX_DOWNLAOD_WORKERS (typo kept for backwards compat)
        env_override = os.getenv("THESS_AMX_DOWNLAOD_WORKERS") or os.getenv(
            "THESS_MAX_DOWNLOAD_WORKERS"
        )
        if env_override:
            try:
                max_workers = max(1, int(env_override))
            except ValueError:
                print(
                    f"[WARN] Ignoring invalid THESS_AMX_DOWNLAOD_WORKERS={env_override!r}"
                )

        if max_workers is None or max_workers <= 0:
            max_workers = 4

        print(f"[INFO] Using raw_storage_mode={storage_mode}")
        print(f"[INFO] max_download_workers={max_workers}")

        storage_mgr = RawAssetStorageManager(
            mode=storage_mode,
            downloader=downloader,
            gcs_client=gcs,
            gcs_prefix=params.gcs_prefix,
        )

        print(
            f"[INFO] Download+validate for first {n} scene(s)… "
            f"(mode={storage_mode}, workers={max_workers})"
        )

        # -------- logging state (thread-safe) --------
        log_rows: list[dict] = []
        log_lock = Lock()

        def _log(scene_id: str, status: str, *, ok_b04=None, ok_b08=None, ok_scl=None, message: str = ""):
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

        # Helper: normalise GCS URL values
        def _norm_gcs(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            v = str(v).strip()
            return v or None

        # -------- per-scene worker --------
        def _process_scene(idx: int) -> None:
            row = df.iloc[idx]
            scene_id = row["scene_id"]

            try:
                # Skip rows with missing hrefs if we're in URL-based modes
                if storage_mode.startswith("url_to_"):
                    if (
                        pd.isna(row["href_b04"])
                        or pd.isna(row["href_b08"])
                        or pd.isna(row["href_scl"])
                    ):
                        msg = "missing one or more hrefs; skipping download"
                        print(f"[SKIP] {scene_id}: {msg}")
                        _log(
                            scene_id,
                            status="skipped_missing_href",
                            ok_b04=False,
                            ok_b08=False,
                            ok_scl=False,
                            message=msg,
                        )
                        return

                p_b04 = Path(row["local_b04"])
                p_b08 = Path(row["local_b08"])
                p_scl = Path(row["local_scl"])

                # Existing GCS URLs (if we have them); treat NaN as None
                gcs_b04 = _norm_gcs(row["gcs_b04"]) if "gcs_b04" in df.columns else None
                gcs_b08 = _norm_gcs(row["gcs_b08"]) if "gcs_b08" in df.columns else None
                gcs_scl = _norm_gcs(row["gcs_scl"]) if "gcs_scl" in df.columns else None

                # 1) Ensure local B04
                ok_b04, new_gcs_b04 = storage_mgr.ensure_local(
                    url=str(row["href_b04"])
                    if "href_b04" in row and not pd.isna(row["href_b04"])
                    else None,
                    local_path=p_b04,
                    scene_id=scene_id,
                    band="B04",
                    gcs_url=gcs_b04,
                )

                # 2) Ensure local B08
                ok_b08, new_gcs_b08 = storage_mgr.ensure_local(
                    url=str(row["href_b08"])
                    if "href_b08" in row and not pd.isna(row["href_b08"])
                    else None,
                    local_path=p_b08,
                    scene_id=scene_id,
                    band="B08",
                    gcs_url=gcs_b08,
                )

                # 3) Ensure local SCL
                ok_scl, new_gcs_scl = storage_mgr.ensure_local(
                    url=str(row["href_scl"])
                    if "href_scl" in row and not pd.isna(row["href_scl"])
                    else None,
                    local_path=p_scl,
                    scene_id=scene_id,
                    band="SCL",
                    gcs_url=gcs_scl,
                )

                # Update manifest with any new GCS URLs
                if new_gcs_b04 is not None:
                    df.at[row.name, "gcs_b04"] = new_gcs_b04
                if new_gcs_b08 is not None:
                    df.at[row.name, "gcs_b08"] = new_gcs_b08
                if new_gcs_scl is not None:
                    df.at[row.name, "gcs_scl"] = new_gcs_scl

                all_ok = bool(ok_b04 and ok_b08 and ok_scl)

                # If validation requested but some bands are missing/unavailable, skip this scene
                if params.validate_rasterio:
                    if not all_ok:
                        msg = "one or more bands missing/failed after storage step — skipping validation"
                        print(f"[WARN] {scene_id}: {msg}")
                        _log(
                            scene_id,
                            status="download_incomplete",
                            ok_b04=bool(ok_b04),
                            ok_b08=bool(ok_b08),
                            ok_scl=bool(ok_scl),
                            message=msg,
                        )
                        return

                    # In drop_local mode, local files may have been removed,
                    # so only validate if the paths still exist.
                    if not (p_b04.exists() and p_b08.exists() and p_scl.exists()):
                        msg = "local files not present for validation — skipping validation"
                        print(f"[WARN] {scene_id}: {msg}")
                        _log(
                            scene_id,
                            status="skipped_no_local_for_validation",
                            ok_b04=bool(ok_b04),
                            ok_b08=bool(ok_b08),
                            ok_scl=bool(ok_scl),
                            message=msg,
                        )
                        return

                    try:
                        for p in (p_b04, p_b08, p_scl):
                            with rasterio.open(p) as ds:
                                _ = (ds.width, ds.height, ds.crs)
                    except Exception as e:
                        msg = f"validation failed: {e}"
                        print(f"[WARN] {scene_id}: {msg}")
                        _log(
                            scene_id,
                            status="validation_failed",
                            ok_b04=bool(ok_b04),
                            ok_b08=bool(ok_b08),
                            ok_scl=bool(ok_scl),
                            message=msg,
                        )
                        return

                    # If we got here, everything is fine (download + validation)
                    _log(
                        scene_id,
                        status="success",
                        ok_b04=True,
                        ok_b08=True,
                        ok_scl=True,
                        message="downloaded and validated",
                    )
                else:
                    # No validation: log based on download result only
                    status = "success" if all_ok else "download_incomplete"
                    msg = "downloaded (no validation)" if all_ok else "one or more bands failed download"
                    if not all_ok:
                        print(f"[WARN] {scene_id}: {msg}")
                    _log(
                        scene_id,
                        status=status,
                        ok_b04=bool(ok_b04),
                        ok_b08=bool(ok_b08),
                        ok_scl=bool(ok_scl),
                        message=msg,
                    )

            except Exception as e:
                # Catch anything unexpected so one scene can't kill the pool
                msg = f"unexpected error: {e}"
                print(f"[WARN] {scene_id}: {msg}")
                _log(
                    scene_id,
                    status="exception",
                    ok_b04=False,
                    ok_b08=False,
                    ok_scl=False,
                    message=msg,
                )

        # -------- parallel execution with completion-based progress bar --------
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
                    # Any remaining uncaught errors (should be rare)
                    fut.result()
                except Exception as e:
                    print(f"[WARN] Worker raised unhandled exception: {e}")

        print(f"[OK] Download+validate done for first {n} scene(s).")

        # -------- write per-scene download status CSV --------
        if log_rows:
            log_df = pd.DataFrame(log_rows)
        else:
            log_df = pd.DataFrame(
                columns=["scene_id", "status", "ok_b04", "ok_b08", "ok_scl", "message"]
            )

        log_csv = RepoPaths.table("assets_download_status.csv")
        log_df.to_csv(log_csv, index=False)

        # Small summary
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
                max_scenes=None,
                sort_mode="as_is",
                download_missing=False,  # no downloads in smoke
                out_name="assets_manifest_selected_smoke.csv",
                band_resolution=10,
            )
        )
        print("[OK] wrote:", out)
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildAssetsManifestPipeline.smoke_test()