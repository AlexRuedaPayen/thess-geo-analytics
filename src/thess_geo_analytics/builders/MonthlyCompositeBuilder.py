from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Tuple

import numpy as np
import pandas as pd
import rasterio
from rasterio.warp import reproject
from rasterio.enums import Resampling
from tqdm import tqdm

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.geo.CloudMasker import CloudMasker
from thess_geo_analytics.geo.NdviProcessor import NdviProcessor
from thess_geo_analytics.geo.AoiTargetGrid import AoiTargetGrid

from thess_geo_analytics.services.CdseAssetDownloader import CdseAssetDownloader
from thess_geo_analytics.services.CdseTokenService import CdseTokenService
from thess_geo_analytics.services.RawAssetStorageManager import (
    RawAssetStorageManager,
    StorageMode as RawStorageMode,
)
from thess_geo_analytics.utils.GcsClient import GcsClient


@dataclass(frozen=True)
class MonthlyCompositeConfig:
    # NDVI / compositing
    nodata: float = -9999.0
    max_scenes: Optional[int] = None
    composite_method: str = "median"
    verbose: bool = False

    # Raw asset downloading (from CDSE / GCS)
    download_missing: bool = True  # if False, never try to fetch missing raw bands

    # density policy
    min_scenes_per_month: int = 2
    fallback_to_quarterly: bool = True

    # ------------- RAW ASSET STORAGE (INPUT) -----------------
    # How raw B04/B08/SCL are obtained & stored:
    #   - "url_to_local":         use href_* URLs (CDSE) -> local only
    #   - "url_to_gcs_keep_local": CDSE -> local + GCS (keep local)
    #   - "url_to_gcs_drop_local": CDSE -> local + GCS, then delete local
    #   - "gcs_to_local":          ignore URLs, use gcs_* to restore local from GCS
    raw_storage_mode: RawStorageMode = "url_to_local"

    # Shared GCS config (raw + composites)
    gcs_bucket: Optional[str] = None
    gcs_credentials: Optional[str] = None

    # Where raw bands live in GCS (if used)
    gcs_prefix_raw: str = "raw_s2"

    # ------------- OUTPUT STORAGE (COMPOSITES) -----------------
    # Whether to upload NDVI composites (GeoTIFF + preview PNG) to GCS
    upload_composites_to_gcs: bool = False
    gcs_prefix_composites: str = "ndvi/composites"  # e.g. ndvi/composites/<aoi_id>/ndvi_YYYY-MM_*.tif


class MonthlyCompositeBuilder:
    """
    Produces AOI-wide NDVI composites using:
      - time_serie.csv (anchor_date -> scene_id(s))
      - assets_manifest_selected.csv (scene_id -> local paths + hrefs + optional gcs_*)

    Raw assets (B04/B08/SCL) can be sourced from:
      - local/CDSE (url_to_*) or
      - GCS (gcs_to_local)

    Outputs:
      - Local GeoTIFF + PNG
      - Optional GCS upload for composites
    """

    def __init__(
        self,
        aoi_path: Path,
        aoi_id: str = "el522",
        cfg: MonthlyCompositeConfig | None = None,
        *,
        time_serie_csv: Path,
        assets_manifest_csv: Path,
    ) -> None:
        self.aoi_path = aoi_path
        self.aoi_id = aoi_id
        self.cfg = cfg or MonthlyCompositeConfig()

        self.time_serie_csv = time_serie_csv
        self.assets_manifest_csv = assets_manifest_csv

        self.ndvi = NdviProcessor()
        self.masker = CloudMasker()

        self.token_service = CdseTokenService()
        self.downloader = CdseAssetDownloader(self.token_service)

        self._target = None  # lazy AOI target grid
        self._gcs_client: Optional[GcsClient] = None
        self._storage_mgr: Optional[RawAssetStorageManager] = None

    # -----------------------
    # Public API
    # -----------------------
    def run_all_months(self) -> list[tuple[Path, Path]]:
        ts = self._load_time_serie()
        df_assets = self._load_assets_manifest()

        months = sorted(ts["anchor_month"].unique())
        outs: list[tuple[Path, Path]] = []

        for m in tqdm(months, desc="NDVI monthly composites", unit="cog"):
            month_ts = ts[ts["anchor_month"] == m].copy()
            out_tif, out_png = self._run_period(label=m, ts_subset=month_ts, df_assets=df_assets)
            outs.append((out_tif, out_png))

        return outs



    def run_all_periods(self) -> list[tuple[str, Path, Path]]:
        """
        Preferred: build monthly composites where density is sufficient,
        otherwise fallback to quarterly composites covering sparse months.

        Returns: [(label, out_tif, out_png), ...]
        """
        ts = self._load_time_serie()
        df_assets = self._load_assets_manifest()

        # Count unique scenes per month
        month_counts = ts.groupby("anchor_month")["id"].nunique().sort_index()
        good_months = set(month_counts[month_counts >= self.cfg.min_scenes_per_month].index)
        all_months = list(month_counts.index)

        # Build list of (label, ts_subset) = each final composite we will produce
        jobs: list[tuple[str, pd.DataFrame]] = []

        # 1) good monthly composites
        for m in all_months:
            if m in good_months:
                month_ts = ts[ts["anchor_month"] == m].copy()
                jobs.append((m, month_ts))

        # 2) fallback quarters for sparse months
        if self.cfg.fallback_to_quarterly:
            sparse_months = [m for m in all_months if m not in good_months]
            sparse_quarters = sorted(ts[ts["anchor_month"].isin(sparse_months)]["anchor_quarter"].unique())

            for q in sparse_quarters:
                q_ts = ts[ts["anchor_quarter"] == q].copy()
                if q_ts.empty:
                    continue
                jobs.append((q, q_ts))

        outs: list[tuple[str, Path, Path]] = []

        # Single tqdm bar: 1 tick per final COG
        for label, ts_subset in tqdm(jobs, desc="NDVI composites", unit="cog"):
            out_tif, out_png = self._run_period(label=label, ts_subset=ts_subset, df_assets=df_assets)
            outs.append((label, out_tif, out_png))

        return outs


    def run_month(self, month: str) -> tuple[Path, Path]:
        ts = self._load_time_serie()
        df_assets = self._load_assets_manifest()

        month_ts = ts[ts["anchor_month"] == month].copy()
        if month_ts.empty:
            raise RuntimeError(f"No anchors found for month={month} in {self.time_serie_csv}")

        return self._run_period(label=month, ts_subset=month_ts, df_assets=df_assets)

    def run_quarter(self, quarter: str) -> tuple[Path, Path]:
        """
        quarter must look like '2024-Q3'
        """
        ts = self._load_time_serie()
        df_assets = self._load_assets_manifest()

        q_ts = ts[ts["anchor_quarter"] == quarter].copy()
        if q_ts.empty:
            raise RuntimeError(f"No anchors found for quarter={quarter} in {self.time_serie_csv}")

        return self._run_period(label=quarter, ts_subset=q_ts, df_assets=df_assets)

    # -----------------------
    # Core runner
    # -----------------------
    def _run_period(self, *, label: str, ts_subset: pd.DataFrame, df_assets: pd.DataFrame) -> tuple[Path, Path]:
        """
        Build one composite for the given label (YYYY-MM or YYYY-Qn) using anchors in ts_subset.
        """

        scene_ids = self._extract_scene_ids(ts_subset)
        scene_ids = list(dict.fromkeys(scene_ids))  # stable unique

        if self.cfg.max_scenes is not None:
            scene_ids = scene_ids[: self.cfg.max_scenes]

        if not scene_ids:
            raise RuntimeError(f"No scene_ids resolved for period={label}")

        rows = self._rows_for_scene_ids(df_assets, scene_ids)
        if not rows:
            raise RuntimeError(
                f"No matching rows in assets manifest for period={label}. "
                f"Check scene_ids and assets_manifest_selected.csv."
            )

        target = self._get_target_grid()

        out_profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "count": 1,
            "crs": target.crs,
            "transform": target.transform,
            "width": target.width,
            "height": target.height,
            "nodata": self.cfg.nodata,
            "tiled": True,
            "compress": "deflate",
        }

        ndvi_stack: List[np.ndarray] = []
        processed = 0
        skipped = 0

        for row in rows:
            sid = str(row["scene_id"])
            try:
                ok_assets = True
                if self.cfg.download_missing:
                    ok_assets = self._ensure_assets(row)

                b04_path = Path(row["local_b04"])
                b08_path = Path(row["local_b08"])
                scl_path = Path(row["local_scl"])

                if not ok_assets or not (b04_path.exists() and b08_path.exists() and scl_path.exists()):
                    skipped += 1
                    if self.cfg.verbose:
                        print(f"[WARN] Missing or unavailable assets for {sid}, skipping.")
                    continue

                # --- NDVI in native grid ---
                with rasterio.open(b04_path) as ds_r, rasterio.open(b08_path) as ds_n:
                    red = ds_r.read(1).astype(np.float32)
                    nir = ds_n.read(1).astype(np.float32)
                    nd_native = self.ndvi.compute_ndvi(red, nir)
                    src_transform = ds_r.transform
                    src_crs = ds_r.crs

                # --- Reproject NDVI to target AOI grid ---
                nd_target = np.empty((target.height, target.width), dtype=np.float32)

                reproject(
                    source=nd_native,
                    destination=nd_target,
                    src_transform=src_transform,
                    src_crs=src_crs,
                    dst_transform=target.transform,
                    dst_crs=target.crs,
                    resampling=Resampling.bilinear,
                    dst_nodata=np.nan,
                )

                # --- Reproject SCL to target AOI grid ---
                with rasterio.open(scl_path) as sds:
                    scl_native = rasterio.band(sds, 1)
                    scl_nodata = sds.nodata

                    scl_target = np.empty((target.height, target.width), dtype=np.uint16)

                    reproject(
                        source=scl_native,
                        destination=scl_target,
                        src_transform=sds.transform,
                        src_crs=sds.crs,
                        dst_transform=target.transform,
                        dst_crs=target.crs,
                        resampling=Resampling.nearest,
                        dst_nodata=scl_nodata,
                    )

                invalid_cloud = self.masker.build_invalid_mask_from_scl(scl_target, scl_nodata)

                nd_target[invalid_cloud] = np.nan
                nd_target[~target.aoi_mask] = np.nan

                ndvi_stack.append(nd_target)
                processed += 1

            except Exception as e:
                skipped += 1
                if self.cfg.verbose:
                    print(f"[WARN] Skipping {sid} → {e}")


        if processed == 0:
            raise RuntimeError(f"No scenes processed successfully for period={label} (skipped={skipped}).")

        stack = np.stack(ndvi_stack, axis=0)

        if self.cfg.composite_method != "median":
            raise ValueError(f"Unsupported composite_method={self.cfg.composite_method} (only 'median').")

        composite = np.nanmedian(stack, axis=0).astype(np.float32)

        # sanity
        valid = composite[~np.isnan(composite)]
        if valid.size > 0 and (valid.min() < -1.0001 or valid.max() > 1.0001):
            raise ValueError("Composite NDVI outside [-1,1].")

        # outputs
        cogs_dir = RepoPaths.OUTPUTS / "cogs"
        cogs_dir.mkdir(parents=True, exist_ok=True)
        RepoPaths.FIGURES.mkdir(parents=True, exist_ok=True)

        out_tif = cogs_dir / f"ndvi_{label}_{self.aoi_id}.tif"
        out_png = RepoPaths.figure(f"ndvi_{label}_preview.png")

        self._write_geotiff(out_tif, composite, out_profile)
        self._write_preview_png(out_png, composite)

        # Optional: upload outputs to GCS
        self._upload_outputs_if_needed(label, out_tif, out_png)

        if self.cfg.verbose:
            print(f"[OK] period={label} scenes_used={processed} skipped={skipped}")
            print(f"[OK] Written:  {out_tif}")
            print(f"[OK] Preview:  {out_png}")

        return out_tif, out_png

    # -----------------------
    # Loading helpers
    # -----------------------
    def _load_time_serie(self) -> pd.DataFrame:
        if not self.time_serie_csv.exists():
            raise FileNotFoundError(f"Missing time serie file: {self.time_serie_csv}")

        ts = pd.read_csv(self.time_serie_csv)

        if "anchor_date" not in ts.columns:
            raise ValueError(f"time_serie.csv missing 'anchor_date' column. Found: {list(ts.columns)}")

        # keep as UTC timestamps (fewer surprises), then derive labels
        ts["anchor_date"] = pd.to_datetime(ts["anchor_date"], errors="coerce", utc=True)
        ts = ts.dropna(subset=["anchor_date"])

        ts["anchor_month"] = ts["anchor_date"].dt.strftime("%Y-%m")
        ts["anchor_quarter"] = ts["anchor_date"].apply(lambda x: f"{x.year}-Q{((x.month - 1)//3) + 1}")

        # normalize scene_id(s) to be present for counting; support either column name
        if "scene_id" not in ts.columns and "scene_ids" in ts.columns:
            # take first id for counting; extraction will handle full list
            ts["scene_id"] = ts["scene_ids"].astype(str).str.split("|").str[0]

        return ts

    def _load_assets_manifest(self) -> pd.DataFrame:
        if not self.assets_manifest_csv.exists():
            raise FileNotFoundError(
                f"Missing assets manifest: {self.assets_manifest_csv}\n"
                f"Run BuildAssetsManifest (new version) first."
            )

        df = pd.read_csv(self.assets_manifest_csv)

        required = {
            "scene_id",
            "href_b04", "href_b08", "href_scl",
            "local_b04", "local_b08", "local_scl",
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"assets manifest missing columns: {sorted(missing)}")

        # gcs_* columns are optional; used for gcs_to_local or url_to_gcs_* modes
        return df

    def _extract_scene_ids(self, ts_subset: pd.DataFrame) -> List[str]:
        if "id" in ts_subset.columns:
            s = ts_subset["id"].dropna().astype(str).tolist()
            return [x for x in s if x.strip()]

        if "ids" in ts_subset.columns:
            out: List[str] = []
            for v in ts_subset["ids"].dropna().astype(str):
                parts = [p.strip() for p in v.split("|") if p.strip()]
                out.extend(parts)
            return out

        raise ValueError(
            "time_serie.csv must have either 'scene_id' or 'scene_ids' column "
            f"(found: {list(ts_subset.columns)})"
        )

    def _rows_for_scene_ids(self, df_assets: pd.DataFrame, scene_ids: List[str]) -> List[Dict[str, str]]:
        sub = df_assets[df_assets["scene_id"].astype(str).isin(scene_ids)].copy()
        rows_by_id = {str(r["scene_id"]): r for _, r in sub.iterrows()}

        rows: List[Dict[str, str]] = []
        for sid in scene_ids:
            r = rows_by_id.get(str(sid))
            if r is None:
                continue
            rows.append({k: r[k] for k in r.index})
        return rows

    # -----------------------
    # AOI grid
    # -----------------------
    def _get_target_grid(self):
        if self._target is None:
            self._target = AoiTargetGrid(
                aoi_path=self.aoi_path,
                target_crs="EPSG:32634",
                resolution=10.0,
            ).build()
        return self._target

    # -----------------------
    # GCS / raw storage
    # -----------------------
    def _get_gcs_client(self) -> GcsClient:
        if self._gcs_client is None:
            if not self.cfg.gcs_bucket:
                raise ValueError(
                    "GCS access requested (raw_storage_mode or upload_composites_to_gcs) "
                    "but cfg.gcs_bucket is not set."
                )
            self._gcs_client = GcsClient(
                bucket=self.cfg.gcs_bucket,
                credentials=self.cfg.gcs_credentials,
            )
        return self._gcs_client

    def _get_storage_manager(self) -> RawAssetStorageManager:
        if self._storage_mgr is None:
            mode = self.cfg.raw_storage_mode

            # GCS client only needed if mode uses GCS
            if mode in {"url_to_gcs_keep_local", "url_to_gcs_drop_local", "gcs_to_local"}:
                gcs = self._get_gcs_client()
            else:
                gcs = None

            self._storage_mgr = RawAssetStorageManager(
                mode=mode,
                downloader=self.downloader,
                gcs_client=gcs,
                gcs_prefix=self.cfg.gcs_prefix_raw,
            )
        return self._storage_mgr

    def _ensure_assets(self, row: Dict[str, str]) -> bool:
        """
        Ensure B04, B08, SCL exist locally according to the chosen raw_storage_mode.
        Returns True if all three are available locally (even temporarily).
        """
        storage_mgr = self._get_storage_manager()
        scene_id = str(row["scene_id"])

        p_b04 = Path(row["local_b04"])
        p_b08 = Path(row["local_b08"])
        p_scl = Path(row["local_scl"])

        gcs_b04 = row.get("gcs_b04")
        gcs_b08 = row.get("gcs_b08")
        gcs_scl = row.get("gcs_scl")

        ok_b04, _ = storage_mgr.ensure_local(
            url=str(row["href_b04"]) if "href_b04" in row else None,
            local_path=p_b04,
            scene_id=scene_id,
            band="B04",
            gcs_url=gcs_b04,
        )
        ok_b08, _ = storage_mgr.ensure_local(
            url=str(row["href_b08"]) if "href_b08" in row else None,
            local_path=p_b08,
            scene_id=scene_id,
            band="B08",
            gcs_url=gcs_b08,
        )
        ok_scl, _ = storage_mgr.ensure_local(
            url=str(row["href_scl"]) if "href_scl" in row else None,
            local_path=p_scl,
            scene_id=scene_id,
            band="SCL",
            gcs_url=gcs_scl,
        )

        return ok_b04 and ok_b08 and ok_scl

    # -----------------------
    # Outputs (local + optional GCS)
    # -----------------------
    def _upload_outputs_if_needed(self, label: str, out_tif: Path, out_png: Path) -> None:
        if not self.cfg.upload_composites_to_gcs:
            return

        try:
            gcs = self._get_gcs_client()
        except Exception as e:
            if self.cfg.verbose:
                print(f"[WARN] Cannot upload composites to GCS (config issue): {e}")
            return

        base_prefix = f"{self.cfg.gcs_prefix_composites}/{self.aoi_id}"
        remote_tif = f"{base_prefix}/ndvi_{label}_{self.aoi_id}.tif"
        remote_png = f"{base_prefix}/ndvi_{label}_{self.aoi_id}_preview.png"

        try:
            url_tif = gcs.upload(out_tif, remote_tif)
            url_png = gcs.upload(out_png, remote_png)
            if self.cfg.verbose:
                print(f"[OK] GCS composite GeoTIFF → {url_tif}")
                print(f"[OK] GCS composite PNG     → {url_png}")
        except Exception as e:
            if self.cfg.verbose:
                print(f"[WARN] Failed to upload composites for {label} to GCS → {e}")

    def _write_geotiff(self, path: Path, arr: np.ndarray, profile: dict) -> None:
        out = np.where(np.isnan(arr), profile["nodata"], arr)
        with rasterio.open(path, "w", **profile) as dst:
            dst.write(out.astype(np.float32), 1)
            dst.build_overviews([2, 4, 8, 16], Resampling.nearest)
            dst.update_tags(ns="rio_overview", resampling="nearest")

    def _write_preview_png(self, path: Path, arr: np.ndarray) -> None:
        import matplotlib.pyplot as plt

        plt.figure(figsize=(10, 8))
        plt.imshow(arr, vmin=-0.2, vmax=0.8)
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(path, dpi=150, bbox_inches="tight", pad_inches=0)
        plt.close()
