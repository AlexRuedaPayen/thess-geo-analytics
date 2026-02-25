from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import warnings

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
from tqdm import tqdm

from thess_geo_analytics.geo.NdviProcessor import NdviProcessor
from thess_geo_analytics.geo.CloudMasker import CloudMasker
from thess_geo_analytics.geo.AoiTargetGrid import AoiTargetGrid
from thess_geo_analytics.utils.RepoPaths import RepoPaths


class NdviAggregatedCompositeBuilder:
    """
    Build NDVI composites from pre-aggregated timestamp folders in:

        DATA_LAKE/data_raw/aggregated/<timestamp>/

    Each folder is expected to contain at least B04 and B08 rasters, and
    optionally an SCL raster for cloud masking.

    Parallelization model:
      - debug=True  => sequential, re-raise errors (helpful for tracebacks)
      - else        => parallel execution with ThreadPoolExecutor
                       workers never raise to main thread; errors are logged
    Outputs:
      - NDVI GeoTIFFs in outputs/cogs/
      - metadata JSON per composite in outputs/metadata/
      - logs in outputs/tables/:
          * ndvi_aggregated_composites_status.csv   (coarse per label)
          * ndvi_aggregated_composites_summary.csv  (detailed per label)
    """

    def __init__(self, *, aoi_path: Path, aoi_id: str) -> None:
        self.aoi_path = aoi_path
        self.aoi_id = aoi_id

        self.ndvi = NdviProcessor()
        self.masker = CloudMasker()

        self._target = None  # lazy AoiTargetGrid result

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------
    def _discover(self, root: Path) -> pd.DataFrame:
        """
        Folder names you have:
          2021-02-27 09_20_31.024000+00_00

        Normalize to ISO-like:
          2021-02-27T09:20:31.024000+00:00
        """
        rows: List[Dict[str, Any]] = []

        if not root.exists():
            raise FileNotFoundError(f"Aggregated root does not exist: {root}")

        for p in sorted(root.iterdir()):
            if not p.is_dir():
                continue

            ts_name = p.name

            dt = pd.to_datetime(ts_name, utc=True, errors="coerce")
            if pd.isna(dt):
                normalised = ts_name.replace(" ", "T").replace("_", ":")
                dt = pd.to_datetime(normalised, utc=True, errors="coerce")

            if pd.isna(dt):
                continue

            rows.append(
                {
                    "timestamp": ts_name,
                    "datetime": dt,
                    "month": dt.strftime("%Y-%m"),
                    "quarter": f"{dt.year}-Q{((dt.month - 1) // 3) + 1}",
                    "path": p,
                }
            )

        if not rows:
            raise RuntimeError(f"No valid timestamp folders in: {root}")

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Public API: per-timestamp
    # ------------------------------------------------------------------
    def run_all_timestamps(
        self,
        *,
        aggregated_root: Path,
        max_scenes: int | None,
        enable_cloud_masking: bool,
        verbose: bool,
        # parallel knobs (pipeline-derived)
        max_workers: int = 4,
        debug: bool = False,
    ) -> List[Tuple[str, Path, Path]]:
        df = self._discover(aggregated_root)

        jobs: List[Tuple[str, List[Path]]] = []
        for _, row in df.iterrows():
            jobs.append((str(row["timestamp"]), [Path(row["path"])]))

        # ensure target grid is built before threads start
        _ = self._get_target()

        return self._execute_jobs(
            jobs=jobs,
            max_scenes=max_scenes,
            enable_cloud_masking=enable_cloud_masking,
            verbose=verbose,
            max_workers=max_workers,
            debug=debug,
            log_prefix="ndvi_aggregated_composites",
        )

    # ------------------------------------------------------------------
    # Public API: monthly + quarterly fallback
    # ------------------------------------------------------------------
    def run_monthly_with_fallback(
        self,
        *,
        aggregated_root: Path,
        max_scenes: int | None,
        min_scenes: int,
        fallback: bool,
        enable_cloud_masking: bool,
        verbose: bool,
        # parallel knobs (pipeline-derived)
        max_workers: int = 4,
        debug: bool = False,
    ) -> List[Tuple[str, Path, Path]]:
        df = self._discover(aggregated_root)

        months = sorted(df["month"].unique())
        jobs: List[Tuple[str, List[Path]]] = []

        for m in months:
            subset = df[df["month"] == m]
            if len(subset) >= min_scenes:
                jobs.append((m, subset["path"].tolist()))

        if fallback:
            sparse_months = [m for m in months if len(df[df["month"] == m]) < min_scenes]
            sparse_quarters = sorted(df[df["month"].isin(sparse_months)]["quarter"].unique())

            for q in sparse_quarters:
                q_paths = df[df["quarter"] == q]["path"].tolist()
                if q_paths:
                    jobs.append((q, q_paths))

        # ensure target grid is built before threads start
        _ = self._get_target()

        return self._execute_jobs(
            jobs=jobs,
            max_scenes=max_scenes,
            enable_cloud_masking=enable_cloud_masking,
            verbose=verbose,
            max_workers=max_workers,
            debug=debug,
            log_prefix="ndvi_aggregated_composites",
        )

    # ------------------------------------------------------------------
    # Parallel execution + logging (modelled after TimestampsAggregationBuilder)
    # ------------------------------------------------------------------
    def _execute_jobs(
        self,
        *,
        jobs: List[Tuple[str, List[Path]]],
        max_scenes: int | None,
        enable_cloud_masking: bool,
        verbose: bool,
        max_workers: int,
        debug: bool,
        log_prefix: str,
    ) -> List[Tuple[str, Path, Path]]:
        """
        jobs: [(label, [folders...]), ...]
        Returns: [(label, out_tif, meta_json), ...] for successful jobs only.
        Also writes:
          outputs/tables/{log_prefix}_status.csv
          outputs/tables/{log_prefix}_summary.csv
        """
        status_rows: List[Dict[str, Any]] = []
        summary_rows: List[Dict[str, Any]] = []
        log_lock = Lock()

        def _log_status(d: Dict[str, Any]) -> None:
            with log_lock:
                status_rows.append(d)

        def _log_summary(d: Dict[str, Any]) -> None:
            with log_lock:
                summary_rows.append(d)

        def _process_job(label: str, folders: List[Path]) -> Optional[Tuple[str, Path, Path]]:
            """
            Worker-safe. Never raises unless debug=True.
            """
            out_tif: Optional[Path] = None
            meta_path: Optional[Path] = None

            try:
                out_tif, meta_path, info = self._run_group(
                    label=label,
                    folders=folders,
                    max_scenes=max_scenes,
                    enable_cloud_masking=enable_cloud_masking,
                    verbose=verbose,
                )

                _log_status(
                    {
                        "label": label,
                        "status": "success",
                        "message": "OK",
                    }
                )

                _log_summary(
                    {
                        "label": label,
                        "success": True,
                        "output_tif": str(out_tif),
                        "metadata_json": str(meta_path),
                        "scenes_used": info.get("scenes_used"),
                        "scenes_skipped": info.get("scenes_skipped"),
                        "ndvi_min": info.get("ndvi_min"),
                        "ndvi_max": info.get("ndvi_max"),
                        "cloud_masking": bool(enable_cloud_masking),
                        "folders": str([str(p) for p in folders]),
                        "error_message": "",
                    }
                )

                return (label, out_tif, meta_path)

            except Exception as e:
                if debug:
                    raise

                msg = f"Exception in worker for label={label}: {e}"
                if verbose:
                    print(f"[WARN] {msg}")

                _log_status(
                    {
                        "label": label,
                        "status": "exception",
                        "message": msg,
                    }
                )

                _log_summary(
                    {
                        "label": label,
                        "success": False,
                        "output_tif": str(out_tif) if out_tif else "",
                        "metadata_json": str(meta_path) if meta_path else "",
                        "scenes_used": "",
                        "scenes_skipped": "",
                        "ndvi_min": "",
                        "ndvi_max": "",
                        "cloud_masking": bool(enable_cloud_masking),
                        "folders": str([str(p) for p in folders]),
                        "error_message": msg,
                    }
                )
                return None

        # ---------------- Execution ----------------
        results: List[Tuple[str, Path, Path]] = []

        if debug or max_workers <= 1:
            print("[INFO] Running NDVI composites in DEBUG (sequential) mode")
            for label, folders in tqdm(jobs, desc="NDVI composites", unit="period"):
                r = _process_job(label, folders)
                if r:
                    results.append(r)
        else:
            if max_workers <= 0:
                max_workers = 4
            print(f"[INFO] Running NDVI composites in parallel with max_workers={max_workers}")

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = {ex.submit(_process_job, label, folders): label for label, folders in jobs}

                for fut in tqdm(
                    as_completed(futs),
                    total=len(futs),
                    desc="NDVI composites",
                    unit="period",
                ):
                    try:
                        r = fut.result()
                        if r:
                            results.append(r)
                    except Exception as e:
                        # Should be rare, but keep it worker-safe.
                        print(f"[WARN] Worker raised unhandled exception: {e}")

        # ---------------- Write logs ----------------
        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)

        status_df = pd.DataFrame(status_rows)
        summary_df = pd.DataFrame(summary_rows)

        status_csv = RepoPaths.table(f"{log_prefix}_status.csv")
        summary_csv = RepoPaths.table(f"{log_prefix}_summary.csv")

        status_df.to_csv(status_csv, index=False)
        summary_df.to_csv(summary_csv, index=False)

        print(f"[OK] Status written → {status_csv}")
        print(f"[OK] Summary written → {summary_csv}")
        if not summary_df.empty and "success" in summary_df.columns:
            print("[INFO] Summary counts:", summary_df["success"].value_counts().to_dict())

        return results

    # ------------------------------------------------------------------
    # Core NDVI composite generation (single label)
    # ------------------------------------------------------------------
    def _run_group(
        self,
        *,
        label: str,
        folders: List[Path],
        max_scenes: int | None,
        enable_cloud_masking: bool,
        verbose: bool,
    ) -> Tuple[Path, Path, Dict[str, Any]]:
        """
        Returns:
          (out_tif, meta_json, info_dict)

        info_dict includes scenes_used, scenes_skipped, ndvi_min, ndvi_max.
        """
        if max_scenes is not None and max_scenes > 0:
            folders = folders[:max_scenes]

        target = self._get_target()

        ndvi_stack: List[np.ndarray] = []
        scenes_used = 0
        scenes_skipped = 0

        for folder in folders:
            b04 = self._first_tif(folder, "B04")
            b08 = self._first_tif(folder, "B08")
            scl = self._first_tif(folder, "SCL")

            if b04 is None or b08 is None:
                scenes_skipped += 1
                if verbose:
                    print(f"[WARN] Missing B04/B08 in {folder}, skipping.")
                continue

            try:
                with rasterio.open(b04) as ds_r, rasterio.open(b08) as ds_n:
                    red = ds_r.read(1).astype(np.float32)
                    nir = ds_n.read(1).astype(np.float32)

                    nd_native = self.ndvi.compute_ndvi(red, nir)

                    nd_target = np.empty((target.height, target.width), dtype=np.float32)
                    reproject(
                        source=nd_native,
                        destination=nd_target,
                        src_transform=ds_r.transform,
                        src_crs=ds_r.crs,
                        dst_transform=target.transform,
                        dst_crs=target.crs,
                        resampling=Resampling.bilinear,
                        dst_nodata=np.nan,
                    )

                # optional cloud mask
                if enable_cloud_masking and scl is not None:
                    try:
                        with rasterio.open(scl) as sds:
                            scl_native = sds.read(1)
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

                        invalid = self.masker.build_invalid_mask_from_scl(scl_target, scl_nodata)
                        nd_target[invalid] = np.nan
                    except Exception as e:
                        if verbose:
                            print(f"[WARN] Cloud masking failed for {folder}: {e}")

                # AOI mask
                nd_target[~target.aoi_mask] = np.nan

                ndvi_stack.append(nd_target)
                scenes_used += 1

            except Exception as e:
                scenes_skipped += 1
                if verbose:
                    print(f"[WARN] Failed to process {folder}: {e}")

        if scenes_used == 0:
            raise RuntimeError(
                f"No valid NDVI scenes for label={label} (folders={len(folders)}, skipped={scenes_skipped})."
            )

        stack = np.stack(ndvi_stack, axis=0)

        # Suppress the "All-NaN slice encountered" warning locally.
        # It's expected when some pixels are NaN across all scenes; we handle that via NaNs anyway.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="All-NaN slice encountered",
                category=RuntimeWarning,
            )
            composite = np.nanmedian(stack, axis=0).astype(np.float32)

        valid = composite[~np.isnan(composite)]
        ndvi_min = float(valid.min()) if valid.size else None
        ndvi_max = float(valid.max()) if valid.size else None

        out_tif = self._write_tif(label, composite)
        meta_path = self._write_metadata(
            label=label,
            out_tif=out_tif,
            composite=composite,
            scenes_used=scenes_used,
            scenes_skipped=scenes_skipped,
            folders=folders,
            enable_cloud_masking=enable_cloud_masking,
            ndvi_min=ndvi_min,
            ndvi_max=ndvi_max,
        )

        info = {
            "scenes_used": scenes_used,
            "scenes_skipped": scenes_skipped,
            "ndvi_min": ndvi_min,
            "ndvi_max": ndvi_max,
        }

        return out_tif, meta_path, info

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _first_tif(self, folder: Path, pattern: str) -> Path | None:
        candidates = sorted(folder.glob(f"*{pattern}*.tif"))
        return candidates[0] if candidates else None

    def _get_target(self):
        if self._target is None:
            self._target = AoiTargetGrid(
                aoi_path=self.aoi_path,
                target_crs="EPSG:32634",
                resolution=10.0,
            ).build()
        return self._target

    # ------------------------------------------------------------------
    # Outputs
    # ------------------------------------------------------------------
    def _write_tif(self, label: str, arr: np.ndarray) -> Path:
        out_dir = RepoPaths.OUTPUTS / "cogs"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_tif = out_dir / f"ndvi_{label}_{self.aoi_id}.tif"
        target = self._get_target()

        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "count": 1,
            "crs": target.crs,
            "transform": target.transform,
            "width": target.width,
            "height": target.height,
            "nodata": -9999.0,
            "compress": "deflate",
            "tiled": True,
        }

        data = np.where(np.isnan(arr), profile["nodata"], arr).astype(np.float32)

        with rasterio.open(out_tif, "w", **profile) as dst:
            dst.write(data, 1)
            dst.build_overviews([2, 4, 8, 16], Resampling.nearest)
            dst.update_tags(ns="rio_overview", resampling="nearest")

        return out_tif

    def _write_metadata(
        self,
        *,
        label: str,
        out_tif: Path,
        composite: np.ndarray,
        scenes_used: int,
        scenes_skipped: int,
        folders: List[Path],
        enable_cloud_masking: bool,
        ndvi_min: float | None,
        ndvi_max: float | None,
    ) -> Path:
        meta_dir = RepoPaths.OUTPUTS / "metadata"
        meta_dir.mkdir(parents=True, exist_ok=True)

        out_meta = meta_dir / f"ndvi_{label}_{self.aoi_id}.json"

        doc: Dict[str, Any] = {
            "label": label,
            "aoi_id": self.aoi_id,
            "output_tif": str(out_tif),
            "scenes_used": scenes_used,
            "scenes_skipped": scenes_skipped,
            "folders": [str(p) for p in folders],
            "cloud_masking": bool(enable_cloud_masking),
            "ndvi_min": ndvi_min,
            "ndvi_max": ndvi_max,
        }

        with out_meta.open("w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)

        return out_meta