from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Dict, Any

import json
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

    This builder is intentionally "dumb": it does not know about configs or
    modes. All behavioral parameters are provided by the pipeline.
    """

    def __init__(self, *, aoi_path: Path, aoi_id: str) -> None:
        self.aoi_path = aoi_path
        self.aoi_id = aoi_id

        self.ndvi = NdviProcessor()
        self.masker = CloudMasker()

        self._target = None  # lazy AoiTargetGrid result

    # ------------------------------------------------------------------
    # Discovery of aggregated timestamp folders
    # ------------------------------------------------------------------
    def _discover(self, root: Path) -> pd.DataFrame:
        """
        Discover timestamp directories and parse their names into datetimes.

        Folder names currently look like:

            2021-02-27 09_20_31.024000+00_00

        which we normalise to an ISO-8601-like string:

            2021-02-27T09:20:31.024000+00:00
        """
        rows: List[Dict[str, Any]] = []

        if not root.exists():
            raise FileNotFoundError(f"Aggregated root does not exist: {root}")

        for p in sorted(root.iterdir()):
            if not p.is_dir():
                continue

            ts_name = p.name

            # First attempt: direct parse
            dt = pd.to_datetime(ts_name, utc=True, errors="coerce")

            # If that fails, try to normalise the known pattern
            if pd.isna(dt):
                # "2021-02-27 09_20_31.024000+00_00"
                #  -> "2021-02-27T09:20:31.024000+00:00"
                normalised = ts_name.replace(" ", "T").replace("_", ":")
                dt = pd.to_datetime(normalised, utc=True, errors="coerce")

            if pd.isna(dt):
                # Cannot interpret as timestamp → skip silently
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
    # Public API: per-timestamp strategy
    # ------------------------------------------------------------------
    def run_all_timestamps(
        self,
        *,
        aggregated_root: Path,
        max_scenes: int | None,
        enable_cloud_masking: bool,
        verbose: bool,
    ) -> List[Tuple[str, Path, Path]]:
        """
        Build one NDVI composite per timestamp folder.
        """
        df = self._discover(aggregated_root)

        outputs: List[Tuple[str, Path, Path]] = []
        for _, row in tqdm(df.iterrows(), total=len(df), desc="NDVI per-timestamp"):
            label = row["timestamp"]
            folders = [row["path"]]

            out_tif, meta = self._run_group(
                label=label,
                folders=folders,
                max_scenes=max_scenes,
                enable_cloud_masking=enable_cloud_masking,
                verbose=verbose,
            )
            outputs.append((label, out_tif, meta))

        return outputs

    # ------------------------------------------------------------------
    # Public API: monthly + quarterly fallback strategy
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
    ) -> List[Tuple[str, Path, Path]]:
        """
        Build NDVI composites grouped by month; if some months are too sparse,
        group their timestamps into quarterly composites.
        """
        df = self._discover(aggregated_root)

        months = sorted(df["month"].unique())
        jobs: List[Tuple[str, List[Path]]] = []

        # 1) Monthly composites where we have enough timestamps
        for m in months:
            subset = df[df["month"] == m]
            if len(subset) >= min_scenes:
                jobs.append((m, subset["path"].tolist()))

        # 2) Fallback to quarters for sparse months
        if fallback:
            sparse_months = [m for m in months if len(df[df["month"] == m]) < min_scenes]
            sparse_quarters = sorted(df[df["month"].isin(sparse_months)]["quarter"].unique())

            for q in sparse_quarters:
                q_paths = df[df["quarter"] == q]["path"].tolist()
                if q_paths:
                    jobs.append((q, q_paths))

        outputs: List[Tuple[str, Path, Path]] = []
        for label, paths in tqdm(jobs, desc="NDVI grouped"):
            out_tif, meta = self._run_group(
                label=label,
                folders=paths,
                max_scenes=max_scenes,
                enable_cloud_masking=enable_cloud_masking,
                verbose=verbose,
            )
            outputs.append((label, out_tif, meta))

        return outputs

    # ------------------------------------------------------------------
    # Core NDVI composite generation
    # ------------------------------------------------------------------
    def _run_group(
        self,
        *,
        label: str,
        folders: List[Path],
        max_scenes: int | None,
        enable_cloud_masking: bool,
        verbose: bool,
    ) -> Tuple[Path, Path]:
        """
        Build a single NDVI composite for the given label (month/quarter/timestamp)
        from the list of aggregated folders.
        """
        if max_scenes is not None and max_scenes > 0:
            folders = folders[:max_scenes]

        ndvi_stack: List[np.ndarray] = []
        scenes_used = 0
        scenes_skipped = 0

        for folder in folders:
            # Find B04 / B08 (and optionally SCL)
            b04 = self._first_tif(folder, "B04")
            b08 = self._first_tif(folder, "B08")
            scl = self._first_tif(folder, "SCL")

            if b04 is None or b08 is None:
                scenes_skipped += 1
                if verbose:
                    print(f"[WARN] Missing B04/B08 in {folder}, skipping.")
                continue

            try:
                # --- Compute NDVI in native grid ---
                with rasterio.open(b04) as ds_r, rasterio.open(b08) as ds_n:
                    red = ds_r.read(1).astype(np.float32)
                    nir = ds_n.read(1).astype(np.float32)

                    nd_native = self.ndvi.compute_ndvi(red, nir)

                    target = self._get_target()
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

                # --- Optional cloud masking via SCL ---
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

                        invalid = self.masker.build_invalid_mask_from_scl(
                            scl_target,
                            scl_nodata,
                        )
                        nd_target[invalid] = np.nan

                    except Exception as e:
                        if verbose:
                            print(f"[WARN] Cloud masking failed for {folder}: {e}")

                # --- Apply AOI mask ---
                nd_target[~self._get_target().aoi_mask] = np.nan

                ndvi_stack.append(nd_target)
                scenes_used += 1

            except Exception as e:
                scenes_skipped += 1
                if verbose:
                    print(f"[WARN] Failed to process folder {folder}: {e}")

        if scenes_used == 0:
            raise RuntimeError(f"No valid NDVI scenes for label={label} (folders={len(folders)}, skipped={scenes_skipped}).")

        # Stack and composite (median)
        stack = np.stack(ndvi_stack, axis=0)
        composite = np.nanmedian(stack, axis=0).astype(np.float32)

        # Sanity: NDVI range
        valid = composite[~np.isnan(composite)]
        if valid.size > 0:
            vmin, vmax = float(valid.min()), float(valid.max())
            if vmin < -1.0001 or vmax > 1.0001 and verbose:
                print(f"[WARN] NDVI composite for {label} has values outside [-1,1]: min={vmin}, max={vmax}")

        out_tif = self._write_tif(label, composite)
        meta_path = self._write_metadata(
            label=label,
            out_tif=out_tif,
            composite=composite,
            scenes_used=scenes_used,
            scenes_skipped=scenes_skipped,
            folders=folders,
            enable_cloud_masking=enable_cloud_masking,
        )

        if verbose:
            print(
                f"[OK] label={label} scenes_used={scenes_used} skipped={scenes_skipped} "
                f"→ {out_tif}"
            )

        return out_tif, meta_path

    # ------------------------------------------------------------------
    # Helper: find first matching GeoTIFF in folder
    # ------------------------------------------------------------------
    def _first_tif(self, folder: Path, pattern: str) -> Path | None:
        """
        Return the first .tif in the folder whose name contains the given pattern.
        """
        candidates = sorted(folder.glob(f"*{pattern}*.tif"))
        return candidates[0] if candidates else None

    # ------------------------------------------------------------------
    # AOI target grid
    # ------------------------------------------------------------------
    def _get_target(self):
        if self._target is None:
            self._target = AoiTargetGrid(
                aoi_path=self.aoi_path,
                target_crs="EPSG:32634",
                resolution=10.0,
            ).build()
        return self._target

    # ------------------------------------------------------------------
    # Output writers
    # ------------------------------------------------------------------
    def _write_tif(self, label: str, arr: np.ndarray) -> Path:
        """
        Write a single-band NDVI GeoTIFF with COG-friendly tiling and overviews.
        """
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
            # basic overviews for COG-style performance
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
    ) -> Path:
        """
        Write a small JSON metadata sidecar for the composite.
        """
        meta_dir = RepoPaths.OUTPUTS / "metadata"
        meta_dir.mkdir(parents=True, exist_ok=True)

        out_meta = meta_dir / f"ndvi_{label}_{self.aoi_id}.json"

        valid = composite[~np.isnan(composite)]
        if valid.size > 0:
            ndvi_min = float(valid.min())
            ndvi_max = float(valid.max())
        else:
            ndvi_min = None
            ndvi_max = None

        doc: Dict[str, Any] = {
            "label": label,
            "aoi_id": self.aoi_id,
            "output_tif": str(out_tif),
            "scenes_used": scenes_used,
            "scenes_skipped": scenes_skipped,
            "folders": [str(p) for p in folders],
            "cloud_masking": enable_cloud_masking,
            "ndvi_min": ndvi_min,
            "ndvi_max": ndvi_max,
        }

        with out_meta.open("w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)

        return out_meta