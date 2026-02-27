from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List

import math
import re

import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window
from tqdm import tqdm

from thess_geo_analytics.geo.NdviFeatureExtractor import NdviFeatureExtractor
from thess_geo_analytics.utils.RepoPaths import RepoPaths

# -------------------------------------------------------------------
# Robust filename → timestamp parser
#   Supports:
#     ndvi_anomaly_YYYY-MM_<aoi>.tif
#     ndvi_anomaly_YYYY-QN_<aoi>.tif
# (plain ndvi_ and climatology* will be filtered out earlier)
# -------------------------------------------------------------------
_COG_LABEL_RE = re.compile(
    r"ndvi_anomaly_(\d{4})-(\d{2}|Q[1-4])_",
    re.IGNORECASE,
)


def parse_cog_timestamp(path: Path) -> np.datetime64:
    """
    Extract representative timestamp from *anomaly* COG filename.

    Supports:
      - ndvi_anomaly_YYYY-MM_<aoi>.tif
      - ndvi_anomaly_YYYY-QN_<aoi>.tif

    For quarters, map to mid-month of quarter:
      Q1 -> Feb, Q2 -> May, Q3 -> Aug, Q4 -> Nov.
    """
    m = _COG_LABEL_RE.search(path.name)
    if not m:
        raise ValueError(f"Cannot extract anomaly period label from filename: {path.name!r}")

    year_str, suffix = m.groups()
    year = int(year_str)

    if suffix.startswith("Q"):
        q = int(suffix[1])
        month = {1: 2, 2: 5, 3: 8, 4: 11}[q]
    else:
        month = int(suffix)

    return np.datetime64(f"{year}-{month:02d}-15")


@dataclass
class BuildPixelFeaturesParams:
    # Where anomaly COGs live
    ndvi_dir: Path = RepoPaths.OUTPUTS / "cogs"
    pattern: str = "ndvi_anomaly_*.tif"

    # Optional: filter by AOI suffix in filename (e.g. "_el522.tif")
    # If None, accept any AOI.
    aoi_id: str | None = None

    # Output 7-band GeoTIFF
    out_path: Path = RepoPaths.OUTPUTS / "cogs" / "pixel_features_7d.tif"

    # Diagnostics CSV
    diagnostics_csv: Path = RepoPaths.table("pixel_features_diagnostics.csv")

    # Tiling to avoid OOM
    tile_height: int = 512
    tile_width: int = 512


class BuildPixelFeaturesPipeline:
    def run(self, params: BuildPixelFeaturesParams) -> Path:
        diagnostics: list[dict] = []

        def log_step(step: str, status: str, message: str = "", **extra) -> None:
            row = {"step": step, "status": status, "message": message}
            for k, v in extra.items():
                # Make sure values are JSON/CSV-friendly
                if isinstance(v, Path):
                    row[k] = str(v)
                elif isinstance(v, np.generic):
                    row[k] = v.item()
                else:
                    row[k] = v
            diagnostics.append(row)

        def flush_diagnostics():
            params.diagnostics_csv.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(diagnostics).to_csv(params.diagnostics_csv, index=False)

        try:
            # --------------------------------------------------------
            # 1. Find anomaly COGs
            # --------------------------------------------------------
            log_step(
                "discover_cogs",
                "start",
                ndvi_dir=str(params.ndvi_dir),
                pattern=params.pattern,
                aoi_filter=params.aoi_id,
            )

            all_paths: List[Path] = sorted(params.ndvi_dir.glob(params.pattern))
            if not all_paths:
                msg = f"No NDVI anomaly COGs found in {params.ndvi_dir} with pattern {params.pattern}"
                log_step("discover_cogs", "error", msg, n_raw_cogs=0)
                raise FileNotFoundError(msg)

            cog_paths: List[Path] = []
            for p in all_paths:
                name = p.name.lower()

                # hard filter out climatology / baseline if pattern ever becomes too broad
                if "climatology" in name or "median" in name:
                    continue

                if not _COG_LABEL_RE.search(p.name):
                    # This catches plain ndvi_YYYY-MM_*.tif etc.
                    continue

                if params.aoi_id is not None:
                    # Enforce AOI-specific suffix if requested
                    if not name.endswith(f"_{params.aoi_id.lower()}.tif"):
                        continue

                cog_paths.append(p)

            if not cog_paths:
                msg = (
                    f"After filtering, no anomaly COGs remained in {params.ndvi_dir} "
                    f"(pattern={params.pattern}, aoi_id={params.aoi_id}). "
                    f"Make sure you generated ndvi_anomaly_*.tif for the correct AOI."
                )
                log_step("discover_cogs", "error", msg, n_filtered_cogs=0)
                raise FileNotFoundError(msg)

            log_step(
                "discover_cogs",
                "ok",
                "Anomaly COGs discovered.",
                n_raw_cogs=len(all_paths),
                n_filtered_cogs=len(cog_paths),
            )

            # --------------------------------------------------------
            # 2. Build timestamps from anomaly filenames
            # --------------------------------------------------------
            timestamps = [parse_cog_timestamp(p) for p in cog_paths]
            timestamps = np.array(timestamps)

            order = np.argsort(timestamps)
            timestamps = timestamps[order]
            cog_paths = [cog_paths[i] for i in order]

            log_step(
                "timestamps",
                "ok",
                "Timestamps parsed and sorted.",
                n_timestamps=len(timestamps),
                first_timestamp=str(timestamps[0]),
                last_timestamp=str(timestamps[-1]),
            )

            # --------------------------------------------------------
            # 3. Inspect first COG for spatial metadata
            # --------------------------------------------------------
            first_path = cog_paths[0]
            with rasterio.open(first_path) as src0:
                profile = src0.profile
                height = src0.height
                width = src0.width
                nodata_in = src0.nodata

            log_step(
                "inspect_first_cog",
                "ok",
                "Spatial metadata read from first COG.",
                first_cog=str(first_path),
                width=int(width),
                height=int(height),
                nodata_in=nodata_in,
            )

            # --------------------------------------------------------
            # 4. Prepare output 7-band GeoTIFF
            # --------------------------------------------------------
            out_nodata = -9999.0  # keep a finite nodata for rasters
            out_profile = profile.copy()
            out_profile.update(
                driver="GTiff",
                count=7,
                dtype="float32",
                nodata=out_nodata,
                compress="deflate",
                tiled=True,
            )

            out_path = params.out_path
            out_path.parent.mkdir(parents=True, exist_ok=True)

            log_step(
                "prepare_output",
                "ok",
                "Output profile prepared.",
                out_path=str(out_path),
                out_nodata=out_nodata,
            )

            extractor = NdviFeatureExtractor()

            # Open inputs once
            srcs = [rasterio.open(p) for p in cog_paths]
            try:
                tile_h = params.tile_height
                tile_w = params.tile_width

                n_tiles_y = math.ceil(height / tile_h)
                n_tiles_x = math.ceil(width / tile_w)
                total_tiles = n_tiles_y * n_tiles_x

                empty_tiles = 0
                tiles_processed = 0

                log_step(
                    "tiling",
                    "ok",
                    "Tiling strategy computed.",
                    tile_height=tile_h,
                    tile_width=tile_w,
                    n_tiles_y=n_tiles_y,
                    n_tiles_x=n_tiles_x,
                    total_tiles=total_tiles,
                )

                with rasterio.open(out_path, "w", **out_profile) as dst:
                    with tqdm(total=total_tiles, desc="Pixel features", unit="tile") as pbar:
                        for ty in range(n_tiles_y):
                            row_off = ty * tile_h
                            h = min(tile_h, height - row_off)

                            for tx in range(n_tiles_x):
                                col_off = tx * tile_w
                                w = min(tile_w, width - col_off)

                                window = Window(col_off, row_off, w, h)

                                # Stack anomalies for this tile: (T, h, w)
                                T = len(srcs)
                                stack = np.empty((T, h, w), dtype=np.float32)

                                for t_idx, src in enumerate(srcs):
                                    arr = src.read(1, window=window).astype(np.float32)
                                    if nodata_in is not None:
                                        arr[arr == nodata_in] = np.nan
                                    stack[t_idx] = arr

                                valid_in_tile = np.isfinite(stack).sum()
                                tiles_processed += 1

                                if valid_in_tile == 0:
                                    # Entire tile is nodata → just fill with nodata
                                    empty_tiles += 1
                                    feats_tile = np.full(
                                        (h, w, 7), np.nan, dtype=np.float32
                                    )
                                else:
                                    feats_tile = extractor.compute_features(stack, timestamps)

                                # Convert NaNs → out_nodata before writing
                                feats_tile = np.where(np.isnan(feats_tile), out_nodata, feats_tile)

                                for band_idx in range(7):
                                    dst.write(
                                        feats_tile[:, :, band_idx].astype(np.float32),
                                        band_idx + 1,
                                        window=window,
                                    )

                                pbar.update(1)

                log_step(
                    "tiles",
                    "ok",
                    "All tiles processed.",
                    tiles_processed=tiles_processed,
                    total_tiles=total_tiles,
                    empty_tiles=empty_tiles,
                )

            finally:
                for src in srcs:
                    src.close()

            log_step(
                "pipeline",
                "ok",
                "Pixel anomaly features written successfully.",
                out_path=str(out_path),
            )

            print(f"[OK] Pixel anomaly features written → {out_path}")
            return out_path

        except Exception as e:
            # Log error and flush diagnostics, then re-raise
            log_step("pipeline", "error", f"{type(e).__name__}: {e}")
            flush_diagnostics()
            raise

        else:
            # Normal completion
            flush_diagnostics()