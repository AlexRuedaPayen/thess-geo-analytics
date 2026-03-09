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
# -------------------------------------------------------------------
_COG_LABEL_RE = re.compile(
    r"ndvi_anomaly_(\d{4})-(\d{2}|Q[1-4])_",
    re.IGNORECASE,
)


def parse_cog_timestamp(path: Path) -> np.datetime64:
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
    ndvi_dir: Path | None = None
    pattern: str = "ndvi_anomaly_*.tif"

    aoi_id: str | None = None

    out_path: Path | None = None

    diagnostics_csv: Path | None = None

    tile_height: int = 512
    tile_width: int = 512

    tile_workers: int | None = None


class BuildPixelFeaturesPipeline:
    def run(self, params: BuildPixelFeaturesParams) -> Path:

        # --------------------------------------------------------
        # Resolve paths lazily (important for THESS_RUN_ROOT)
        # --------------------------------------------------------
        ndvi_dir = params.ndvi_dir or RepoPaths.outputs("cogs")

        if params.out_path is None:
            aoi = params.aoi_id or "aoi"
            out_path = RepoPaths.outputs("cogs") / f"pixel_features_7d_{aoi}.tif"
        else:
            out_path = params.out_path

        diagnostics_csv = params.diagnostics_csv or RepoPaths.table(
            "pixel_features_diagnostics.csv"
        )

        diagnostics: list[dict] = []

        def log_step(step: str, status: str, message: str = "", **extra) -> None:
            row = {"step": step, "status": status, "message": message}
            for k, v in extra.items():
                if isinstance(v, Path):
                    row[k] = str(v)
                elif isinstance(v, np.generic):
                    row[k] = v.item()
                else:
                    row[k] = v
            diagnostics.append(row)

        def flush_diagnostics():
            diagnostics_csv.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(diagnostics).to_csv(diagnostics_csv, index=False)

        try:
            # --------------------------------------------------------
            # 1. Discover anomaly COGs
            # --------------------------------------------------------
            log_step(
                "discover_cogs",
                "start",
                ndvi_dir=str(ndvi_dir),
                pattern=params.pattern,
                aoi_filter=params.aoi_id,
            )

            all_paths: List[Path] = sorted(ndvi_dir.glob(params.pattern))
            if not all_paths:
                msg = f"No NDVI anomaly COGs found in {ndvi_dir} with pattern {params.pattern}"
                log_step("discover_cogs", "error", msg, n_raw_cogs=0)
                raise FileNotFoundError(msg)

            cog_paths: List[Path] = []
            for p in all_paths:
                name = p.name.lower()

                if "climatology" in name or "median" in name:
                    continue

                if not _COG_LABEL_RE.search(p.name):
                    continue

                if params.aoi_id is not None:
                    if not name.endswith(f"_{params.aoi_id.lower()}.tif"):
                        continue

                cog_paths.append(p)

            if not cog_paths:
                msg = (
                    f"After filtering, no anomaly COGs remained in {ndvi_dir} "
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
            # 2. Parse timestamps
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
            # 3. Inspect first COG
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
            # 4. Prepare output raster
            # --------------------------------------------------------
            out_nodata = -9999.0
            out_profile = profile.copy()
            out_profile.update(
                driver="GTiff",
                count=7,
                dtype="float32",
                nodata=out_nodata,
                compress="deflate",
            )

            if height >= 16 and width >= 16:
                out_profile.update(
                    tiled=True,
                    blockxsize=16,
                    blockysize=16,
                )

            out_path.parent.mkdir(parents=True, exist_ok=True)

            extractor = NdviFeatureExtractor()

            # --------------------------------------------------------
            # 5. Open COGs
            # --------------------------------------------------------
            srcs = [rasterio.open(p) for p in cog_paths]

            try:
                tile_h = params.tile_height
                tile_w = params.tile_width

                n_tiles_y = math.ceil(height / tile_h)
                n_tiles_x = math.ceil(width / tile_w)
                total_tiles = n_tiles_y * n_tiles_x

                print("[INFO] Using serial tile processing.")
                print(f"[INFO] Starting tiled feature computation on {total_tiles} tiles…")

                with rasterio.open(out_path, "w", **out_profile) as dst:
                    with tqdm(total=total_tiles, desc="Pixel features", unit="tile") as pbar:

                        for ty in range(n_tiles_y):
                            row_off = ty * tile_h
                            h = min(tile_h, height - row_off)

                            for tx in range(n_tiles_x):
                                col_off = tx * tile_w
                                w = min(tile_w, width - col_off)

                                window = Window(col_off, row_off, w, h)

                                T = len(srcs)
                                stack = np.empty((T, h, w), dtype=np.float32)

                                for t_idx, src in enumerate(srcs):
                                    arr = src.read(1, window=window).astype(np.float32)

                                    src_nodata = src.nodata if src.nodata is not None else nodata_in
                                    if src_nodata is not None:
                                        arr[arr == src_nodata] = np.nan

                                    stack[t_idx] = arr

                                valid_mask = np.isfinite(stack).any(axis=0)

                                if not valid_mask.any():
                                    feats_tile = np.full((h, w, 7), np.nan, dtype=np.float32)
                                else:
                                    feats_tile = extractor.compute_features(stack, timestamps)

                                # Force pixels outside the valid AOI footprint to nodata for all 7 bands
                                feats_tile[~valid_mask, :] = np.nan

                                feats_tile = np.where(np.isnan(feats_tile), out_nodata, feats_tile)

                                for band_idx in range(7):
                                    dst.write(
                                        feats_tile[:, :, band_idx].astype(np.float32),
                                        band_idx + 1,
                                        window=window,
                                    )

                                pbar.update(1)

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

            flush_diagnostics()
            return out_path

        except Exception as e:
            log_step("pipeline", "error", f"{type(e).__name__}: {e}")
            flush_diagnostics()
            raise