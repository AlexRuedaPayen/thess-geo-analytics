from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List

import math
import re

import numpy as np
import rasterio
from rasterio.windows import Window
from tqdm import tqdm

from thess_geo_analytics.geo.NdviFeatureExtractor import (
    NdviFeatureExtractor,
)
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
    ndvi_dir: Path = RepoPaths.OUTPUTS / "cogs"
    pattern: str = "ndvi_anomaly_*.tif"

    out_path: Path = RepoPaths.OUTPUTS / "cogs" / "pixel_features_7d.tif"

    # Tiling to avoid OOM
    tile_height: int = 512
    tile_width: int = 512


class BuildPixelFeaturesPipeline:
    def run(self, params: BuildPixelFeaturesParams) -> Path:
        # --------------------------------------------------------
        # 1. Find anomaly COGs
        # --------------------------------------------------------
        all_paths: List[Path] = sorted(params.ndvi_dir.glob(params.pattern))
        if not all_paths:
            raise FileNotFoundError(
                f"No NDVI anomaly COGs found in {params.ndvi_dir} with pattern {params.pattern}"
            )

        cog_paths: List[Path] = []
        for p in all_paths:
            name = p.name.lower()

            # hard filter out climatology / baseline if pattern ever becomes too broad
            if "climatology" in name or "median" in name:
                # print(f"[SKIP] Climatology/baseline COG: {p.name}")
                continue

            if not _COG_LABEL_RE.search(p.name):
                # This catches plain ndvi_YYYY-MM_*.tif etc.
                # print(f"[SKIP] Non-anomaly COG: {p.name}")
                continue

            cog_paths.append(p)

        if not cog_paths:
            raise FileNotFoundError(
                f"After filtering, no anomaly COGs remained in {params.ndvi_dir} "
                f"(pattern={params.pattern}). Make sure you generated ndvi_anomaly_*.tif."
            )

        # --------------------------------------------------------
        # 2. Build timestamps from anomaly filenames
        # --------------------------------------------------------
        timestamps = [parse_cog_timestamp(p) for p in cog_paths]
        timestamps = np.array(timestamps)

        order = np.argsort(timestamps)
        timestamps = timestamps[order]
        cog_paths = [cog_paths[i] for i in order]

        # --------------------------------------------------------
        # 3. Inspect first COG for spatial metadata
        # --------------------------------------------------------
        first_path = cog_paths[0]
        with rasterio.open(first_path) as src0:
            profile = src0.profile
            height = src0.height
            width = src0.width
            nodata = src0.nodata

        # --------------------------------------------------------
        # 4. Prepare output 7-band GeoTIFF
        # --------------------------------------------------------
        out_profile = profile.copy()
        out_profile.update(
            driver="GTiff",
            count=7,
            dtype="float32",
            nodata=np.nan,
            compress="deflate",
            tiled=True,
        )

        out_path = params.out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)

        extractor = NdviFeatureExtractor()

        # Open inputs once
        srcs = [rasterio.open(p) for p in cog_paths]
        try:
            tile_h = params.tile_height
            tile_w = params.tile_width

            n_tiles_y = math.ceil(height / tile_h)
            n_tiles_x = math.ceil(width / tile_w)
            total_tiles = n_tiles_y * n_tiles_x

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

                            for t, src in enumerate(srcs):
                                arr = src.read(1, window=window).astype(np.float32)
                                if nodata is not None:
                                    arr[arr == nodata] = np.nan
                                stack[t] = arr

                            feats_tile = extractor.compute_features(stack, timestamps)
                            # feats_tile: (h, w, 7)

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

        print(f"[OK] Pixel anomaly features written → {out_path}")
        return out_path


if __name__ == "__main__":
    params = BuildPixelFeaturesParams()
    BuildPixelFeaturesPipeline().run(params)