from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import rasterio
from rasterio.enums import Resampling
from affine import Affine
from tqdm import tqdm

from thess_geo_analytics.geo.RasterDownsampler import RasterDownsampler, DownsampleConfig


@dataclass(frozen=True)
class DownsampleAggregatedTimestampsParams:
    src_root: Path
    dst_root: Path

    # 1 = no downsampling
    # 10 -> 100m if input is 10m
    factor: int = 10

    bands: tuple[str, ...] = ("B04", "B08", "SCL")
    scl_nodata: int = 0

    # continuous: B04/B08
    continuous_method: str = "nanmean"  # or nanmedian
    # categorical: SCL
    categorical_method: str = "mode"    # or nearest


class DownsampleAggregatedTimestampsBuilder:
    """
    Reads mosaics in:
      <src_root>/<timestamp>/<band>.tif

    Writes downsampled mosaics in:
      <dst_root>/<timestamp>/<band>.tif

    Notes:
      - For tiny rasters (unit tests), we avoid tiling/overviews that GDAL rejects.
      - For large rasters, we write tiled + compressed with sensible overviews.
    """

    DEFAULT_OVERVIEWS = [2, 4, 8, 16]
    DEFAULT_TILE = 256

    def __init__(self, params: DownsampleAggregatedTimestampsParams) -> None:
        self.params = params
        self.ds = RasterDownsampler(
            DownsampleConfig(
                continuous_method=params.continuous_method,    # type: ignore[arg-type]
                categorical_method=params.categorical_method,  # type: ignore[arg-type]
            )
        )

    def run(self) -> List[Path]:
        src = self.params.src_root
        dst = self.params.dst_root

        if not src.exists():
            raise FileNotFoundError(f"src_root does not exist: {src}")

        dst.mkdir(parents=True, exist_ok=True)

        ts_folders = [p for p in sorted(src.iterdir()) if p.is_dir()]
        if not ts_folders:
            raise RuntimeError(f"No timestamp folders under {src}")

        written: List[Path] = []

        total_jobs = sum(
            1
            for ts_dir in ts_folders
            for band in self.params.bands
            if (ts_dir / f"{band}.tif").exists()
        )

        with tqdm(total=total_jobs, desc="Downsampling rasters", unit="raster") as pbar:
            for ts_dir in ts_folders:
                out_dir = dst / ts_dir.name
                out_dir.mkdir(parents=True, exist_ok=True)

                for band in self.params.bands:
                    in_tif = ts_dir / f"{band}.tif"
                    if not in_tif.exists():
                        continue

                    out_tif = out_dir / f"{band}.tif"
                    self._downsample_one(in_tif, out_tif, band=band)
                    written.append(out_tif)
                    pbar.update(1)

        return written

    def _downsample_one(self, in_tif: Path, out_tif: Path, *, band: str) -> None:
        factor = int(self.params.factor)
        if factor <= 0:
            raise ValueError(f"factor must be >= 1, got {factor}")

        band_upper = band.upper()

        with rasterio.open(in_tif) as src:
            arr = src.read(1)
            profile = src.profile.copy()

            # Normalize profile for our outputs (single-band GeoTIFF)
            profile.update(driver="GTiff", count=1, compress="deflate")

            # -----------------------------
            # NO DOWNSAMPLING
            # -----------------------------
            if factor == 1:
                out_arr = arr
                new_transform = src.transform
                new_h, new_w = out_arr.shape

                # Keep original dtype if we're not touching it.
                # (But make sure it's something rasterio understands)
                out_dtype = profile.get("dtype", arr.dtype)

            # -----------------------------
            # DOWNSAMPLING
            # -----------------------------
            else:
                if band_upper == "SCL":
                    out_arr = self.ds.downsample_categorical(
                        arr,
                        factor=factor,
                        nodata=self.params.scl_nodata,
                        max_class=255,
                    )
                    out_dtype = "uint16"
                else:
                    out_arr = self.ds.downsample_continuous(arr, factor=factor)
                    out_dtype = "float32"

                new_h, new_w = out_arr.shape
                new_transform = src.transform * Affine.scale(factor, factor)

            # Update geometry + dtype
            profile.update(
                height=new_h,
                width=new_w,
                transform=new_transform,
                dtype=out_dtype,
            )

            # -----------------------------
            # TILING (only when safe)
            # GDAL GeoTIFF tiling blocks must be multiples of 16.
            # Also, tiling tiny rasters is pointless and sometimes breaks tests.
            # -----------------------------
            if new_h >= self.DEFAULT_TILE and new_w >= self.DEFAULT_TILE:
                profile.update(
                    tiled=True,
                    blockxsize=self.DEFAULT_TILE,
                    blockysize=self.DEFAULT_TILE,
                )
            else:
                # Ensure we don't accidentally carry tiling/block sizes from the source.
                profile.pop("tiled", None)
                profile.pop("blockxsize", None)
                profile.pop("blockysize", None)

        out_tif.parent.mkdir(parents=True, exist_ok=True)

        with rasterio.open(out_tif, "w", **profile) as dst:
            dst.write(out_arr.astype(out_dtype, copy=False), 1)

            # -----------------------------
            # OVERVIEWS (only valid levels)
            # For small rasters, requesting [2,4,8,16] can fail (e.g., 2x2).
            # -----------------------------
            min_dim = min(profile["height"], profile["width"])
            valid_levels = [lvl for lvl in self.DEFAULT_OVERVIEWS if (min_dim // lvl) >= 1]

            # Also avoid overviews entirely for extremely small outputs.
            if valid_levels and min_dim >= 2:
                dst.build_overviews(valid_levels, Resampling.nearest)
                dst.update_tags(ns="rio_overview", resampling="nearest")