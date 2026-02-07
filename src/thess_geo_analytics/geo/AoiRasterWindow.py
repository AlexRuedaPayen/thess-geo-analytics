from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import rasterio
from rasterio.windows import from_bounds, Window
from rasterio.windows import transform as window_transform
from rasterio.features import rasterize
import numpy as np


@dataclass(frozen=True)
class AoiWindowResult:
    window: Window
    transform: rasterio.Affine
    crs: object
    width: int
    height: int
    aoi_mask: np.ndarray  # True inside AOI


class AoiRasterWindow:
    """
    Builds a raster window & AOI mask in the raster CRS/grid for a given AOI polygon.
    This ensures output GeoTIFF extent matches AOI bounds (cropped to the AOI bounding box).
    """

    def __init__(self, aoi_path: Path) -> None:
        self.aoi_gdf = gpd.read_file(aoi_path)
        if self.aoi_gdf.empty:
            raise ValueError(f"AOI is empty: {aoi_path}")

        # dissolve to a single geometry (safe even if it's already single)
        self.geom = self.aoi_gdf.to_crs(self.aoi_gdf.crs).unary_union

    def build(self, reference_raster_path: Path) -> AoiWindowResult:
        with rasterio.open(reference_raster_path) as ds:
            raster_crs = ds.crs

            aoi_in_raster_crs = gpd.GeoSeries([self.geom], crs=self.aoi_gdf.crs).to_crs(raster_crs).iloc[0]
            minx, miny, maxx, maxy = aoi_in_raster_crs.bounds

            # 1) window from AOI bounds in raster grid
            win = from_bounds(minx, miny, maxx, maxy, transform=ds.transform)
            win = win.round_offsets().round_lengths()

            # 2) clip window to raster bounds to avoid out-of-bounds mismatch
            full = Window(0, 0, ds.width, ds.height)

            col0 = max(win.col_off, full.col_off)
            row0 = max(win.row_off, full.row_off)
            col1 = min(win.col_off + win.width, full.col_off + full.width)
            row1 = min(win.row_off + win.height, full.row_off + full.height)

            win = Window(col0, row0, col1 - col0, row1 - row0).round_offsets().round_lengths()

            w = int(win.width)
            h = int(win.height)
            if w <= 0 or h <= 0:
                raise ValueError("AOI window has non-positive width/height after clipping. CRS mismatch or AOI outside tile?")

            win_transform = window_transform(win, ds.transform)

            # 3) AOI mask on window grid (True inside polygon)
            mask = rasterize(
                [(aoi_in_raster_crs, 1)],
                out_shape=(h, w),
                transform=win_transform,
                fill=0,
                dtype="uint8",
                all_touched=False,
            ).astype(bool)

            return AoiWindowResult(
                window=win,
                transform=win_transform,
                crs=raster_crs,
                width=w,
                height=h,
                aoi_mask=mask,
            )

