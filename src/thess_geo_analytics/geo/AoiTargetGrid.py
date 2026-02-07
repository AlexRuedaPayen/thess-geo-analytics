from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import math

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.features import rasterize


@dataclass(frozen=True)
class AoiTargetGridResult:
    crs: rasterio.crs.CRS
    transform: rasterio.Affine
    width: int
    height: int
    aoi_mask: np.ndarray  # True inside AOI


class AoiTargetGrid:
    """
    Builds a fixed AOI-based target grid (extent == AOI bounds) in a chosen CRS + resolution.
    This is the correct basis for mosaicking multiple tiles/zones into one monthly product.
    """

    def __init__(self, aoi_path: Path, target_crs: str = "EPSG:32634", resolution: float = 10.0) -> None:
        self.aoi = gpd.read_file(aoi_path)
        if self.aoi.empty:
            raise ValueError(f"AOI is empty: {aoi_path}")
        self.target_crs = rasterio.crs.CRS.from_string(target_crs)
        self.res = float(resolution)

        geom = self.aoi.unary_union
        self.geom_target = gpd.GeoSeries([geom], crs=self.aoi.crs).to_crs(self.target_crs).iloc[0]

    def build(self) -> AoiTargetGridResult:
        minx, miny, maxx, maxy = self.geom_target.bounds

        # snap bounds to resolution grid (so output is stable)
        minx_s = math.floor(minx / self.res) * self.res
        miny_s = math.floor(miny / self.res) * self.res
        maxx_s = math.ceil(maxx / self.res) * self.res
        maxy_s = math.ceil(maxy / self.res) * self.res

        width = int((maxx_s - minx_s) / self.res)
        height = int((maxy_s - miny_s) / self.res)

        # rasterio transform from upper-left
        transform = from_origin(minx_s, maxy_s, self.res, self.res)

        mask = rasterize(
            [(self.geom_target, 1)],
            out_shape=(height, width),
            transform=transform,
            fill=0,
            dtype="uint8",
            all_touched=False,
        ).astype(bool)

        return AoiTargetGridResult(
            crs=self.target_crs,
            transform=transform,
            width=width,
            height=height,
            aoi_mask=mask,
        )
