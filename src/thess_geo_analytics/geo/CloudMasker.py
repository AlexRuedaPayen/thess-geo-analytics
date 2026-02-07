from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject


@dataclass(frozen=True)
class CloudMaskConfig:
    # SCL classes to mask (set to nodata)
    # 3 = cloud shadows
    # 8 = cloud medium prob
    # 9 = cloud high prob
    # 10 = thin cirrus
    masked_classes: Tuple[int, ...] = (3, 8, 9, 10)


class CloudMasker:
    def __init__(self, config: CloudMaskConfig | None = None) -> None:
        self.config = config or CloudMaskConfig()

    def build_invalid_mask_from_scl(self, scl: np.ndarray, scl_nodata: Optional[float]) -> np.ndarray:
        """
        Returns invalid_mask (True where pixel should be masked out).
        """
        invalid = np.isin(scl, np.array(self.config.masked_classes, dtype=scl.dtype))
        if scl_nodata is not None:
            invalid = invalid | (scl == scl_nodata)
        return invalid

    def read_scl_as_target_grid(
        self,
        scl_path: Path,
        target_profile: dict,
    ) -> Tuple[np.ndarray, Optional[float]]:
        """
        Reads SCL and reprojects/resamples it to match the target raster grid.
        Uses nearest-neighbor (categorical).
        Returns (scl_on_target_grid, scl_nodata).
        """
        with rasterio.open(scl_path) as src:
            scl_nodata = src.nodata

            dst = np.empty((target_profile["height"], target_profile["width"]), dtype=src.dtypes[0])

            # Reproject into target grid
            reproject(
                source=rasterio.band(src, 1),
                destination=dst,
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=target_profile["transform"],
                dst_crs=target_profile["crs"],
                dst_nodata=scl_nodata,
                resampling=Resampling.nearest,
            )

        return dst, scl_nodata
