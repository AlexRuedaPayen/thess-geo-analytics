from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import rasterio


@dataclass(frozen=True)
class NdviConfig:
    # Output nodata value for NDVI GeoTIFF
    nodata: float = -9999.0


class NdviProcessor:
    def __init__(self, config: NdviConfig | None = None) -> None:
        self.config = config or NdviConfig()

    def _check_alignment(self, prof_a: dict, prof_b: dict) -> None:
        keys = ["crs", "transform", "width", "height"]
        for k in keys:
            if prof_a.get(k) != prof_b.get(k):
                raise ValueError(f"Raster alignment mismatch on '{k}': {prof_a.get(k)} != {prof_b.get(k)}")

    def compute_ndvi(self, red: np.ndarray, nir: np.ndarray) -> np.ndarray:
        """
        NDVI = (NIR - RED) / (NIR + RED)
        Returns float32 array with NaNs where invalid (divide-by-zero).
        """
        red_f = red.astype(np.float32)
        nir_f = nir.astype(np.float32)

        denom = nir_f + red_f
        with np.errstate(divide="ignore", invalid="ignore"):
            ndvi = (nir_f - red_f) / denom
            ndvi[denom == 0] = np.nan

        # Enforce bounds (acceptance wants values in [-1, 1])
        # NDVI can numerically drift slightly, clamp it.
        ndvi = np.clip(ndvi, -1.0, 1.0)

        return ndvi.astype(np.float32)

    def read_bands(
        self,
        b04_path: Path,
        b08_path: Path,
    ) -> Tuple[np.ndarray, np.ndarray, dict]:
        """
        Returns (red, nir, profile) from B04 and B08.
        Ensures they are aligned (same grid).
        """
        with rasterio.open(b04_path) as ds_red, rasterio.open(b08_path) as ds_nir:
            prof_red = ds_red.profile.copy()
            prof_nir = ds_nir.profile.copy()

            self._check_alignment(prof_red, prof_nir)

            red = ds_red.read(1)
            nir = ds_nir.read(1)

        # We'll output float32 NDVI on same grid
        out_profile = prof_red.copy()
        out_profile.update(
            dtype="float32",
            count=1,
            nodata=self.config.nodata,
        )
        return red, nir, out_profile

    def apply_mask_to_ndvi(self, ndvi: np.ndarray, invalid_mask: np.ndarray) -> np.ndarray:
        """
        Applies invalid mask by setting those pixels to NaN.
        """
        ndvi_masked = ndvi.copy()
        ndvi_masked[invalid_mask] = np.nan
        return ndvi_masked

    def to_nodata(self, ndvi_masked: np.ndarray) -> np.ndarray:
        """
        Converts NaNs to configured nodata value for GeoTIFF writing.
        """
        out = ndvi_masked.copy()
        out[np.isnan(out)] = self.config.nodata
        return out.astype(np.float32)
