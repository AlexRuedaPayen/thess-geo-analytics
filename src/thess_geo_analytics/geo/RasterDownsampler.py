from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np


@dataclass(frozen=True)
class DownsampleConfig:
    """
    Utilities for downsampling rasters by an integer factor.

    - For continuous rasters (B04/B08 reflectance, NDVI): use nanmean or nanmedian.
    - For categorical rasters (SCL): use mode (most frequent) or nearest.

    NOTE:
    This module operates on numpy arrays only (no rasterio IO here).
    """
    # How to downsample continuous data
    continuous_method: Literal["nanmean", "nanmedian"] = "nanmean"
    # How to downsample categorical data
    categorical_method: Literal["mode", "nearest"] = "mode"


class RasterDownsampler:
    def __init__(self, config: DownsampleConfig | None = None) -> None:
        self.config = config or DownsampleConfig()

    # ---------------------------
    # Continuous (float-like)
    # ---------------------------
    def downsample_continuous(self, arr: np.ndarray, factor: int) -> np.ndarray:
        """
        Downsample a 2D continuous raster by integer factor using nanmean/nanmedian.

        Crops array to multiples of factor (top-left aligned) to avoid padding artifacts.
        """
        if factor <= 1:
            return arr.astype(np.float32, copy=False)

        arr_f = arr.astype(np.float32, copy=False)

        h, w = arr_f.shape
        h2 = (h // factor) * factor
        w2 = (w // factor) * factor
        if h2 == 0 or w2 == 0:
            raise ValueError(f"Array too small ({h}x{w}) for factor={factor}")

        cropped = arr_f[:h2, :w2]
        # reshape to (H_out, factor, W_out, factor)
        blocks = cropped.reshape(h2 // factor, factor, w2 // factor, factor)

        if self.config.continuous_method == "nanmedian":
            out = np.nanmedian(blocks, axis=(1, 3))
        else:
            out = np.nanmean(blocks, axis=(1, 3))

        return out.astype(np.float32)

    # ---------------------------
    # Categorical (uint16-like)
    # ---------------------------
    def downsample_categorical(
        self,
        arr: np.ndarray,
        factor: int,
        *,
        nodata: int | None = 0,
        max_class: int = 255,
    ) -> np.ndarray:
        """
        Downsample a 2D categorical raster (e.g. SCL) by integer factor.

        Methods:
          - mode: choose the most frequent class per block (ignoring nodata if provided)
          - nearest: take the top-left pixel of each block

        `max_class` is used to size bincount; SCL is small (<= 11), so 255 is safe.
        """
        if factor <= 1:
            return arr.astype(np.uint16, copy=False)

        a = arr.astype(np.uint16, copy=False)
        h, w = a.shape
        h2 = (h // factor) * factor
        w2 = (w // factor) * factor
        if h2 == 0 or w2 == 0:
            raise ValueError(f"Array too small ({h}x{w}) for factor={factor}")

        cropped = a[:h2, :w2]

        if self.config.categorical_method == "nearest":
            # top-left pixel in each block
            return cropped[0:h2:factor, 0:w2:factor].astype(np.uint16)

        # mode per block (safe but loops; output is small so OK)
        out_h = h2 // factor
        out_w = w2 // factor
        out = np.zeros((out_h, out_w), dtype=np.uint16)

        for oy in range(out_h):
            y0 = oy * factor
            for ox in range(out_w):
                x0 = ox * factor
                block = cropped[y0 : y0 + factor, x0 : x0 + factor].ravel()

                if nodata is not None:
                    block = block[block != np.uint16(nodata)]

                if block.size == 0:
                    out[oy, ox] = np.uint16(nodata if nodata is not None else 0)
                    continue

                # bincount needs int
                bc = np.bincount(block.astype(np.int32), minlength=max_class + 1)
                out[oy, ox] = np.uint16(int(bc.argmax()))

        return out

    # ---------------------------
    # Smoke test
    # ---------------------------
    @staticmethod
    def smoke_test() -> None:
        print("=== RasterDownsampler Smoke Test ===")

        ds = RasterDownsampler(
            DownsampleConfig(continuous_method="nanmean", categorical_method="mode")
        )

        # ---- Continuous test (nanmean) ----
        base = np.arange(1, 37, dtype=np.float32).reshape(6, 6)
        out = ds.downsample_continuous(base, factor=3)

        # Means of the 3x3 blocks in a row-wise 6x6:
        # TL: mean([1,2,3,7,8,9,13,14,15]) = 8
        # TR: mean([4,5,6,10,11,12,16,17,18]) = 11
        # BL: mean([19,20,21,25,26,27,31,32,33]) = 26
        # BR: mean([22,23,24,28,29,30,34,35,36]) = 29
        expected = np.array([[8.0, 11.0], [26.0, 29.0]], dtype=np.float32)

        print("[continuous] input shape:", base.shape, "output shape:", out.shape)
        print("[continuous] output:\n", out)
        assert out.shape == (2, 2)
        assert np.allclose(out, expected), f"Expected {expected}, got {out}"

        # ---- Continuous with NaNs (nanmean should ignore) ----
        base2 = base.copy()
        base2[0, 0] = np.nan  # remove value=1 from first block
        out2 = ds.downsample_continuous(base2, factor=3)

        # First block mean becomes mean([2,3,7,8,9,13,14,15]) = 71/8 = 8.875
        assert np.isclose(out2[0, 0], 8.875), f"Expected 8.875, got {out2[0,0]}"
        print("[continuous+nan] OK")


if __name__ == "__main__":
    RasterDownsampler.smoke_test()