from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import numpy as np
import rasterio
from rasterio.transform import from_origin


@dataclass
class MiniDownsampleSceneConfig:
    """
    Generates ORIGINAL rasters (B04/B08/SCL) and EXPECTED downsampled rasters.

    Layout created under out_dir:

      out_dir/
        original/
          timestamp_001/
            B04.tif
            B08.tif
            SCL.tif
        expected/
          timestamp_001/
            B04.tif
            B08.tif
            SCL.tif
    """
    out_dir: Path
    name: str
    H: int
    W: int
    factor: int = 2
    pixel_size: float = 10.0
    crs: str = "EPSG:32634"
    scl_nodata: int = 0


class MiniDownsampleSceneGenerator:
    """
    Generate synthetic rasters + expected downsampled outputs.
    """

    def __init__(self, cfg: MiniDownsampleSceneConfig):
        self.cfg = cfg

    def generate(self) -> Tuple[Path, Path]:
        """
        Returns:
          (original_root, expected_root)

        Each contains:
          <root>/timestamp_001/{B04,B08,SCL}.tif
        """
        cfg = self.cfg
        f = int(cfg.factor)
        if f <= 0:
            raise ValueError("factor must be >= 1")

        out_dir = cfg.out_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        original_root = out_dir / "original"
        expected_root = out_dir / "expected"

        orig_ts = original_root / "timestamp_001"
        exp_ts = expected_root / "timestamp_001"
        orig_ts.mkdir(parents=True, exist_ok=True)
        exp_ts.mkdir(parents=True, exist_ok=True)

        H, W = cfg.H, cfg.W

        transform = from_origin(0, 0, cfg.pixel_size, cfg.pixel_size)

        profile_base = dict(
            driver="GTiff",
            count=1,
            crs=cfg.crs,
            transform=transform,
            compress="deflate",
        )

        # ----------------------------------------------------
        # Continuous data (B04/B08) - deterministic pattern
        # ----------------------------------------------------
        b04 = np.arange(1, H * W + 1, dtype="float32").reshape(H, W)
        b08 = b04 * 2.0

        def block_nanmean(a: np.ndarray, factor: int) -> np.ndarray:
            h2 = (a.shape[0] // factor) * factor
            w2 = (a.shape[1] // factor) * factor
            blocks = a[:h2, :w2].reshape(h2 // factor, factor, w2 // factor, factor)
            return np.nanmean(blocks, axis=(1, 3)).astype("float32")

        # Expected downsample for continuous: nanmean
        if f == 1:
            b04_expected = b04.copy()
            b08_expected = b08.copy()
            exp_transform = transform
        else:
            b04_expected = block_nanmean(b04, f)
            b08_expected = block_nanmean(b08, f)
            exp_transform = from_origin(0, 0, cfg.pixel_size * f, cfg.pixel_size * f)

        # ----------------------------------------------------
        # Categorical data (SCL) - 4 quadrants
        # ----------------------------------------------------
        scl = np.zeros((H, W), dtype="uint16")
        scl[: H // 2, : W // 2] = 4
        scl[: H // 2, W // 2 :] = 5
        scl[H // 2 :, : W // 2] = 7
        scl[H // 2 :, W // 2 :] = 8
        # nodata not used in this synthetic, but set in profile

        def block_mode(a: np.ndarray, factor: int, nodata: int) -> np.ndarray:
            h2 = (a.shape[0] // factor) * factor
            w2 = (a.shape[1] // factor) * factor
            out = np.zeros((h2 // factor, w2 // factor), dtype="uint16")

            for oy in range(out.shape[0]):
                for ox in range(out.shape[1]):
                    block = a[
                        oy * factor : (oy + 1) * factor,
                        ox * factor : (ox + 1) * factor,
                    ].ravel()
                    block = block[block != np.uint16(nodata)]
                    if block.size == 0:
                        out[oy, ox] = np.uint16(nodata)
                        continue
                    bc = np.bincount(block.astype(np.int32), minlength=256)
                    out[oy, ox] = np.uint16(int(bc.argmax()))
            return out

        if f == 1:
            scl_expected = scl.copy()
        else:
            scl_expected = block_mode(scl, f, cfg.scl_nodata)

        # ----------------------------------------------------
        # Write ORIGINAL
        # ----------------------------------------------------
        profile_f32 = profile_base | {"dtype": "float32", "height": H, "width": W, "nodata": -9999.0}
        profile_u16 = profile_base | {"dtype": "uint16", "height": H, "width": W, "nodata": cfg.scl_nodata}

        with rasterio.open(orig_ts / "B04.tif", "w", **profile_f32) as dst:
            dst.write(b04, 1)
        with rasterio.open(orig_ts / "B08.tif", "w", **profile_f32) as dst:
            dst.write(b08, 1)
        with rasterio.open(orig_ts / "SCL.tif", "w", **profile_u16) as dst:
            dst.write(scl, 1)

        # ----------------------------------------------------
        # Write EXPECTED
        # ----------------------------------------------------
        exp_h, exp_w = b04_expected.shape
        exp_profile_f32 = profile_base | {
            "dtype": "float32",
            "height": exp_h,
            "width": exp_w,
            "transform": exp_transform,
            "nodata": -9999.0,
        }
        exp_profile_u16 = profile_base | {
            "dtype": "uint16",
            "height": scl_expected.shape[0],
            "width": scl_expected.shape[1],
            "transform": exp_transform,
            "nodata": cfg.scl_nodata,
        }

        with rasterio.open(exp_ts / "B04.tif", "w", **exp_profile_f32) as dst:
            dst.write(b04_expected.astype("float32"), 1)
        with rasterio.open(exp_ts / "B08.tif", "w", **exp_profile_f32) as dst:
            dst.write(b08_expected.astype("float32"), 1)
        with rasterio.open(exp_ts / "SCL.tif", "w", **exp_profile_u16) as dst:
            dst.write(scl_expected.astype("uint16"), 1)

        return original_root, expected_root