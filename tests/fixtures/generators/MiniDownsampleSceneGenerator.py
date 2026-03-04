from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import numpy as np
import rasterio
from rasterio.transform import from_origin


@dataclass
class MiniDownsampleSceneConfig:
    out_dir: Path
    name: str
    H: int
    W: int
    factor: int = 2
    pixel_size: float = 10.0
    crs: str = "EPSG:32634"


class MiniDownsampleSceneGenerator:
    """
    Generate synthetic rasters + expected downsampled outputs.

    Creates:

        <name>_B04.tif
        <name>_B08.tif
        <name>_SCL.tif

    and expected:

        <name>_B04_expected.tif
        <name>_B08_expected.tif
        <name>_SCL_expected.tif
    """

    def __init__(self, cfg: MiniDownsampleSceneConfig):
        self.cfg = cfg

    def generate(self) -> Tuple[Path, Path]:

        cfg = self.cfg
        root = cfg.out_dir

        src_root = root / "src"
        expected_root = root / "expected"

        timestamp = "timestamp_001"

        src_ts = src_root / timestamp
        exp_ts = expected_root / timestamp

        src_ts.mkdir(parents=True, exist_ok=True)
        exp_ts.mkdir(parents=True, exist_ok=True)

        H, W = cfg.H, cfg.W
        f = cfg.factor

        transform = from_origin(0, 0, cfg.pixel_size, cfg.pixel_size)

        profile = dict(
            driver="GTiff",
            height=H,
            width=W,
            count=1,
            crs=cfg.crs,
            transform=transform,
            compress="deflate",
        )

        profile_f32 = profile | {"dtype": "float32"}
        profile_u16 = profile | {"dtype": "uint16"}

        # ----------------------------------------------------
        # Continuous bands
        # ----------------------------------------------------

        b04 = np.arange(1, H * W + 1, dtype="float32").reshape(H, W)
        b08 = b04 * 2

        def block_mean(a):
            h2 = (H // f) * f
            w2 = (W // f) * f
            blocks = a[:h2, :w2].reshape(h2 // f, f, w2 // f, f)
            return np.mean(blocks, axis=(1, 3))

        b04_expected = block_mean(b04)
        b08_expected = block_mean(b08)

        # ----------------------------------------------------
        # Categorical SCL
        # ----------------------------------------------------

        scl = np.zeros((H, W), dtype="uint16")

        scl[:H//2, :W//2] = 4
        scl[:H//2, W//2:] = 5
        scl[H//2:, :W//2] = 7
        scl[H//2:, W//2:] = 8

        scl_expected = np.array(
            [
                [4, 5],
                [7, 8],
            ],
            dtype="uint16",
        )

        # ----------------------------------------------------
        # Write input rasters
        # ----------------------------------------------------

        with rasterio.open(src_ts / "B04.tif", "w", **profile_f32) as dst:
            dst.write(b04, 1)

        with rasterio.open(src_ts / "B08.tif", "w", **profile_f32) as dst:
            dst.write(b08, 1)

        with rasterio.open(src_ts / "SCL.tif", "w", **profile_u16) as dst:
            dst.write(scl, 1)

        # ----------------------------------------------------
        # Expected rasters
        # ----------------------------------------------------

        exp_profile_f32 = profile_f32 | {
            "height": b04_expected.shape[0],
            "width": b04_expected.shape[1],
            "transform": from_origin(0, 0, cfg.pixel_size * f, cfg.pixel_size * f),
        }

        exp_profile_u16 = profile_u16 | {
            "height": scl_expected.shape[0],
            "width": scl_expected.shape[1],
            "transform": from_origin(0, 0, cfg.pixel_size * f, cfg.pixel_size * f),
        }

        with rasterio.open(exp_ts / "B04.tif", "w", **exp_profile_f32) as dst:
            dst.write(b04_expected.astype("float32"), 1)

        with rasterio.open(exp_ts / "B08.tif", "w", **exp_profile_f32) as dst:
            dst.write(b08_expected.astype("float32"), 1)

        with rasterio.open(exp_ts / "SCL.tif", "w", **exp_profile_u16) as dst:
            dst.write(scl_expected, 1)

        return src_root, expected_root