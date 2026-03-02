from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import numpy as np
import rasterio
from rasterio.transform import from_origin


@dataclass
class MiniNdviSceneConfig:
    """
    Synthetic NDVI scene configuration.

    Process:
      1) Draw NDVI from a uniform distribution in [ndvi_min, ndvi_max]
      2) Draw S = RED + NIR from [s_min, s_max]
      3) Enforce NDVI exactly by solving:

           NDVI = (NIR - RED) / (NIR + RED)   with S = NIR + RED

         => NIR = 0.5 * S * (1 + NDVI)
            RED = 0.5 * S * (1 - NDVI)

      4) Write:
           <name>_NDVI_TRUE.tif  (ground truth NDVI)
           <name>_B04.tif        (RED)
           <name>_B08.tif        (NIR)
           <name>_SCL.tif        (simple classification mask)

    This gives:
      - spatially varying NDVI (uniform)
      - plausible reflectances (0–1)
      - exact consistency between NDVI and bands
      - an SCL raster with a valid uint16 nodata
    """

    out_dir: Path
    name: str
    H: int
    W: int
    ndvi_min: float = -0.2
    ndvi_max: float = 0.9
    s_min: float = 0.2
    s_max: float = 0.8
    nodata: float = -9999.0
    crs: str = "EPSG:32634"
    pixel_size: float = 10.0


class MiniNdviSceneGenerator:
    def __init__(self, cfg: MiniNdviSceneConfig) -> None:
        self.cfg = cfg

    def generate(self) -> Tuple[Path, Path, Path, Path]:
        """
        Generate one NDVI scene + B04/B08/SCL rasters.

        Returns
        -------
        ndvi_path : Path
            Path to ground-truth NDVI GeoTIFF.
        b04_path : Path
            Path to B04 (RED) GeoTIFF.
        b08_path : Path
            Path to B08 (NIR) GeoTIFF.
        scl_path : Path
            Path to SCL (classification) GeoTIFF.
        """
        cfg = self.cfg
        out_dir = cfg.out_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        H, W = cfg.H, cfg.W
        rng = np.random.default_rng(1234 + hash(cfg.name) % 10000)

        # 1) NDVI: uniform spatial field in [ndvi_min, ndvi_max]
        ndvi = rng.uniform(cfg.ndvi_min, cfg.ndvi_max, size=(H, W)).astype("float32")
        # stay safely inside (-1, 1)
        eps = 1e-3
        ndvi = np.clip(ndvi, -1.0 + eps, 1.0 - eps)

        # 2) Brightness S = RED + NIR
        S = rng.uniform(cfg.s_min, cfg.s_max, size=(H, W)).astype("float32")

        # 3) Solve for NIR / RED exactly
        nir = 0.5 * S * (1.0 + ndvi)   # B08
        red = 0.5 * S * (1.0 - ndvi)   # B04

        nir = np.clip(nir, 0.0, 1.0)
        red = np.clip(red, 0.0, 1.0)

        # 4) Simple SCL: mix of classes for diversity
        # codes (example): 1=saturated/invalid, 4=vegetation, 5=bare, 6=water
        scl = rng.choice(
            [1, 4, 5, 6],
            size=(H, W),
            p=[0.05, 0.6, 0.25, 0.1],
        ).astype("uint16")

        transform = from_origin(0.0, 0.0, cfg.pixel_size, cfg.pixel_size)

        profile_f32 = {
            "driver": "GTiff",
            "height": H,
            "width": W,
            "count": 1,
            "dtype": "float32",
            "crs": cfg.crs,
            "transform": transform,
            "nodata": cfg.nodata,
            "compress": "deflate",
        }

        # Ground-truth NDVI
        ndvi_path = out_dir / f"{cfg.name}_NDVI_TRUE.tif"
        with rasterio.open(ndvi_path, "w", **profile_f32) as dst:
            dst.write(ndvi, 1)

        # B04
        b04_path = out_dir / f"{cfg.name}_B04.tif"
        with rasterio.open(b04_path, "w", **profile_f32) as dst:
            dst.write(red, 1)

        # B08
        b08_path = out_dir / f"{cfg.name}_B08.tif"
        with rasterio.open(b08_path, "w", **profile_f32) as dst:
            dst.write(nir, 1)

        # SCL (uint16, with valid nodata)
        profile_u16 = profile_f32.copy()
        profile_u16["dtype"] = "uint16"
        # use 0 as nodata; our codes are {1,4,5,6}, so 0 is safe
        profile_u16["nodata"] = 0

        scl_path = out_dir / f"{cfg.name}_SCL.tif"
        with rasterio.open(scl_path, "w", **profile_u16) as dst:
            dst.write(scl, 1)

        print(
            f"[GEN NDVI] {cfg.name}: H={H}, W={W}, "
            f"NDVI[min,max]=({ndvi.min():.3f}, {ndvi.max():.3f}), "
            f"S[min,max]=({S.min():.3f}, {S.max():.3f})"
        )

        return ndvi_path, b04_path, b08_path, scl_path


# --------------------------------------------------------------
# CLI entrypoint (optional; handy for local debugging)
# --------------------------------------------------------------
if __name__ == "__main__":
    root = Path("tests/fixtures/generated/ndvi_reconstruction")

    cases = [
        ("mini", 16, 16),
        ("large", 128, 128),
    ]

    for name, H, W in cases:
        cfg = MiniNdviSceneConfig(
            out_dir=root / name,
            name=name,
            H=H,
            W=W,
            ndvi_min=-0.2,
            ndvi_max=0.9,
            s_min=0.2,
            s_max=0.8,
        )
        gen = MiniNdviSceneGenerator(cfg)
        ndvi_path, b04_path, b08_path, scl_path = gen.generate()
        print(
            f"[GEN NDVI] case={name}, ndvi={ndvi_path.name}, "
            f"B04={b04_path.name}, B08={b08_path.name}, SCL={scl_path.name}"
        )

    print("✓ Synthetic NDVI scenes generated.")