from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List

import numpy as np
import rasterio
from rasterio.transform import from_origin


@dataclass
class MiniAnomalyConfig:
    """
    Configuration for a synthetic NDVI anomaly stack.

    - out_dir: directory where the anomaly COGs will be written
    - aoi_id: suffix used in filenames (ndvi_anomaly_YYYY-MM_<aoi_id>.tif)
    - H, W: raster height/width
    - T: number of time steps
    """
    out_dir: Path
    aoi_id: str
    H: int
    W: int
    T: int = 6
    base_date: str = "2020-01-15"   # ISO date
    step_days: int = 30             # spacing between timestamps (days)
    nodata: float = -9999.0
    pixel_size: float = 10.0        # arbitrary
    crs: str = "EPSG:4326"


class MiniAnomalyRasterGenerator:
    """
    Generates small stacks of NDVI anomaly COGs for testing the
    BuildPixelFeaturesPipeline on different raster sizes.

    We deliberately keep the anomaly signal simple and deterministic:
      NDVI(t) = 0.3 + 0.05 * t - 0.02 * sin(2π t / T)

    That gives:
      - a mild trend
      - some seasonality
      - no nodata pixels
    """

    def __init__(self, cfg: MiniAnomalyConfig) -> None:
        self.cfg = cfg

    # --------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        """
        Generate T anomaly COGs:

          ndvi_anomaly_YYYY-MM_<aoi_id>.tif

        Returns a dict with:
          - "paths":   list[Path] to COGs
          - "timestamps": np.ndarray[datetime64]
          - "cfg":     the MiniAnomalyConfig
        """
        cfg = self.cfg
        cfg.out_dir.mkdir(parents=True, exist_ok=True)

        timestamps = self._generate_timestamps(cfg.T, cfg.base_date, cfg.step_days)
        stack = self._generate_stack(cfg.T, cfg.H, cfg.W)

        paths: List[Path] = []
        for i in range(cfg.T):
            ts = timestamps[i]
            ym = str(ts)[:7]  # "YYYY-MM"
            name = f"ndvi_anomaly_{ym}_{cfg.aoi_id}.tif"
            path = cfg.out_dir / name
            self._write_single_cog(path, stack[i], cfg)
            paths.append(path)

        return {
            "paths": paths,
            "timestamps": timestamps,
            "cfg": cfg,
        }

    # --------------------------------------------------------------
    # Internals
    # --------------------------------------------------------------
    @staticmethod
    def _generate_timestamps(T: int, base_date: str, step_days: int) -> np.ndarray:
        """
        Returns an array of length T of np.datetime64[D], spaced by step_days.
        """
        base = np.datetime64(base_date, "D")
        step = np.timedelta64(step_days, "D")
        return np.array([base + i * step for i in range(T)], dtype="datetime64[D]")

    @staticmethod
    def _generate_stack(T: int, H: int, W: int) -> np.ndarray:
        """
        Generate a (T, H, W) NDVI anomaly stack with a simple, smooth pattern.
        """
        t = np.arange(T, dtype=np.float32)
        series = 0.3 + 0.05 * t - 0.02 * np.sin(2.0 * np.pi * t / max(T, 1))

        stack = np.empty((T, H, W), dtype=np.float32)
        for i in range(T):
            stack[i, :, :] = series[i]

        return stack

    @staticmethod
    def _write_single_cog(path: Path, arr: np.ndarray, cfg: MiniAnomalyConfig) -> None:
        """
        Write a single-band GeoTIFF with deflate compression.
        We don't care about tiling here; these are just test inputs.
        """
        H, W = arr.shape
        transform = from_origin(0.0, 0.0, cfg.pixel_size, cfg.pixel_size)

        profile = {
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

        with rasterio.open(path, "w", **profile) as dst:
            dst.write(arr.astype("float32"), 1)

        print(f"[GEN] wrote anomaly COG → {path}")


# --------------------------------------------------------------
# CLI entrypoint (optional; handy for local debugging)
# --------------------------------------------------------------
if __name__ == "__main__":
    root = Path("tests/fixtures/generated/pixel_features_sizes")

    cases = [
        ("mini", 3, 3),
        ("little", 32, 32),
        ("medium", 128, 128),
    ]

    for name, H, W in cases:
        cfg = MiniAnomalyConfig(
            out_dir=root / name,
            aoi_id=name,
            H=H,
            W=W,
            T=6,
        )
        gen = MiniAnomalyRasterGenerator(cfg)
        out = gen.run()
        print(
            f"[GEN] case={name}, H={H}, W={W}, "
            f"n_cogs={len(out['paths'])}, "
            f"first_ts={out['timestamps'][0]}, "
            f"last_ts={out['timestamps'][-1]}"
        )

    print("✓ Mini anomaly rasters generated.")