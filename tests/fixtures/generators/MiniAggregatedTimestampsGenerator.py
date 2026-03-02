# tests/fixtures/generators/MiniAggregatedTimestampsGenerator.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import numpy as np
import rasterio
from rasterio.transform import from_origin
from affine import Affine  # rasterio's transform type


@dataclass(frozen=True)
class MiniAggregatedTimestampsConfig:
    """
    Minimal config to create a tiny aggregated-timestamps tree:

        <root>/
          aggregated/
            <sanitized_timestamp1>/
              ...B04.tif
              ...B08.tif
            <sanitized_timestamp2>/
              ...

    Each timestamp folder contains synthetic B04 (RED) and B08 (NIR) rasters.

    You can control the RED/NIR values per timestamp via `red_values`
    and `nir_values` (same length as `timestamps`).

    You can also force a specific spatial grid by providing `height`,
    `width`, `transform`, and `crs`. If `transform`/`crs` are not given,
    a default grid around (0,0) in EPSG:32634 is used.

    IMPORTANT:
      B04 and B08 share the SAME multiplicative spatial pattern, so
      NDVI is constant for each timestamp, but B04/B08 are not flat.
    """
    root: Path
    timestamps: List[str]
    red_values: Optional[List[float]] = None
    nir_values: Optional[List[float]] = None
    height: int = 16
    width: int = 16
    transform: Optional[Affine] = None
    crs: Optional[str] = None


class MiniAggregatedTimestampsGenerator:
    """
    Creates a tiny aggregated root with a couple of timestamp folders
    containing synthetic B04/B08 rasters.
    """

    def __init__(self, cfg: MiniAggregatedTimestampsConfig) -> None:
        self.cfg = cfg

        if self.cfg.red_values is not None and len(self.cfg.red_values) != len(
            self.cfg.timestamps
        ):
            raise ValueError("red_values must have same length as timestamps")

        if self.cfg.nir_values is not None and len(self.cfg.nir_values) != len(
            self.cfg.timestamps
        ):
            raise ValueError("nir_values must have same length as timestamps")

    def generate(self) -> Path:
        """
        Returns the aggregated root folder:

            aggregated_root = <root> / "aggregated"
        """
        aggregated_root = self.cfg.root / "aggregated"
        aggregated_root.mkdir(parents=True, exist_ok=True)

        for idx, ts in enumerate(self.cfg.timestamps):
            # ---- Windows-safe folder name ----
            folder_name = ts.replace(":", "_")

            ts_dir = aggregated_root / folder_name
            ts_dir.mkdir(parents=True, exist_ok=True)

            # Per-timestamp RED/NIR values (or defaults)
            if self.cfg.red_values is not None:
                red_val = float(self.cfg.red_values[idx])
            else:
                red_val = 0.2

            if self.cfg.nir_values is not None:
                nir_val = float(self.cfg.nir_values[idx])
            else:
                nir_val = 0.8

            self._write_pair(ts_dir, red_val=red_val, nir_val=nir_val)

        return aggregated_root

    def _make_pattern(self, h: int, w: int, seed: int) -> np.ndarray:
        """
        Deterministic spatial pattern around 1.0, in [0.8, 1.2].

        This makes the synthetic rasters more "textured" but NDVI stays
        constant because both B04 and B08 use the SAME pattern.
        """
        # Use seed for reproducibility per timestamp
        rng = np.random.default_rng(seed)
        # Base sinusoidal pattern on a grid
        y_idx, x_idx = np.indices((h, w))
        nx = (x_idx - w / 2) / max(1, (w / 2))
        ny = (y_idx - h / 2) / max(1, (h / 2))

        base = 0.1 * np.sin(np.pi * nx) * np.cos(np.pi * ny)  # [-0.1, 0.1]
        noise = 0.02 * rng.standard_normal(size=(h, w))       # small randomness

        pattern = base + noise
        # Clamp overall perturbation to [-0.2, 0.2]
        pattern = np.clip(pattern, -0.2, 0.2)
        # Convert to multiplicative factor in [0.8, 1.2]
        multiplier = 1.0 + pattern
        return multiplier.astype("float32")

    def _write_pair(self, ts_dir: Path, *, red_val: float, nir_val: float) -> None:
        h, w = self.cfg.height, self.cfg.width

        # Use provided transform/CRS if any, else a simple default grid
        if self.cfg.transform is not None:
            transform = self.cfg.transform
        else:
            transform = from_origin(0.0, 0.0, 10.0, 10.0)

        crs = self.cfg.crs or "EPSG:32634"

        profile = {
            "driver": "GTiff",
            "height": h,
            "width": w,
            "count": 1,
            "dtype": "float32",
            "crs": crs,
            "transform": transform,
            "nodata": -9999.0,
        }

        # Deterministic pattern based on folder name
        seed = hash(ts_dir.name) & 0xFFFFFFFF
        m = self._make_pattern(h, w, seed=seed)  # multiplicative pattern in [0.8, 1.2]

        red_arr = red_val * m
        nir_arr = nir_val * m

        # Keep reflectances within [0, 1] for sanity
        red_arr = np.clip(red_arr, 0.0, 1.0)
        nir_arr = np.clip(nir_arr, 0.0, 1.0)

        # Write B04
        b04_path = ts_dir / "dummy_B04.tif"
        with rasterio.open(b04_path, "w", **profile) as dst:
            dst.write(red_arr, 1)

        # Write B08
        b08_path = ts_dir / "dummy_B08.tif"
        with rasterio.open(b08_path, "w", **profile) as dst:
            dst.write(nir_arr, 1)


# ----------------------------------------------------------------------
# Simple CLI: allow you to just "run the generator"
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Generate a minimal aggregated-timestamps tree for testing "
            "(B04/B08 synthetic rasters)."
        )
    )
    parser.add_argument(
        "--root",
        type=str,
        default="tests/fixtures/generated/ndvi_aggregated",
        help="Root directory where 'aggregated/' will be created (default: %(default)s).",
    )
    parser.add_argument(
        "--timestamps",
        type=str,
        nargs="*",
        default=[
            "2024-01-10T10:00:00Z",
            "2024-01-20T10:00:00Z",
            "2024-02-05T10:00:00Z",
        ],
        help="List of timestamp labels (ISO-like) to generate.",
    )
    args = parser.parse_args()

    cfg = MiniAggregatedTimestampsConfig(
        root=Path(args.root),
        timestamps=list(args.timestamps),
    )
    gen = MiniAggregatedTimestampsGenerator(cfg)
    aggregated_root = gen.generate()
    print(f"[OK] Aggregated test data written under: {aggregated_root}")