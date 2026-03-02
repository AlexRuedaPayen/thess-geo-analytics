from __future__ import annotations

import unittest
from pathlib import Path

import numpy as np
import rasterio

from thess_geo_analytics.geo.NdviProcessor import NdviProcessor

from tests.fixtures.generators.MiniNdviSceneGenerator import (
    MiniNdviSceneConfig,
    MiniNdviSceneGenerator,
)


class NdviReconstructionFromSyntheticBandsTest(unittest.TestCase):
    """
    Pipeline-style test:

      1. Start from a ground-truth NDVI raster (uniform distribution).
      2. From that, derive B04, B08, SCL (reflectances & mask).
      3. Feed B04/B08 into NdviProcessor.
      4. Check that the reconstructed NDVI matches the original NDVI
         field (up to tiny numerical noise).

    We do this for:
      - mini  (16 x 16 raster)
      - large (128 x 128 raster)
    """

    def _run_one_case(self, name: str, H: int, W: int) -> None:
        root = Path("tests/fixtures/generated/ndvi_reconstruction") / name

        cfg = MiniNdviSceneConfig(
            out_dir=root,
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

        # ---------------------------
        # Load ground-truth NDVI
        # ---------------------------
        with rasterio.open(ndvi_path) as ds_true:
            ndvi_true = ds_true.read(1).astype("float32")
            ndvi_nodata = ds_true.nodata

        if ndvi_nodata is not None:
            ndvi_true = np.where(ndvi_true == ndvi_nodata, np.nan, ndvi_true)

        # ---------------------------
        # Use NdviProcessor on B04/B08
        # ---------------------------
        proc = NdviProcessor()

        red, nir, _profile = proc.read_bands(b04_path, b08_path)
        ndvi_est = proc.compute_ndvi(red, nir)

        # ---------------------------
        # Compare
        # ---------------------------
        mask = np.isfinite(ndvi_true) & np.isfinite(ndvi_est)
        self.assertTrue(mask.any(), f"No valid pixels for case={name}")

        diff = np.abs(ndvi_est[mask] - ndvi_true[mask])
        max_diff = float(diff.max()) if diff.size > 0 else 0.0

        # Tight comparison; everything is analytic
        self.assertTrue(
            np.allclose(ndvi_est[mask], ndvi_true[mask], rtol=1e-5, atol=1e-5),
            msg=f"NDVI reconstruction mismatch for case={name}, max_diff={max_diff}",
        )

    def test_mini_scene(self):
        """16x16 NDVI scene reconstruction."""
        self._run_one_case("mini", H=16, W=16)

    def test_large_scene(self):
        """128x128 NDVI scene reconstruction."""
        self._run_one_case("large", H=128, W=128)


if __name__ == "__main__":
    unittest.main()