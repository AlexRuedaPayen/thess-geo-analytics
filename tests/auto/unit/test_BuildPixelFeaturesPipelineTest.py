# tests/auto/unit/test_BuildPixelFeaturesPipelineTest.py

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Dict, Any

import numpy as np
import rasterio

from thess_geo_analytics.geo.NdviFeatureExtractor import NdviFeatureExtractor
from thess_geo_analytics.pipelines.BuildPixelFeaturesPipeline import (
    BuildPixelFeaturesPipeline,
    BuildPixelFeaturesParams,
    parse_cog_timestamp,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths

from tests.fixtures.generators.MiniAnomalyRasterGenerator import (
    MiniAnomalyConfig,
    MiniAnomalyRasterGenerator,
)


class BuildPixelFeaturesPipelineTest(unittest.TestCase):
    """
    Validate BuildPixelFeaturesPipeline on synthetic anomaly stacks for
    three raster size regimes:

      - mini   (3x3)     → no tiling path
      - little (32x32)   → small tiled raster
      - medium (128x128) → more realistic tiled raster

    For each case, we:
      1. Generate anomaly COGs with MiniAnomalyRasterGenerator
      2. Run BuildPixelFeaturesPipeline to get pixel_features_7d_*.tif
      3. Recompute features in memory via NdviFeatureExtractor
      4. Assert that both agree (within a small tolerance)
    """

    @classmethod
    def setUpClass(cls) -> None:
        root = Path("tests/fixtures/generated/pixel_features_sizes")
        cases = [
            ("mini", 3, 3),
            ("little", 32, 32),
            ("medium", 128, 128),
        ]

        cls.cases: Dict[str, Dict[str, Any]] = {}

        for name, H, W in cases:
            out_dir = root / name
            cfg = MiniAnomalyConfig(
                out_dir=out_dir,
                aoi_id=name,
                H=H,
                W=W,
                T=6,
            )
            gen = MiniAnomalyRasterGenerator(cfg)
            artifacts = gen.run()
            cls.cases[name] = artifacts

    def _run_one_case(self, name: str) -> None:
        artifacts = self.cases[name]
        paths = artifacts["paths"]
        cfg = artifacts["cfg"]
        out_dir: Path = cfg.out_dir

        # ------------------------------------------------------------------
        # 1) Run pipeline on this stack
        # ------------------------------------------------------------------
        out_path = out_dir / f"pixel_features_7d_{name}.tif"
        diag_csv = out_dir / f"pixel_features_diag_{name}.csv"

        params = BuildPixelFeaturesParams(
            ndvi_dir=out_dir,
            pattern="ndvi_anomaly_*.tif",
            aoi_id=name,
            out_path=out_path,
            diagnostics_csv=diag_csv,
            tile_height=8,
            tile_width=8,
        )

        # Force serial tiles for easier debugging/CI determinism
        setattr(params, "tile_workers", 1)

        # RepoPaths.TABLES is used only for diagnostics CSV in this pipeline,
        # so we temporarily redirect it under this test case directory.
        orig_tables = RepoPaths.TABLES
        RepoPaths.TABLES = out_dir
        try:
            pipe = BuildPixelFeaturesPipeline()
            result_path = pipe.run(params)
        finally:
            RepoPaths.TABLES = orig_tables

        self.assertTrue(result_path.exists(), f"Output GeoTIFF does not exist for case={name}")

        # ------------------------------------------------------------------
        # 2) Compute features directly with NdviFeatureExtractor
        # ------------------------------------------------------------------
        paths_sorted = sorted(paths, key=lambda p: parse_cog_timestamp(p))
        timestamps = [parse_cog_timestamp(p) for p in paths_sorted]
        timestamps = np.array(timestamps, dtype="datetime64[D]")

        srcs = [rasterio.open(p) for p in paths_sorted]
        try:
            # read into stack (T, H, W)
            arrs = [src.read(1).astype("float32") for src in srcs]
        finally:
            for s in srcs:
                s.close()

        stack = np.stack(arrs, axis=0)
        extractor = NdviFeatureExtractor()
        feats_true = extractor.compute_features(stack, timestamps)  # (H, W, 7)

        # ------------------------------------------------------------------
        # 3) Read pipeline output and compare
        # ------------------------------------------------------------------
        with rasterio.open(result_path) as dst:
            out = dst.read().astype("float32")  # (7, H, W)
            out_nodata = dst.nodata

        # (H, W, 7)
        feats_pred = np.moveaxis(out, 0, -1)

        self.assertEqual(feats_pred.shape, feats_true.shape)

        # Mask out nodata & NaNs in either
        mask_true = np.isfinite(feats_true)
        mask_pred = np.isfinite(feats_pred)
        mask = mask_true & mask_pred

        # If pipeline uses a finite nodata (e.g. -9999), also exclude those
        if out_nodata is not None:
            mask &= feats_pred != out_nodata

        # Sanity: we should have some valid pixels
        self.assertTrue(mask.any(), f"No valid pixels for comparison in case={name}")

        diff = np.abs(feats_pred[mask] - feats_true[mask])
        max_diff = float(diff.max()) if diff.size > 0 else 0.0

        # Loose but meaningful tolerance; both sides share the same algorithm
        self.assertTrue(
            np.allclose(feats_pred[mask], feats_true[mask], rtol=1e-5, atol=1e-5),
            msg=f"Features mismatch for case={name}, max_diff={max_diff}",
        )

    # ------------------------------------------------------------------
    # Individual tests for each size regime
    # ------------------------------------------------------------------
    """def test_mini_raster(self):
        #3x3 raster → exercises no-tiling path.
        #self._run_one_case("mini")
        pass"""

    def test_little_raster(self):
        """32x32 raster → small tiled raster."""
        self._run_one_case("little")

    def test_medium_raster(self):
        """128x128 raster → more realistic tiled raster."""
        self._run_one_case("medium")