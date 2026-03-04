from __future__ import annotations

import unittest
import shutil
import logging
from pathlib import Path

import numpy as np
import rasterio

from thess_geo_analytics.builders.DownsampleAggregatedTimestampsBuilder import (
    DownsampleAggregatedTimestampsBuilder,
    DownsampleAggregatedTimestampsParams,
)

from tests.fixtures.generators.MiniDownsampleSceneGenerator import (
    MiniDownsampleSceneConfig,
    MiniDownsampleSceneGenerator,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


class DownsampleAggregatedTimestampsBuilderTest(unittest.TestCase):

    def setUp(self):
        self.root = Path("tests/fixtures/generated/downsample_builder")

        logger.info("Preparing test directory: %s", self.root)

        if self.root.exists():
            shutil.rmtree(self.root)

    def _load(self, path):
        with rasterio.open(path) as ds:
            arr = ds.read(1)
            logger.info(
                "Loaded raster %s | shape=(%d,%d) | CRS=%s",
                path.name,
                ds.height,
                ds.width,
                ds.crs,
            )
            return arr, ds.transform, ds.crs, ds.width, ds.height

    # ---------------------------------------------------------
    # Test 1 — Correct downsampling values
    # ---------------------------------------------------------
    def test_builder_downsamples_correctly(self):

        logger.info("TEST: downsampling correctness")

        cfg = MiniDownsampleSceneConfig(
            out_dir=self.root,
            name="case_downsample",
            H=6,
            W=6,
            factor=3,
        )

        gen = MiniDownsampleSceneGenerator(cfg)

        logger.info("Generating synthetic raster scene")
        src_root, expected_root = gen.generate()

        dst_root = self.root / "produced"

        params = DownsampleAggregatedTimestampsParams(
            src_root=src_root,
            dst_root=dst_root,
            factor=3,
            continuous_method="nanmean",
            categorical_method="mode",
        )

        logger.info("Running builder with factor=%d", params.factor)

        builder = DownsampleAggregatedTimestampsBuilder(params)
        builder.run()

        produced = dst_root / "timestamp_001"
        expected = expected_root / "timestamp_001"

        for band in ["B04", "B08", "SCL"]:

            logger.info("Comparing band: %s", band)

            arr_p, tr_p, crs_p, w_p, h_p = self._load(produced / f"{band}.tif")
            arr_e, tr_e, crs_e, w_e, h_e = self._load(expected / f"{band}.tif")

            logger.info("Produced shape=(%d,%d) | Expected shape=(%d,%d)", h_p, w_p, h_e, w_e)

            # values
            if band == "SCL":
                self.assertTrue(np.array_equal(arr_p, arr_e))
            else:
                self.assertTrue(np.allclose(arr_p, arr_e))

            # geometry
            self.assertEqual((w_p, h_p), (w_e, h_e))
            self.assertEqual(tr_p, tr_e)
            self.assertEqual(crs_p, crs_e)

    # ---------------------------------------------------------
    # Test 2 — Downsampling reduces raster size
    # ---------------------------------------------------------
    def test_downsampling_changes_resolution(self):

        logger.info("TEST: resolution reduction")

        cfg = MiniDownsampleSceneConfig(
            out_dir=self.root,
            name="case_resolution",
            H=12,
            W=12,
            factor=3,
        )

        gen = MiniDownsampleSceneGenerator(cfg)

        logger.info("Generating synthetic raster scene")
        src_root, _ = gen.generate()

        dst_root = self.root / "produced"

        params = DownsampleAggregatedTimestampsParams(
            src_root=src_root,
            dst_root=dst_root,
            factor=3,
        )

        builder = DownsampleAggregatedTimestampsBuilder(params)

        logger.info("Running builder with factor=%d", params.factor)
        builder.run()

        src_file = src_root / "timestamp_001" / "B04.tif"
        dst_file = dst_root / "timestamp_001" / "B04.tif"

        _, _, _, w_src, h_src = self._load(src_file)
        _, _, _, w_dst, h_dst = self._load(dst_file)

        logger.info(
            "Resolution change: src=(%d,%d) -> dst=(%d,%d)",
            h_src,
            w_src,
            h_dst,
            w_dst,
        )

        self.assertEqual(w_dst, w_src // 3)
        self.assertEqual(h_dst, h_src // 3)

    # ---------------------------------------------------------
    # Test 3 — factor=1 should NOT downsample
    # ---------------------------------------------------------
    def test_factor_one_does_not_downsample(self):

        logger.info("TEST: factor=1 no downsampling")

        cfg = MiniDownsampleSceneConfig(
            out_dir=self.root,
            name="case_no_downsample",
            H=8,
            W=8,
            factor=2,
        )

        gen = MiniDownsampleSceneGenerator(cfg)

        logger.info("Generating synthetic raster scene")
        src_root, _ = gen.generate()

        dst_root = self.root / "produced"

        params = DownsampleAggregatedTimestampsParams(
            src_root=src_root,
            dst_root=dst_root,
            factor=1,
        )

        builder = DownsampleAggregatedTimestampsBuilder(params)

        logger.info("Running builder with factor=1 (no resampling expected)")
        builder.run()

        src_file = src_root / "timestamp_001" / "B04.tif"
        dst_file = dst_root / "timestamp_001" / "B04.tif"

        arr_src, tr_src, crs_src, w_src, h_src = self._load(src_file)
        arr_dst, tr_dst, crs_dst, w_dst, h_dst = self._load(dst_file)

        logger.info("Comparing rasters for equality")

        # same raster
        self.assertTrue(np.allclose(arr_src, arr_dst))

        # same geometry
        self.assertEqual((w_src, h_src), (w_dst, h_dst))
        self.assertEqual(tr_src, tr_dst)
        self.assertEqual(crs_src, crs_dst)


if __name__ == "__main__":
    unittest.main()