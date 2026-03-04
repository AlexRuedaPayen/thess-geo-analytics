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

    @classmethod
    def setUpClass(cls):

        cls.session_root = Path(
            "tests/fixtures/generated/downsample_builder/session"
        )

        cls.session_root.mkdir(parents=True, exist_ok=True)

        logger.info("Session root: %s", cls.session_root)

    # ---------------------------------------------------------
    # helper
    # ---------------------------------------------------------

    def _case_dir(self, name: str) -> Path:

        case_dir = self.session_root / name

        if case_dir.exists():
            shutil.rmtree(case_dir)

        case_dir.mkdir(parents=True)

        return case_dir

    def _load(self, path):

        with rasterio.open(path) as ds:
            arr = ds.read(1)
            return arr, ds.transform, ds.crs, ds.width, ds.height

    # ---------------------------------------------------------
    # Test 1
    # ---------------------------------------------------------

    def test_builder_downsamples_correctly(self):

        logger.info("TEST: correctness")

        root = self._case_dir("test_downsample_correctness")

        cfg = MiniDownsampleSceneConfig(
            out_dir=root,
            name="case_downsample",
            H=6,
            W=6,
            factor=3,
        )

        gen = MiniDownsampleSceneGenerator(cfg)

        src_root, expected_root = gen.generate()

        produced_root = root / "produced"

        params = DownsampleAggregatedTimestampsParams(
            src_root=src_root,
            dst_root=produced_root,
            factor=3,
        )

        builder = DownsampleAggregatedTimestampsBuilder(params)
        builder.run()

        produced = produced_root / "timestamp_001"
        expected = expected_root / "timestamp_001"

        for band in ["B04", "B08", "SCL"]:

            arr_p, tr_p, crs_p, w_p, h_p = self._load(produced / f"{band}.tif")
            arr_e, tr_e, crs_e, w_e, h_e = self._load(expected / f"{band}.tif")

            if band == "SCL":
                self.assertTrue(np.array_equal(arr_p, arr_e))
            else:
                self.assertTrue(np.allclose(arr_p, arr_e))

            self.assertEqual((w_p, h_p), (w_e, h_e))
            self.assertEqual(tr_p, tr_e)
            self.assertEqual(crs_p, crs_e)

    # ---------------------------------------------------------
    # Test 2
    # ---------------------------------------------------------

    def test_downsampling_changes_resolution(self):

        logger.info("TEST: resolution change")

        root = self._case_dir("test_resolution_change")

        cfg = MiniDownsampleSceneConfig(
            out_dir=root,
            name="case_resolution",
            H=12,
            W=12,
            factor=3,
        )

        gen = MiniDownsampleSceneGenerator(cfg)

        src_root, _ = gen.generate()

        produced_root = root / "produced"

        params = DownsampleAggregatedTimestampsParams(
            src_root=src_root,
            dst_root=produced_root,
            factor=3,
        )

        builder = DownsampleAggregatedTimestampsBuilder(params)
        builder.run()

        src_file = src_root / "timestamp_001" / "B04.tif"
        dst_file = produced_root / "timestamp_001" / "B04.tif"

        _, _, _, w_src, h_src = self._load(src_file)
        _, _, _, w_dst, h_dst = self._load(dst_file)

        self.assertEqual(w_dst, w_src // 3)
        self.assertEqual(h_dst, h_src // 3)

    # ---------------------------------------------------------
    # Test 3
    # ---------------------------------------------------------

    def test_factor_one_does_not_downsample(self):

        logger.info("TEST: factor=1")

        root = self._case_dir("test_factor_one")

        cfg = MiniDownsampleSceneConfig(
            out_dir=root,
            name="case_no_downsample",
            H=8,
            W=8,
            factor=2,
        )

        gen = MiniDownsampleSceneGenerator(cfg)

        src_root, _ = gen.generate()

        produced_root = root / "produced"

        params = DownsampleAggregatedTimestampsParams(
            src_root=src_root,
            dst_root=produced_root,
            factor=1,
        )

        builder = DownsampleAggregatedTimestampsBuilder(params)
        builder.run()

        src_file = src_root / "timestamp_001" / "B04.tif"
        dst_file = produced_root / "timestamp_001" / "B04.tif"

        arr_src, tr_src, crs_src, w_src, h_src = self._load(src_file)
        arr_dst, tr_dst, crs_dst, w_dst, h_dst = self._load(dst_file)

        self.assertTrue(np.allclose(arr_src, arr_dst))
        self.assertEqual((w_src, h_src), (w_dst, h_dst))
        self.assertEqual(tr_src, tr_dst)
        self.assertEqual(crs_src, crs_dst)

    # ---------------------------------------------------------
    # Test 4 (visual debugging case)
    # ---------------------------------------------------------

    def test_visual_case(self):

        logger.info("TEST: visual case")

        root = self._case_dir("test_visual_case")

        cfg = MiniDownsampleSceneConfig(
            out_dir=root,
            name="case_visual",
            H=24,
            W=24,
            factor=4,
        )

        gen = MiniDownsampleSceneGenerator(cfg)

        src_root, _ = gen.generate()

        produced_root = root / "produced"

        params = DownsampleAggregatedTimestampsParams(
            src_root=src_root,
            dst_root=produced_root,
            factor=4,
        )

        builder = DownsampleAggregatedTimestampsBuilder(params)
        builder.run()


if __name__ == "__main__":
    unittest.main()