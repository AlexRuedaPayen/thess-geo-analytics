from __future__ import annotations

import shutil
import unittest
from pathlib import Path

import numpy as np
import rasterio

from thess_geo_analytics.builders.TimestampsAggregationBuilder import (
    TimestampsAggregationBuilder,
    TimestampsAggregationParams,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths

from tests.fixtures.generators.MiniTimestampAggregationSceneGenerator import (
    MiniTimestampAggregationSceneConfig,
    MiniTimestampAggregationSceneGenerator,
)


class RepoPathsOverride:
    """
    Redirect RepoPaths.CACHE_S2, DATA_RAW, TABLES to a test root so that
    TimestampsAggregationBuilder writes under tests/fixtures/generated/...
    """

    def __init__(self, new_root: Path) -> None:
        self.new_root = new_root.resolve()
        self._orig_cache_s2 = RepoPaths.CACHE_S2
        self._orig_data_raw = RepoPaths.DATA_RAW
        self._orig_tables = RepoPaths.TABLES

    def __enter__(self):
        RepoPaths.CACHE_S2 = self.new_root / "cache_s2"
        RepoPaths.DATA_RAW = self.new_root / "data_raw"
        RepoPaths.TABLES = self.new_root / "tables"

        RepoPaths.CACHE_S2.mkdir(parents=True, exist_ok=True)
        RepoPaths.DATA_RAW.mkdir(parents=True, exist_ok=True)
        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)

        return self

    def __exit__(self, exc_type, exc, tb):
        RepoPaths.CACHE_S2 = self._orig_cache_s2
        RepoPaths.DATA_RAW = self._orig_data_raw
        RepoPaths.TABLES = self._orig_tables


class TimestampsAggregationBuilderTest(unittest.TestCase):
    """
    Tests TimestampsAggregationBuilder using synthetic scenes:

      1. Single-tile case → aggregated B04/B08 reconstruct the master scene.
      2. Two-tiles split case → aggregated B04/B08 reconstruct the master scene.
    """

    def setUp(self) -> None:
        self.test_root = Path(
            "tests/fixtures/generated/timestamps_aggregation"
        ).resolve()
        if self.test_root.exists():
            shutil.rmtree(self.test_root)
        self.test_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1.1 Single-tile roundtrip
    # ------------------------------------------------------------------
    def test_single_tile_roundtrip_reconstructs_master(self) -> None:
        """
        MASTER scene (H x W) → one tile → aggregation → reconstructed
        B04/B08 must equal MASTER arrays.
        """
        cfg = MiniTimestampAggregationSceneConfig(
            root=self.test_root,
            ts="2024-01-10T10:00:00Z",
            H=16,
            W=16,
        )
        gen = MiniTimestampAggregationSceneGenerator(cfg)

        with RepoPathsOverride(self.test_root):
            artifacts = gen.generate_single_tile_case(scene_id="SCENE_A")

            ts = artifacts["ts"]
            master_red = artifacts["master_red"]
            master_nir = artifacts["master_nir"]

            # Run aggregation
            params = TimestampsAggregationParams(
                max_workers=1,
                merge_method="first",
                resampling="nearest",
                nodata=float("nan"),
                bands=("B04", "B08", "SCL"),
                debug=True,
            )
            builder = TimestampsAggregationBuilder(params=params)
            folders = builder.run()

            # Expect exactly one aggregated folder
            self.assertEqual(len(folders), 1)
            out_folder = folders[0]

            expected_folder = RepoPaths.DATA_RAW / "aggregated" / ts.replace(":", "_")
            self.assertEqual(out_folder, expected_folder)
            self.assertTrue(out_folder.exists())

            # Compare aggregated B04/B08 to master
            for band, master_arr in [("B04", master_red), ("B08", master_nir)]:
                agg_path = out_folder / f"{band}.tif"
                self.assertTrue(agg_path.exists(), f"Missing aggregated {band}: {agg_path}")

                with rasterio.open(agg_path) as ds:
                    agg_arr = ds.read(1).astype("float32")

                self.assertEqual(agg_arr.shape, master_arr.shape)
                self.assertTrue(
                    np.allclose(agg_arr, master_arr),
                    f"Aggregated {band} does not match master for single-tile case.",
                )

    # ------------------------------------------------------------------
    # 1.2 Two-tiles roundtrip
    # ------------------------------------------------------------------
    def test_two_tiles_roundtrip_reconstructs_master(self) -> None:
        """
        MASTER scene (H x 2W) → left + right tiles → aggregation →
        reconstructed B04/B08 must equal MASTER arrays.
        """
        cfg = MiniTimestampAggregationSceneConfig(
            root=self.test_root,
            ts="2024-02-01T10:00:00Z",
            H=10,
            W=10,
        )
        gen = MiniTimestampAggregationSceneGenerator(cfg)

        with RepoPathsOverride(self.test_root):
            artifacts = gen.generate_two_tiles_split_case(
                scene_left="SCENE_LEFT",
                scene_right="SCENE_RIGHT",
            )

            ts = artifacts["ts"]
            master_red = artifacts["master_red"]
            master_nir = artifacts["master_nir"]
            master_transform = artifacts["master_transform"]

            # Run aggregation
            params = TimestampsAggregationParams(
                max_workers=1,
                merge_method="first",
                resampling="nearest",
                nodata=float("nan"),
                bands=("B04", "B08", "SCL"),
                debug=True,
            )
            builder = TimestampsAggregationBuilder(params=params)
            folders = builder.run()

            self.assertEqual(len(folders), 1)
            out_folder = folders[0]

            expected_folder = RepoPaths.DATA_RAW / "aggregated" / ts.replace(":", "_")
            self.assertEqual(out_folder, expected_folder)
            self.assertTrue(out_folder.exists())

            # Read aggregated B04/B08 and compare to MASTER
            b04_path = out_folder / "B04.tif"
            b08_path = out_folder / "B08.tif"
            self.assertTrue(b04_path.exists(), f"Missing aggregated B04: {b04_path}")
            self.assertTrue(b08_path.exists(), f"Missing aggregated B08: {b08_path}")

            with rasterio.open(b04_path) as ds_r, rasterio.open(b08_path) as ds_n:
                agg_red = ds_r.read(1).astype("float32")
                agg_nir = ds_n.read(1).astype("float32")

                self.assertEqual(agg_red.shape, master_red.shape)
                self.assertEqual(agg_nir.shape, master_nir.shape)

                # In this synthetic layout, the mosaic transform should match master_transform
                self.assertEqual(ds_r.transform, master_transform)
                self.assertEqual(ds_r.crs, ds_n.crs)

            self.assertTrue(
                np.allclose(agg_red, master_red),
                "Aggregated B04 does not reconstruct master scene for two-tile case.",
            )
            self.assertTrue(
                np.allclose(agg_nir, master_nir),
                "Aggregated B08 does not reconstruct master scene for two-tile case.",
            )


if __name__ == "__main__":
    unittest.main()