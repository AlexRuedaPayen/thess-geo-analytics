# tests/auto/unit/test_BuildSceneCatalogPipelineTest.py

from __future__ import annotations

import json
import unittest
from datetime import date
from pathlib import Path
from typing import List, Dict, Any
from contextlib import contextmanager

import pandas as pd

from thess_geo_analytics.builders.SceneCatalogBuilder import SceneCatalogBuilder
from thess_geo_analytics.pipelines.BuildSceneCatalogPipeline import (
    BuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths

from tests.fixtures.generators.SceneCatalogTestDataGenerator import (
    SceneCatalogTestDataConfig,
    SceneCatalogTestDataGenerator,
)
from tests.mocks.FakeCdseSceneCatalogService import FakeCdseSceneCatalogService


# ---------------------------------------------------------------------
# Helper: temporarily patch RepoPaths.TABLES for each test
# ---------------------------------------------------------------------
@contextmanager
def patch_tables_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)
    orig = RepoPaths.TABLES
    RepoPaths.TABLES = path
    try:
        yield
    finally:
        RepoPaths.TABLES = orig


# =====================================================================
# Main test class — Step 2: BuildSceneCatalogPipeline
# =====================================================================

class BuildSceneCatalogPipelineTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        """
        Load already-generated AOI + items produced by SceneCatalogTestDataGenerator.
        If missing, generate once.
        """
        gen_dir = Path("tests/fixtures/generated/scene_catalog")
        gen_dir.mkdir(parents=True, exist_ok=True)

        aoi_path = gen_dir / "aoi_scene_catalog.geojson"
        items_path = gen_dir / "scene_catalog_items.json"

        if not aoi_path.exists() or not items_path.exists():
            cfg = SceneCatalogTestDataConfig(
                output_dir=gen_dir,
                start_datetime="2021-01-05T09:13:51Z",
                n_timestamps=40,
                tiles_per_timestamp=20,
                cloud_min=0.0,
                cloud_max=80.0,
                rng_seed=42,
                preview_geojson=True,
                preview_csv=True,
            )
            gen = SceneCatalogTestDataGenerator(cfg)
            artifacts = gen.run()
            aoi_path = artifacts["aoi_path"]
            items_path = artifacts["items_path"]

        cls.aoi_path = aoi_path

        # Load synthetic STAC-like rows produced by the generator
        with items_path.open("r", encoding="utf-8") as f:
            cls.raw_items: List[Dict[str, Any]] = json.load(f)

    # =================================================================
    # Test 1: realistic STAC filtering + tile selector
    # =================================================================

    def testRunBuildSceneCatalogWithCloudFilterAndSelector(self):
        """
        - realistic raw synthetic catalog (clouds 0–80%)
        - filter cloud_cover_max=20 → STAC-like behaviour
        - tile selector enabled
        - max_items limits the number of scenes returned
        """

        fake_service = FakeCdseSceneCatalogService(self.raw_items)
        builder = SceneCatalogBuilder(service=fake_service)
        pipeline = BuildSceneCatalogPipeline(aoi_path=self.aoi_path, builder=builder)

        # Config similar to real pipeline
        params = BuildSceneCatalogParams(
            date_start="2021-01-01",
            cloud_cover_max=20.0,
            max_items=200,
            collection="sentinel-2-l2a",
            use_tile_selector=True,
            full_cover_threshold=0.5,
            allow_union=True,
            max_union_tiles=20,
            n_anchors=10,
            window_days=21,
        )

        # sanity: ensure raw generator has high-cloud scenes dropped by filter
        num_high_cloud = sum(
            1 for it in self.raw_items
            if float(it["properties"]["cloud_cover"]) > 20.0
        )
        self.assertGreater(num_high_cloud, 0)

        # Patch date.today() inside the pipeline
        import thess_geo_analytics.pipelines.BuildSceneCatalogPipeline as bsc_mod

        class _FakeDate(date):
            @classmethod
            def today(cls):
                return cls(2021, 12, 31)

        orig_date = bsc_mod.date
        bsc_mod.date = _FakeDate

        # Separate output dir for this test
        out_dir = Path("tests/artifacts/pipeline_tables/normal_run")

        try:
            with patch_tables_dir(out_dir):
                out_path = pipeline.run(params)
        finally:
            bsc_mod.date = orig_date

        # ------------------------------------------------------------------
        # Assertions
        # ------------------------------------------------------------------

        raw = out_dir / "scenes_catalog.csv"
        sel = out_dir / "scenes_selected.csv"
        ts = out_dir / "time_serie.csv"
        cov = out_dir / "timestamps_coverage.csv"

        self.assertTrue(raw.exists())
        self.assertTrue(sel.exists())
        self.assertTrue(ts.exists())
        self.assertTrue(cov.exists())

        # Selector enabled → pipeline returns the time series path
        self.assertEqual(out_path, ts)

        raw_df = pd.read_csv(raw)
        self.assertGreater(len(raw_df), 0)
        self.assertLessEqual(raw_df["cloud_cover"].max(), 20.0 + 1e-6)

        cov_df = pd.read_csv(cov)
        self.assertFalse(cov_df.empty)
        self.assertIn("coverage_frac", cov_df.columns)
        self.assertTrue(cov_df["has_full_cover"].any())

        ts_df = pd.read_csv(ts)
        self.assertFalse(ts_df.empty)
        self.assertLessEqual(len(ts_df), params.n_anchors)
        self.assertIn("cloud_score", ts_df.columns)
        self.assertLessEqual(ts_df["cloud_score"].max(), 20.0 + 1e-6)

    # =================================================================
    # Test 2: empty STAC branch
    # =================================================================

    def testRunWithNoItemsProducesEmptyCsvs(self):

        empty_service = FakeCdseSceneCatalogService(items=[])
        builder = SceneCatalogBuilder(service=empty_service)
        pipeline = BuildSceneCatalogPipeline(aoi_path=self.aoi_path, builder=builder)

        params = BuildSceneCatalogParams(
            date_start="2021-01-01",
            use_tile_selector=True,
        )

        out_dir = Path("tests/artifacts/pipeline_tables/empty_case")

        with patch_tables_dir(out_dir):
            out_path = pipeline.run(params)

        raw = out_dir / "scenes_catalog.csv"
        sel = out_dir / "scenes_selected.csv"
        ts = out_dir / "time_serie.csv"
        cov = out_dir / "timestamps_coverage.csv"

        self.assertTrue(raw.exists())
        self.assertTrue(sel.exists())
        self.assertTrue(ts.exists())
        self.assertTrue(cov.exists())

        self.assertTrue(pd.read_csv(raw).empty)
        self.assertTrue(pd.read_csv(sel).empty)
        self.assertTrue(pd.read_csv(ts).empty)
        self.assertTrue(pd.read_csv(cov).empty)

        # Empty STAC → run returns raw_csv
        self.assertEqual(out_path, raw)