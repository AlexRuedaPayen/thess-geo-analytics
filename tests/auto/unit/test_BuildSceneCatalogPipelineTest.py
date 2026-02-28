from __future__ import annotations

import json
import unittest
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

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


class BuildSceneCatalogPipelineTest(unittest.TestCase):
    """
    CI-friendly tests for pipeline step 2: BuildSceneCatalogPipeline.

    Uses:
      - synthetic AOI + STAC-like items from SceneCatalogTestDataGenerator
      - FakeCdseSceneCatalogService as a test double for the real STAC service
      - RepoPaths.TABLES patched to write into tests/artifacts/pipeline_tables
      - date.today() patched inside the pipeline module so the period aligns
        with the synthetic 2021 data.
    """

    # ------------------------------------------------------------------ #
    # Class-wide setup / teardown
    # ------------------------------------------------------------------ #

    @classmethod
    def setUpClass(cls) -> None:
        """
        Ensure AOI + fake items exist for all tests in this class.

        Artifacts (kept on disk for manual inspection):

          tests/fixtures/generated/scene_catalog/aoi_scene_catalog.geojson
          tests/fixtures/generated/scene_catalog/scene_catalog_items.json
        """
        gen_dir = Path("tests/fixtures/generated/scene_catalog")
        gen_dir.mkdir(parents=True, exist_ok=True)

        aoi_path = gen_dir / "aoi_scene_catalog.geojson"
        items_path = gen_dir / "scene_catalog_items.json"

        # Generate once if missing (idempotent)
        if not aoi_path.exists() or not items_path.exists():
            cfg = SceneCatalogTestDataConfig(
                output_dir=gen_dir,
                start_datetime="2021-01-05T09:13:51Z",
                n_timestamps=24,        # more timestamps → bigger catalog
                tiles_per_timestamp=15, # maximal tile density per timestamp
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

        with items_path.open("r", encoding="utf-8") as f:
            cls.raw_items: List[Dict[str, Any]] = json.load(f)

        # Where the pipeline will write its CSVs (kept for manual inspection)
        cls.tables_dir = Path("tests/artifacts/pipeline_tables")
        cls.tables_dir.mkdir(parents=True, exist_ok=True)

        # Patch RepoPaths.TABLES so that step 2 writes under tests/artifacts
        cls._orig_tables = RepoPaths.TABLES
        RepoPaths.TABLES = cls.tables_dir

    @classmethod
    def tearDownClass(cls) -> None:
        # Restore original RepoPaths.TABLES
        RepoPaths.TABLES = cls._orig_tables

    # ------------------------------------------------------------------ #
    # Per-test setup
    # ------------------------------------------------------------------ #

    def setUp(self) -> None:
        """
        For each test, build a SceneCatalogBuilder wired to the fake STAC service.
        """
        fake_service = FakeCdseSceneCatalogService(self.raw_items)
        self.builder = SceneCatalogBuilder(service=fake_service)

    # ------------------------------------------------------------------ #
    # Main step-2 test: normal STAC + selector usage
    # ------------------------------------------------------------------ #

    def testRunBuildSceneCatalogWithCloudFilterAndTileSelector(self):
        """
        Step 2 under "normal" conditions:

        - Synthetic AOI + items (2021-01 .. 2021-07)
        - STAC-like filtering: cloud_cover_max=20
        - TileSelector enabled (unions up to 15 tiles)
        - date.today() patched to 2021-07-31 so anchors fall in 2021
        """
        pipeline = BuildSceneCatalogPipeline(aoi_path=self.aoi_path, builder=self.builder)

        params = BuildSceneCatalogParams(
            date_start="2021-01-01",
            cloud_cover_max=20.0,          # STAC-like cloud filter
            max_items=1000,
            collection="sentinel-2-l2a",
            use_tile_selector=True,
            full_cover_threshold=0.5,
            allow_union=True,
            max_union_tiles=15,
            n_anchors=6,
            window_days=21,
        )

        # Sanity check: generator must produce some high-cloud scenes that are dropped
        num_high_cloud = sum(
            1
            for it in self.raw_items
            if float(it.get("properties", {}).get("cloud_cover", 0.0)) > 20.0
        )
        self.assertGreater(
            num_high_cloud,
            0,
            msg="SceneCatalogTestDataGenerator should produce some scenes with cloud_cover > 20",
        )

        # Patch "today" inside the pipeline module so that period_end is in 2021.
        import thess_geo_analytics.pipelines.BuildSceneCatalogPipeline as bsc_mod

        class _FakeDate(date):
            @classmethod
            def today(cls) -> date:
                # Our synthetic data span 2021-01..~2021-07
                return cls(2021, 7, 31)

        orig_date_class = bsc_mod.date
        bsc_mod.date = _FakeDate
        try:
            out_path = pipeline.run(params)
        finally:
            # Always restore original date class
            bsc_mod.date = orig_date_class

        raw_csv = self.tables_dir / "scenes_catalog.csv"
        selected_csv = self.tables_dir / "scenes_selected.csv"
        ts_csv = self.tables_dir / "time_serie.csv"
        ts_cov_csv = self.tables_dir / "timestamps_coverage.csv"

        # ------------------------
        # Files exist
        # ------------------------
        self.assertTrue(raw_csv.exists())
        self.assertTrue(selected_csv.exists())
        self.assertTrue(ts_csv.exists())
        self.assertTrue(ts_cov_csv.exists())

        # With selector enabled & non-empty input, run() returns ts_csv
        self.assertEqual(out_path, ts_csv)

        # ------------------------
        # Raw catalog (scenes_catalog.csv)
        # ------------------------
        raw_df = pd.read_csv(raw_csv)
        self.assertGreater(len(raw_df), 0)
        self.assertIn("cloud_cover", raw_df.columns)
        self.assertIn("platform", raw_df.columns)
        self.assertIn("collection", raw_df.columns)

        # Cloud filter must be respected by the catalog
        self.assertLessEqual(raw_df["cloud_cover"].max(), 20.0 + 1e-6)

        # ------------------------
        # Coverage table (timestamps_coverage.csv)
        # ------------------------
        cov_df = pd.read_csv(ts_cov_csv)
        self.assertIn("coverage_frac", cov_df.columns)
        self.assertIn("has_full_cover", cov_df.columns)

        # It is allowed to be empty (geometry + cloud filter can be strict),
        # but if not empty, coverage_frac must be within (0, 1].
        if not cov_df.empty:
            self.assertGreater(cov_df["coverage_frac"].min(), 0.0)
            self.assertLessEqual(cov_df["coverage_frac"].max(), 1.0)

        # ------------------------
        # Time series (time_serie.csv)
        # ------------------------
        ts_df = pd.read_csv(ts_csv)
        self.assertIn("coverage_frac", ts_df.columns)
        self.assertIn("cloud_score", ts_df.columns)
        self.assertIn("tiles_count", ts_df.columns)

        # If we have some anchors selected, coverage/cloud should be reasonable
        if not ts_df.empty:
            self.assertLessEqual(len(ts_df), params.n_anchors)
            self.assertGreater(ts_df["coverage_frac"].min(), 0.0)
            self.assertLessEqual(ts_df["coverage_frac"].max(), 1.0)
            self.assertGreaterEqual(ts_df["cloud_score"].min(), 0.0)
            self.assertLessEqual(ts_df["cloud_score"].max(), 20.0 + 1e-6)

    # ------------------------------------------------------------------ #
    # Empty STAC branch
    # ------------------------------------------------------------------ #

    def testRunWithNoItemsWritesEmptyCsvs(self):
        """
        Step 2 behaviour when STAC search returns no items.

        This exercises the early-return branch in BuildSceneCatalogPipeline.run
        and still leaves CSVs (with headers) on disk for inspection.
        """
        empty_service = FakeCdseSceneCatalogService(items=[])
        builder = SceneCatalogBuilder(service=empty_service)
        pipeline = BuildSceneCatalogPipeline(aoi_path=self.aoi_path, builder=builder)

        params = BuildSceneCatalogParams(
            date_start="2021-01-01",
            use_tile_selector=True,
        )

        out_path = pipeline.run(params)

        raw_csv = self.tables_dir / "scenes_catalog.csv"
        selected_csv = self.tables_dir / "scenes_selected.csv"
        ts_csv = self.tables_dir / "time_serie.csv"
        ts_cov_csv = self.tables_dir / "timestamps_coverage.csv"

        # Raw catalog should exist and be empty
        self.assertTrue(raw_csv.exists())
        raw_df = pd.read_csv(raw_csv)
        self.assertTrue(raw_df.empty)

        # Selected / time_serie / coverage all exist and are empty (but with headers)
        for csv_path in (selected_csv, ts_csv, ts_cov_csv):
            self.assertTrue(csv_path.exists())
            df = pd.read_csv(csv_path)
            self.assertTrue(df.empty)

        # In this branch, run() returns raw_csv
        self.assertEqual(out_path, raw_csv)