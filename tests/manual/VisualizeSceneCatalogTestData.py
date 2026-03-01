from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from thess_geo_analytics.builders.SceneCatalogBuilder import SceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.pipelines.BuildSceneCatalogPipeline import (
    BuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths

from tests.fixtures.generators.SceneCatalogTestDataGenerator import (
    SceneCatalogTestDataConfig,
    SceneCatalogTestDataGenerator,
)


class FakeCdseSceneCatalogService:
    """
    Test double for CdseSceneCatalogService:

    - starts from an in-memory list of RAW STAC-like items (cloud in [0, 80])
    - applies cloud_cover_max filter like a real STAC search
    - echoes AOI geometry from the provided GeoJSON path
    """

    def __init__(self, items: List[Dict[str, Any]]) -> None:
        self._items = items

    def search_items(
        self,
        aoi_geojson_path: Path,
        date_start: str,
        date_end: str,
        params: StacQueryParams,
    ) -> Tuple[List[Any], Dict[str, Any]]:
        with aoi_geojson_path.open("r", encoding="utf-8") as f:
            aoi_fc = json.load(f)
        aoi_geom = aoi_fc["features"][0]["geometry"]

        max_cloud = getattr(params, "cloud_cover_max", None)

        if max_cloud is None:
            filtered = list(self._items)
        else:
            filtered = []
            for it in self._items:
                props = it.get("properties", {}) or {}
                cc = props.get("cloud_cover")
                try:
                    cc_val = float(cc)
                except (TypeError, ValueError):
                    cc_val = float("inf")
                if cc_val <= max_cloud:
                    filtered.append(it)

        return filtered, aoi_geom

    def items_to_dataframe(self, items: List[Any], *, collection: str) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for idx, it in enumerate(items):
            props = it.get("properties", {})
            platform = "sentinel-2a" if idx % 2 == 0 else "sentinel-2b"

            rows.append(
                {
                    "id": it["id"],
                    "datetime": props["datetime"],
                    "cloud_cover": props.get("cloud_cover"),
                    "platform": platform,
                    "constellation": "sentinel-2",
                    "collection": collection,
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        return df


class BuildSceneCatalogPipelineTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        """
        Ensure AOI + fake items exist (realistic config), then load them.

        Files used:
          tests/fixtures/generated/scene_catalog/aoi_scene_catalog.geojson
          tests/fixtures/generated/scene_catalog/scene_catalog_items.json

        If someone already ran SceneCatalogTestDataGenerator (e.g. in CI),
        we reuse those files and DO NOT regenerate.
        """
        gen_dir = Path("tests/fixtures/generated/scene_catalog")
        gen_dir.mkdir(parents=True, exist_ok=True)

        aoi_path = gen_dir / "aoi_scene_catalog.geojson"
        items_path = gen_dir / "scene_catalog_items.json"

        if not aoi_path.exists() or not items_path.exists():
            cfg = SceneCatalogTestDataConfig(
                output_dir=gen_dir,
                start_datetime="2021-01-05T09:13:51Z",
                n_timestamps=10,
                tiles_per_timestamp=15,
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

        # Where pipeline will write its CSVs (left for manual inspection):
        cls.tables_dir = Path("tests/artifacts/pipeline_tables")
        cls.tables_dir.mkdir(parents=True, exist_ok=True)

        # Patch RepoPaths.TABLES so pipeline writes here
        cls._orig_tables = RepoPaths.TABLES
        RepoPaths.TABLES = cls.tables_dir

    @classmethod
    def tearDownClass(cls) -> None:
        RepoPaths.TABLES = cls._orig_tables

    def setUp(self) -> None:
        # SceneCatalogBuilder using fake service backed by generated RAW items
        fake_service = FakeCdseSceneCatalogService(self.raw_items)
        self.builder = SceneCatalogBuilder(service=fake_service)

    # ---------------------------------------------------------- #
    # Tests rely only on BuildSceneCatalogPipeline.run + params
    # ---------------------------------------------------------- #

    def testRunUsesTileSelectorAndWritesCsvs(self):
        """
        High-level realistic scenario:

        - RAW generator clouds uniform in [0, 80]
        - STAC-like layer filters to cloud_cover_max=20
        - selector enabled with unions of many tiles
        - moderate number of anchors (monthly-ish)
        """
        pipeline = BuildSceneCatalogPipeline(aoi_path=self.aoi_path, builder=self.builder)

        params = BuildSceneCatalogParams(
            date_start="2021-01-01",
            cloud_cover_max=20.0,   # realistic filter
            max_items=1000,
            collection="sentinel-2-l2a",
            use_tile_selector=True,
            full_cover_threshold=0.5,
            allow_union=True,
            max_union_tiles=15,
            n_anchors=12,
            window_days=21,
        )

        # Sanity: RAW synthetic items must include clouds > 20 %
        num_high_cloud = sum(
            1
            for it in self.raw_items
            if float(it.get("properties", {}).get("cloud_cover", 0.0)) > 20.0
        )
        self.assertGreater(
            num_high_cloud, 0,
            msg="Generator should produce some scenes with cloud_cover > 20%"
        )

        out_path = pipeline.run(params)

        raw_csv = self.tables_dir / "scenes_catalog.csv"
        selected_csv = self.tables_dir / "scenes_selected.csv"
        ts_csv = self.tables_dir / "time_serie.csv"
        ts_cov_csv = self.tables_dir / "timestamps_coverage.csv"

        self.assertTrue(raw_csv.exists())
        self.assertTrue(selected_csv.exists())
        self.assertTrue(ts_csv.exists())
        self.assertTrue(ts_cov_csv.exists())

        # Non-empty + selector-enabled → pipeline should return ts_csv
        self.assertEqual(out_path, ts_csv)

        raw_df = pd.read_csv(raw_csv)

        self.assertGreater(len(raw_df), 0)
        self.assertIn("cloud_cover", raw_df.columns)
        self.assertIn("platform", raw_df.columns)
        self.assertIn("collection", raw_df.columns)

        # After STAC filter, all scenes must have <= 20% cloud
        self.assertLessEqual(raw_df["cloud_cover"].max(), 20.0 + 1e-6)

        cov_df = pd.read_csv(ts_cov_csv)
        self.assertFalse(cov_df.empty)
        self.assertIn("coverage_frac", cov_df.columns)
        self.assertIn("has_full_cover", cov_df.columns)

        # At least one timestamp with "full-ish" cover (>= full_cover_threshold)
        self.assertTrue(cov_df["has_full_cover"].any())

        ts_df = pd.read_csv(ts_csv)
        self.assertFalse(ts_df.empty)
        self.assertIn("coverage_frac", ts_df.columns)
        self.assertIn("cloud_score", ts_df.columns)
        self.assertIn("tiles_count", ts_df.columns)

        self.assertLessEqual(len(ts_df), params.n_anchors)
        self.assertGreater(ts_df["coverage_frac"].min(), 0.0)
        self.assertLessEqual(ts_df["coverage_frac"].max(), 1.0)
        self.assertGreaterEqual(ts_df["cloud_score"].min(), 0.0)
        self.assertLessEqual(ts_df["cloud_score"].max(), 20.0 + 1e-6)

    def testRunWithNoItemsMatchesEmptyBranch(self):
        """
        Equivalent to a case where STAC search returns no items.

        This exercises the early-return branch in BuildSceneCatalogPipeline.run
        but still leaves CSVs to inspect.
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

        self.assertTrue(raw_csv.exists())
        raw_df = pd.read_csv(raw_csv)
        self.assertTrue(raw_df.empty)

        for csv_path in (selected_csv, ts_csv, ts_cov_csv):
            self.assertTrue(csv_path.exists())
            df = pd.read_csv(csv_path)
            self.assertTrue(df.empty)

        self.assertEqual(out_path, raw_csv)