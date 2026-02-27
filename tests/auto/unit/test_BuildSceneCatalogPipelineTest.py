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
    - returns in-memory items produced by SceneCatalogTestDataGenerator
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
        return self._items, aoi_geom

    def items_to_dataframe(self, items: List[Any], *, collection: str) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for it in items:
            props = it.get("properties", {})
            rows.append(
                {
                    "id": it["id"],
                    "datetime": props["datetime"],
                    "cloud_cover": props.get("cloud_cover"),
                    "platform": props.get("platform"),
                    "constellation": props.get("constellation"),
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
        Generate AOI + fake items once and keep them on disk + in memory.
        """
        gen_dir = Path("tests/fixtures/generated/scene_catalog")
        cfg = SceneCatalogTestDataConfig(
            output_dir=gen_dir,
            start_datetime="2021-01-05T10:00:00Z",
            n_timestamps=5,
            tiles_per_timestamp=1,
            base_cloud=5.0,
            cloud_step=15.0,
        )
        gen = SceneCatalogTestDataGenerator(cfg)
        artifacts = gen.run()

        cls.aoi_path = artifacts["aoi_path"]
        cls.items = artifacts["items"]

        # Where pipeline will write its CSVs (left for manual inspection):
        cls.tables_dir = Path("tests/artifacts/pipeline_tables")
        cls.tables_dir.mkdir(parents=True, exist_ok=True)

        # Patch RepoPaths.TABLES so pipeline writes here
        cls._orig_tables = RepoPaths.TABLES
        RepoPaths.TABLES = cls.tables_dir

    @classmethod
    def tearDownClass(cls) -> None:
        # Restore RepoPaths, but DO NOT delete tables_dir: user might inspect CSVs.
        RepoPaths.TABLES = cls._orig_tables

    def setUp(self) -> None:
        # SceneCatalogBuilder using fake service backed by generated items
        fake_service = FakeCdseSceneCatalogService(self.items)
        self.builder = SceneCatalogBuilder(service=fake_service)

    # ---------------------------------------------------------- #
    # Tests rely only on BuildSceneCatalogPipeline.run + params
    # ---------------------------------------------------------- #

    def testRunUsesTileSelectorAndWritesCsvs(self):
        """
        High-level equivalence of:

        - load config/pipeline.thess.yaml (date_start, scene_catalog_params.*)
        - build BuildSceneCatalogParams
        - run BuildSceneCatalogPipeline(aoi_path).run(params)

        Here we just build params directly, matching the same names.
        """
        pipeline = BuildSceneCatalogPipeline(aoi_path=self.aoi_path, builder=self.builder)

        params = BuildSceneCatalogParams(
            # Equivalent to cfg.raw["pipeline"]["date_start"]
            date_start="2021-01-01",

            # Equivalent to scene_catalog_params in YAML / CLI flags:
            cloud_cover_max=100.0,   # --cloud-max
            max_items=100,           # --max-items
            collection="sentinel-2-l2a",  # --collection

            use_tile_selector=True,        # --use-tile-selector
            full_cover_threshold=0.5,      # --full-cover-threshold
            allow_union=True,              # --allow-union
            max_union_tiles=1,             # --max-union-tiles
            n_anchors=6,                   # --n-anchors
            window_days=21,                # --window-days
        )

        out_path = pipeline.run(params)

        raw_csv = self.tables_dir / "scenes_catalog.csv"
        selected_csv = self.tables_dir / "scenes_selected.csv"
        ts_csv = self.tables_dir / "time_serie.csv"
        ts_cov_csv = self.tables_dir / "timestamps_coverage.csv"

        # Files exist (and remain on disk for manual inspection)
        self.assertTrue(raw_csv.exists())
        self.assertTrue(selected_csv.exists())
        self.assertTrue(ts_csv.exists())
        self.assertTrue(ts_cov_csv.exists())

        # Non-empty + selector-enabled → pipeline should return ts_csv
        self.assertEqual(out_path, ts_csv)

        raw_df = pd.read_csv(raw_csv)
        self.assertGreater(len(raw_df), 0)
        self.assertIn("cloud_cover", raw_df.columns)

        cov_df = pd.read_csv(ts_cov_csv)
        self.assertFalse(cov_df.empty)
        self.assertIn("coverage_frac", cov_df.columns)
        self.assertIn("has_full_cover", cov_df.columns)

        # At least one timestamp with full cover
        self.assertTrue(cov_df["has_full_cover"].any())

        ts_df = pd.read_csv(ts_csv)
        self.assertFalse(ts_df.empty)
        self.assertIn("coverage_frac", ts_df.columns)
        self.assertIn("cloud_score", ts_df.columns)

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

        # In this branch, run() returns raw_csv path
        self.assertEqual(out_path, raw_csv)