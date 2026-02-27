from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path

from thess_geo_analytics.geo.TileSelector import TileSelector
from tests.fixtures.generators.SceneCatalogTestDataGenerator import (
    SceneCatalogTestDataConfig,
    SceneCatalogTestDataGenerator,
)


class TileSelectorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Generate AOI + items once for all tests in this class.

        Artifacts (left on disk for manual inspection):

          tests/fixtures/generated/scene_catalog/aoi_scene_catalog.geojson
          tests/fixtures/generated/scene_catalog/scene_catalog_items.json
        """
        output_dir = Path("tests/fixtures/generated/scene_catalog")

        # These numbers correspond conceptually to the same knobs you have in
        # pipeline.thess.yaml / CLI (cloud, temporal spacing, etc.).
        cfg = SceneCatalogTestDataConfig(
            output_dir=output_dir,
            start_datetime="2021-01-05T10:00:00Z",
            n_timestamps=3,       # like 3 real acquisitions across the period
            tiles_per_timestamp=1,
            base_cloud=10.0,      # first timestamp 10% cloud
            cloud_step=20.0,      # then 30%, 50% ...
        )

        gen = SceneCatalogTestDataGenerator(cfg)
        artifacts = gen.run()

        cls.aoi_geom = artifacts["aoi_geom"]
        cls.items = artifacts["items"]
        cls.aoi_path = artifacts["aoi_path"]
        cls.items_path = artifacts["items_path"]

    # ------------------------------------------------------------------ #
    # Tests using only high-level public API
    # ------------------------------------------------------------------ #

    def testRankCandidatesUsesCloudAndCoverage(self):
        """
        Equivalent to: "Given a time window and AOI, rank the candidate
        acquisitions (timestamps) by cloud + coverage".

        This maps to how the TileSelector is used from the pipeline.
        """
        selector = TileSelector(
            full_cover_threshold=0.5,  # like cfg.scene_catalog_params.full_cover_threshold
            allow_union=True,          # like allow_union / --allow-union
            max_union_tiles=1,         # max_union_tiles / --max-union-tiles
        )

        # Anchor somewhere in the middle of the generated period
        anchor = date(2021, 1, 15)

        ranked = selector.rank_candidates_for_anchor(
            items=self.items,
            aoi_geom_4326=self.aoi_geom,
            anchor_date=anchor,
            window_days=21,   # matches CLI --window-days
            top_k=5,
        )

        # We expect to see at least 2 timestamps in this window
        self.assertGreaterEqual(len(ranked), 2)

        # First candidate should have the lowest cloud_score among them
        clouds = [c.cloud_score for c in ranked]
        self.assertAlmostEqual(min(clouds), ranked[0].cloud_score, places=3)

        # Coverage should be > 0 for all ranked candidates
        for c in ranked:
            self.assertGreater(c.coverage_frac, 0.0)

    def testSelectRegularTimeSeriesHighLevel(self):
        """
        Equivalent to the way pipeline uses TileSelector:

        - define [period_start, period_end] (from date_start .. today)
        - define n_anchors and window_days
        - ask TileSelector to build a regular time series.
        """
        selector = TileSelector(
            full_cover_threshold=0.5,
            allow_union=True,
            max_union_tiles=1,
        )

        period_start = date(2021, 1, 1)
        period_end = date(2021, 2, 1)
        n_anchors = 4     # like cfg.scene_catalog_params.n_anchors
        window_days = 21  # like cfg.scene_catalog_params.window_days

        selected_scenes = selector.select_regular_time_series(
            items=self.items,
            aoi_geom_4326=self.aoi_geom,
            period_start=period_start,
            period_end=period_end,
            n_anchors=n_anchors,
            window_days=window_days,
        )

        # High-level expectations (no internal knowledge):
        # - We should get at least one anchor with a selected scene.
        self.assertGreater(len(selected_scenes), 0)
        # - Coverage and cloud_score should be finite and reasonable.
        for s in selected_scenes:
            self.assertGreater(s.coverage_frac, 0.0)
            self.assertLessEqual(s.cloud_score, 100.0)
            self.assertGreaterEqual(len(s.items), 1)