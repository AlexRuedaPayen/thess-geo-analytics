from __future__ import annotations

import json
import unittest
from datetime import date
from pathlib import Path

from shapely.geometry import shape

from thess_geo_analytics.geo.TileSelector import TileSelector


class TileSelectorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Load AOI + items from disk, assuming they were generated beforehand
        by SceneCatalogTestDataGenerator.

        Expected files:

          tests/fixtures/generated/scene_catalog/aoi_scene_catalog.geojson
          tests/fixtures/generated/scene_catalog/scene_catalog_items.json
        """
        gen_dir = Path("tests/fixtures/generated/scene_catalog")
        aoi_path = gen_dir / "aoi_scene_catalog.geojson"
        items_path = gen_dir / "scene_catalog_items.json"

        if not aoi_path.exists() or not items_path.exists():
            raise unittest.SkipTest(
                "Scene catalog test data not found for TileSelectorTest. "
                "Please run SceneCatalogTestDataGenerator beforehand "
                "to create aoi_scene_catalog.geojson and scene_catalog_items.json."
            )

        with aoi_path.open("r", encoding="utf-8") as f:
            aoi_fc = json.load(f)
        cls.aoi_geom = shape(aoi_fc["features"][0]["geometry"])

        with items_path.open("r", encoding="utf-8") as f:
            cls.items = json.load(f)

        cls.aoi_path = aoi_path
        cls.items_path = items_path

    # ------------------------------------------------------------------ #
    # Tests using only high-level public API
    # ------------------------------------------------------------------ #

    def testRankCandidatesUsesCloudAndCoverage(self):
        selector = TileSelector(
            full_cover_threshold=0.5,
            allow_union=True,
            max_union_tiles=1,
        )

        anchor = date(2021, 1, 15)

        ranked = selector.rank_candidates_for_anchor(
            items=self.items,
            aoi_geom_4326=self.aoi_geom,
            anchor_date=anchor,
            window_days=21,
            top_k=5,
        )

        self.assertGreaterEqual(len(ranked), 2)

        clouds = [c.cloud_score for c in ranked]
        self.assertAlmostEqual(min(clouds), ranked[0].cloud_score, places=3)

        for c in ranked:
            self.assertGreater(c.coverage_frac, 0.0)

    def testSelectRegularTimeSeriesHighLevel(self):
        selector = TileSelector(
            full_cover_threshold=0.5,
            allow_union=True,
            max_union_tiles=1,
        )

        period_start = date(2021, 1, 1)
        period_end = date(2021, 2, 1)
        n_anchors = 4
        window_days = 21

        selected_scenes = selector.select_regular_time_series(
            items=self.items,
            aoi_geom_4326=self.aoi_geom,
            period_start=period_start,
            period_end=period_end,
            n_anchors=n_anchors,
            window_days=window_days,
        )

        self.assertGreater(len(selected_scenes), 0)
        for s in selected_scenes:
            self.assertGreater(s.coverage_frac, 0.0)
            self.assertLessEqual(s.cloud_score, 100.0)
            self.assertGreaterEqual(len(s.items), 1)