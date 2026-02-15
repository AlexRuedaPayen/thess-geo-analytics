# tests/test_tile_selector.py
from __future__ import annotations

import unittest
from datetime import date, datetime, timezone

from shapely.geometry import box, mapping

from thess_geo_analytics.geo.TileSelector import TileSelector


def _item(item_id: str, geom, dt: datetime, cloud: float):
    """Dict-like STAC item compatible with TileSelector."""
    return {
        "id": item_id,
        "geometry": mapping(geom),
        "properties": {
            "datetime": dt.isoformat().replace("+00:00", "Z"),
            "cloud_cover": cloud,
            "eo:cloud_cover": cloud,
        },
    }


class TestTileSelectorRegularSeries(unittest.TestCase):
    def test_select_regular_time_series_prefers_lower_max_cloud_union(self) -> None:
        # AOI = 2x2 square
        aoi = box(0, 0, 2, 2)

        # Two candidate timestamps within the window:
        # - dt_a requires a union of 2 tiles with clouds 2 and 9 => union cloud_score = 9
        # - dt_b single full cover with cloud 6 => union cloud_score = 6 (should win)
        dt_a = datetime(2024, 1, 10, 9, 0, tzinfo=timezone.utc)
        dt_b = datetime(2024, 1, 12, 9, 0, tzinfo=timezone.utc)

        left_a = _item("left_a", box(0, 0, 1, 2), dt_a, 2.0)
        right_a = _item("right_a", box(1, 0, 2, 2), dt_a, 9.0)
        full_b = _item("full_b", box(0, 0, 2, 2), dt_b, 6.0)

        selector = TileSelector(full_cover_threshold=0.999, allow_union=True, max_union_tiles=2)

        selected = selector.select_regular_time_series(
            items=[left_a, right_a, full_b],
            aoi_geom_4326=aoi,
            period_start=date(2024, 1, 1),
            period_end=date(2024, 1, 31),
            n_anchors=3,
            window_days=15,
        )

        # We don't require EVERY anchor to pick dt_b (depends on window overlap),
        # but at least one anchor should select the best candidate (full_b with cloud_score 6).
        self.assertTrue(selected, "Expected at least one SelectedScene")

        best_scores = [s.cloud_score for s in selected]
        self.assertIn(6.0, best_scores, "Expected full_b (cloud_score=6) to be selected at least once")

        # If a selection uses dt_b, verify it uses the single tile full_b
        for s in selected:
            if abs(s.cloud_score - 6.0) < 1e-9:
                ids = [it.get("id") for it in s.items]
                self.assertEqual(ids, ["full_b"])
                self.assertGreaterEqual(s.coverage_frac, 0.999)

    def test_rank_candidates_for_anchor_orders_by_cloud_then_coverage_then_distance(self) -> None:
        aoi = box(0, 0, 2, 2)

        anchor = date(2024, 1, 11)

        dt_a = datetime(2024, 1, 10, 9, 0, tzinfo=timezone.utc)  # dist 1 day
        dt_b = datetime(2024, 1, 12, 9, 0, tzinfo=timezone.utc)  # dist 1 day

        # dt_a union requires two tiles => max cloud = 9
        left_a = _item("left_a", box(0, 0, 1, 2), dt_a, 2.0)
        right_a = _item("right_a", box(1, 0, 2, 2), dt_a, 9.0)

        # dt_b single tile full cover => cloud = 6
        full_b = _item("full_b", box(0, 0, 2, 2), dt_b, 6.0)

        selector = TileSelector(full_cover_threshold=0.999, allow_union=True, max_union_tiles=2)

        ranked = selector.rank_candidates_for_anchor(
            items=[left_a, right_a, full_b],
            aoi_geom_4326=aoi,
            anchor_date=anchor,
            window_days=15,
            top_k=5,
        )

        self.assertGreaterEqual(len(ranked), 2)

        # Best should be dt_b (cloud 6 < 9)
        self.assertAlmostEqual(ranked[0].cloud_score, 6.0, places=9)
        ids0 = [it.get("id") for it in ranked[0].items]
        self.assertEqual(ids0, ["full_b"])

        # Second should be dt_a union (cloud 9)
        self.assertAlmostEqual(ranked[1].cloud_score, 9.0, places=9)
        ids1 = sorted([it.get("id") for it in ranked[1].items])
        self.assertEqual(ids1, ["left_a", "right_a"])


if __name__ == "__main__":
    unittest.main()
