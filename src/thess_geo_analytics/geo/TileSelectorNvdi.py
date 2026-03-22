# thess_geo_analytics/geo/TileSelectorNdvi.py
from __future__ import annotations

from datetime import date, datetime
from typing import Any, List, Sequence

from thess_geo_analytics.geo.BaseTileSelector import (
    BaseTileSelector,
    CoverageInfo,
    RankedCandidate,
)


class TileSelectorNdvi(BaseTileSelector):
    """
    NDVI / Sentinel-2 selector.

    Ranking logic:
      - per union: lower max cloud is better
      - tie: higher coverage
      - tie: fewer tiles

    per timestamp:
      - lower max cloud
      - tie: higher coverage
      - tie: closer to anchor date
      - tie: fewer tiles
    """

    def __init__(
        self,
        *,
        full_cover_threshold: float = 0.999,
        allow_union: bool = True,
        max_union_tiles: int = 2,
        min_intersection_frac: float = 1e-6,
        datetime_key: str = "datetime",
        cloud_keys: Sequence[str] = ("cloud_cover", "eo:cloud_cover"),
    ):
        super().__init__(
            full_cover_threshold=full_cover_threshold,
            allow_union=allow_union,
            max_union_tiles=max_union_tiles,
            min_intersection_frac=min_intersection_frac,
            datetime_key=datetime_key,
        )
        self.cloud_keys = tuple(cloud_keys)

    def _get_cloud(self, item) -> float:
        for k in self.cloud_keys:
            v = self._get_prop(item, k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        return float("inf")

    def _score_union(self, combo: Sequence[CoverageInfo]) -> float:
        if not combo:
            return float("inf")
        return float(max(self._get_cloud(ci.item) for ci in combo))

    def _is_better_union(
        self,
        *,
        cov_frac: float,
        quality_score: Any,
        combo_infos: List[CoverageInfo],
        best_cov: float,
        best_quality: Any,
        best_combo: List[CoverageInfo],
    ) -> bool:
        if float(quality_score) < float(best_quality) - 1e-12:
            return True

        if abs(float(quality_score) - float(best_quality)) <= 1e-12:
            if cov_frac > best_cov + 1e-12:
                return True

            if abs(cov_frac - best_cov) <= 1e-12 and len(combo_infos) < len(best_combo):
                return True

        return False

    def _is_better_timestamp(
        self,
        *,
        cov_frac: float,
        quality_score: Any,
        dt: datetime,
        chosen_infos: List[CoverageInfo],
        best_cov: float,
        best_quality: Any,
        best_dt: datetime,
        best_infos: List[CoverageInfo],
        anchor: date,
    ) -> bool:
        if float(quality_score) < float(best_quality) - 1e-12:
            return True

        if abs(float(quality_score) - float(best_quality)) <= 1e-12:
            if cov_frac > best_cov + 1e-12:
                return True

            if abs(cov_frac - best_cov) <= 1e-12:
                dist = abs((dt.date() - anchor).days)
                best_dist = abs((best_dt.date() - anchor).days)
                if dist < best_dist:
                    return True

                if dist == best_dist and len(chosen_infos) < len(best_infos):
                    return True

        return False

    def _ranked_candidate_sort_key(self, candidate: RankedCandidate):
        return (
            float(candidate.quality_score),
            -float(candidate.coverage_frac),
            int(candidate.dist_days),
            len(candidate.items),
        )