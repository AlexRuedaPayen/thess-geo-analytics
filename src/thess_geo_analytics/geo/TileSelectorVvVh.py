# thess_geo_analytics/geo/TileSelectorVvVh.py
from __future__ import annotations

from datetime import date, datetime
from typing import Any, List, Sequence

from thess_geo_analytics.geo.BaseTileSelector import (
    BaseTileSelector,
    CoverageInfo,
    RankedCandidate,
)


class TileSelectorVvVh(BaseTileSelector):
    """
    VV/VH / Sentinel-1 selector.

    Initial SAR ranking logic:
      - per union: higher coverage first, then fewer tiles
      - per timestamp: higher coverage, then closer to anchor, then fewer tiles

    quality_score is kept as a placeholder numeric score for interface consistency.
    For now, all valid unions get quality_score = 0.0.
    """

    def _score_union(self, combo: Sequence[CoverageInfo]) -> float:
        return 0.0

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
            -float(candidate.coverage_frac),
            int(candidate.dist_days),
            len(candidate.items),
        )