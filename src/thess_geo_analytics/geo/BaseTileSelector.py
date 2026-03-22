# thess_geo_analytics/geo/BaseTileSelector.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import combinations
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from shapely.geometry import shape
from shapely.ops import transform as shp_transform

try:
    from pyproj import CRS, Transformer
except ImportError:
    CRS = None
    Transformer = None


ItemLike = Union[Mapping[str, Any], Any]


@dataclass(frozen=True)
class CoverageInfo:
    item: ItemLike
    frac: float
    covered_geom: Any
    acq_dt: datetime
    acq_date: date


@dataclass(frozen=True)
class SelectedScene:
    anchor_date: date
    acq_dt: datetime
    items: List[ItemLike]
    quality_score: Any
    coverage_frac: float
    coverage_area: float


@dataclass(frozen=True)
class RankedCandidate:
    anchor_date: date
    acq_dt: datetime
    items: List[ItemLike]
    quality_score: Any
    coverage_frac: float
    dist_days: int


class BaseTileSelector:
    """
    Shared tile selector logic.

    Owns:
      - anchor generation
      - AOI/footprint coverage computation
      - tile-union search within a timestamp
      - regular time-series orchestration

    Subclasses define:
      - how a union is scored
      - how unions are compared
      - how timestamps are compared
    """

    def __init__(
        self,
        *,
        full_cover_threshold: float = 0.999,
        allow_union: bool = True,
        max_union_tiles: int = 2,
        min_intersection_frac: float = 1e-6,
        datetime_key: str = "datetime",
    ):
        self.full_cover_threshold = float(full_cover_threshold)
        self.allow_union = bool(allow_union)
        self.max_union_tiles = int(max_union_tiles)
        self.min_intersection_frac = float(min_intersection_frac)
        self.datetime_key = datetime_key

        if self.max_union_tiles < 1:
            raise ValueError("max_union_tiles must be >= 1")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def rank_candidates_for_anchor(
        self,
        *,
        items: Sequence[ItemLike],
        aoi_geom_4326,
        anchor_date: date,
        window_days: int = 21,
        top_k: int = 5,
    ) -> List[RankedCandidate]:
        if not items:
            return []
        if window_days <= 0:
            raise ValueError("window_days must be > 0")
        if top_k <= 0:
            return []

        half = window_days // 2
        lo = anchor_date - timedelta(days=half)
        hi = anchor_date + timedelta(days=half)

        infos_all, _, _, aoi_area_value = self._coverage_infos(items, aoi_geom_4326)
        if not infos_all or aoi_area_value <= 0:
            return []

        by_dt: Dict[datetime, List[CoverageInfo]] = {}
        for ci in infos_all:
            if lo <= ci.acq_date <= hi:
                by_dt.setdefault(ci.acq_dt, []).append(ci)

        ranked: List[RankedCandidate] = []

        for dt, infos in by_dt.items():
            chosen = self._best_union_for_timestamp(infos, aoi_area_value)
            if chosen is None:
                continue

            cov_frac, quality_score, chosen_infos = chosen
            dist_days = abs((dt.date() - anchor_date).days)

            ranked.append(
                RankedCandidate(
                    anchor_date=anchor_date,
                    acq_dt=dt,
                    items=[ci.item for ci in chosen_infos],
                    quality_score=quality_score,
                    coverage_frac=cov_frac,
                    dist_days=dist_days,
                )
            )

        ranked.sort(key=self._ranked_candidate_sort_key)
        return ranked[:top_k]

    def select_regular_time_series(
        self,
        items: Sequence[ItemLike],
        aoi_geom_4326,
        *,
        period_start: date,
        period_end: date,
        n_anchors: int,
        window_days: int = 15,
    ) -> List[SelectedScene]:
        if not items:
            return []
        if n_anchors <= 0:
            raise ValueError("n_anchors must be > 0")
        if period_end < period_start:
            raise ValueError("period_end must be >= period_start")
        if window_days <= 0:
            raise ValueError("window_days must be > 0")

        anchors = self._make_midpoint_anchors(period_start, period_end, n_anchors)
        half = window_days // 2

        infos_all, _, _, aoi_area_value = self._coverage_infos(items, aoi_geom_4326)
        if not infos_all or aoi_area_value <= 0:
            return []

        by_dt: Dict[datetime, List[CoverageInfo]] = {}
        for ci in infos_all:
            by_dt.setdefault(ci.acq_dt, []).append(ci)

        out: List[SelectedScene] = []

        for anchor in anchors:
            lo = anchor - timedelta(days=half)
            hi = anchor + timedelta(days=half)

            candidate_dts = sorted([dt for dt in by_dt.keys() if lo <= dt.date() <= hi])
            if not candidate_dts:
                continue

            best = self._choose_best_timestamp(
                candidate_dts=candidate_dts,
                by_dt=by_dt,
                aoi_area_value=aoi_area_value,
                anchor=anchor,
            )
            if best is None:
                continue

            cov_frac, quality_score, chosen_dt, chosen_infos = best
            cov_area = cov_frac * aoi_area_value

            out.append(
                SelectedScene(
                    anchor_date=anchor,
                    acq_dt=chosen_dt,
                    items=[ci.item for ci in chosen_infos],
                    quality_score=quality_score,
                    coverage_frac=cov_frac,
                    coverage_area=cov_area,
                )
            )

        return out

    def debug_coverage(self, items: Sequence[ItemLike], aoi_geom_4326, n: int = 5) -> None:
        infos, proj, _, aoi_area_value = self._coverage_infos(items, aoi_geom_4326)
        print("[DBG] pyproj:", proj is not None)
        print("[DBG] AOI area:", aoi_area_value)

        if not infos:
            print("[DBG] No intersections at all (AOI/footprints mismatch?)")
            return

        print("\n[DBG] First infos:")
        for ci in infos[:n]:
            item_id = getattr(ci.item, "id", None) or (
                ci.item.get("id") if isinstance(ci.item, Mapping) else None
            )
            print(f"  id={item_id} acq={ci.acq_dt} frac={ci.frac:.4f}")

        infos_sorted = sorted(infos, key=lambda x: x.frac, reverse=True)
        print("\n[DBG] Top by frac:")
        for ci in infos_sorted[:n]:
            item_id = getattr(ci.item, "id", None) or (
                ci.item.get("id") if isinstance(ci.item, Mapping) else None
            )
            print(f"  id={item_id} acq={ci.acq_dt} frac={ci.frac:.4f}")

    # ------------------------------------------------------------------
    # Hooks for subclasses
    # ------------------------------------------------------------------

    def _score_union(self, combo: Sequence[CoverageInfo]) -> Any:
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def _ranked_candidate_sort_key(self, candidate: RankedCandidate):
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Internals: anchors
    # ------------------------------------------------------------------

    def _make_midpoint_anchors(self, start: date, end: date, n: int) -> List[date]:
        total_days = (end - start).days
        if total_days <= 0:
            return [start] * n

        step = total_days / n
        anchors: List[date] = []
        seen: set[date] = set()

        for i in range(n):
            mid = (i + 0.5) * step
            d = start + timedelta(days=int(mid))
            d = min(max(d, start), end)
            if d not in seen:
                anchors.append(d)
                seen.add(d)

        cur = anchors[-1] if anchors else start
        while len(anchors) < n and cur < end:
            cur = min(cur + timedelta(days=1), end)
            if cur not in seen:
                anchors.append(cur)
                seen.add(cur)

        while len(anchors) < n:
            anchors.append(end)

        return anchors

    # ------------------------------------------------------------------
    # Internals: item access
    # ------------------------------------------------------------------

    def _get_prop(self, item: ItemLike, key: str, default=None):
        if hasattr(item, "properties"):
            return (item.properties or {}).get(key, default)
        return (item.get("properties", {}) or {}).get(key, default)

    def _get_geometry(self, item: ItemLike) -> Dict[str, Any]:
        if hasattr(item, "properties"):
            props = item.properties or {}
            g = props.get("proj:geometry")
            if isinstance(g, dict) and "type" in g and "coordinates" in g:
                return g
            return item.geometry

        props = item.get("properties", {}) or {}
        g = props.get("proj:geometry")
        if isinstance(g, dict) and "type" in g and "coordinates" in g:
            return g
        return item["geometry"]

    def _get_datetime(self, item: ItemLike) -> datetime:
        dt = self._get_prop(item, self.datetime_key)
        if dt is None and hasattr(item, "datetime") and item.datetime is not None:
            return item.datetime
        if isinstance(dt, datetime):
            return dt
        if isinstance(dt, str):
            return datetime.fromisoformat(dt.replace("Z", "+00:00"))
        raise ValueError("Item missing datetime (properties.datetime or item.datetime).")

    # ------------------------------------------------------------------
    # Internals: projection + coverage
    # ------------------------------------------------------------------

    def _to_area_crs(self, aoi_geom_4326):
        if Transformer is None:
            return None, aoi_geom_4326

        lon, lat = aoi_geom_4326.centroid.x, aoi_geom_4326.centroid.y
        zone = int((lon + 180) // 6) + 1
        epsg = 32600 + zone if lat >= 0 else 32700 + zone

        crs_from = CRS.from_epsg(4326)
        crs_to = CRS.from_epsg(epsg)
        transformer = Transformer.from_crs(crs_from, crs_to, always_xy=True)

        proj = lambda x, y, z=None: transformer.transform(x, y)
        aoi_area_geom = shp_transform(proj, aoi_geom_4326)
        return proj, aoi_area_geom

    def _coverage_infos(
        self,
        items: Sequence[ItemLike],
        aoi_geom_4326,
    ) -> Tuple[List[CoverageInfo], Optional[Any], Any, float]:
        proj, aoi_area_geom = self._to_area_crs(aoi_geom_4326)
        aoi_area_value = float(aoi_area_geom.area) if aoi_area_geom.area else 0.0

        infos: List[CoverageInfo] = []
        for it in items:
            footprint_4326 = shape(self._get_geometry(it))
            acq_dt = self._get_datetime(it)

            if proj is not None:
                footprint_area = shp_transform(proj, footprint_4326)
                inter = aoi_area_geom.intersection(footprint_area)
            else:
                inter = aoi_geom_4326.intersection(footprint_4326)

            if inter.is_empty:
                continue

            inter_area = float(inter.area)
            frac = (inter_area / aoi_area_value) if aoi_area_value > 0 else 0.0
            if frac < self.min_intersection_frac:
                continue

            infos.append(
                CoverageInfo(
                    item=it,
                    frac=frac,
                    covered_geom=inter,
                    acq_dt=acq_dt,
                    acq_date=acq_dt.date(),
                )
            )

        return infos, proj, aoi_area_geom, aoi_area_value

    # ------------------------------------------------------------------
    # Internals: choose best timestamp around an anchor
    # ------------------------------------------------------------------

    def _choose_best_timestamp(
        self,
        *,
        candidate_dts: List[datetime],
        by_dt: Dict[datetime, List[CoverageInfo]],
        aoi_area_value: float,
        anchor: date,
    ) -> Optional[Tuple[float, Any, datetime, List[CoverageInfo]]]:
        best: Optional[Tuple[float, Any, datetime, List[CoverageInfo]]] = None

        for dt in candidate_dts:
            infos = by_dt.get(dt, [])
            if not infos:
                continue

            chosen = self._best_union_for_timestamp(infos, aoi_area_value)
            if chosen is None:
                continue

            cov_frac, quality_score, chosen_infos = chosen

            if best is None:
                best = (cov_frac, quality_score, dt, chosen_infos)
                continue

            best_cov, best_quality, best_dt, best_infos = best

            if self._is_better_timestamp(
                cov_frac=cov_frac,
                quality_score=quality_score,
                dt=dt,
                chosen_infos=chosen_infos,
                best_cov=best_cov,
                best_quality=best_quality,
                best_dt=best_dt,
                best_infos=best_infos,
                anchor=anchor,
            ):
                best = (cov_frac, quality_score, dt, chosen_infos)

        return best

    # ------------------------------------------------------------------
    # Internals: unions within a timestamp group
    # ------------------------------------------------------------------

    def _best_union_for_timestamp(
        self,
        infos: Sequence[CoverageInfo],
        aoi_area_value: float,
    ) -> Optional[Tuple[float, Any, List[CoverageInfo]]]:
        if not infos or aoi_area_value <= 0:
            return None

        max_k = 1 if not self.allow_union else max(1, self.max_union_tiles)

        best_full: Optional[Tuple[float, Any, List[CoverageInfo]]] = None
        best_any: Optional[Tuple[float, Any, List[CoverageInfo]]] = None

        for k in range(1, min(max_k, len(infos)) + 1):
            for combo in combinations(infos, k):
                union_geom = combo[0].covered_geom
                for ci in combo[1:]:
                    union_geom = union_geom.union(ci.covered_geom)

                cov_frac = float(union_geom.area) / aoi_area_value
                if cov_frac < self.min_intersection_frac:
                    continue

                combo_infos = list(combo)
                quality_score = self._score_union(combo_infos)

                if best_any is None:
                    best_any = (cov_frac, quality_score, combo_infos)
                else:
                    any_cov, any_quality, any_combo = best_any
                    if self._is_better_union(
                        cov_frac=cov_frac,
                        quality_score=quality_score,
                        combo_infos=combo_infos,
                        best_cov=any_cov,
                        best_quality=any_quality,
                        best_combo=any_combo,
                    ):
                        best_any = (cov_frac, quality_score, combo_infos)

                if cov_frac < self.full_cover_threshold:
                    continue

                if best_full is None:
                    best_full = (cov_frac, quality_score, combo_infos)
                    continue

                best_cov, best_quality, best_combo = best_full
                if self._is_better_union(
                    cov_frac=cov_frac,
                    quality_score=quality_score,
                    combo_infos=combo_infos,
                    best_cov=best_cov,
                    best_quality=best_quality,
                    best_combo=best_combo,
                ):
                    best_full = (cov_frac, quality_score, combo_infos)

        if best_full is not None:
            return best_full

        if best_any is not None:
            best_cov, best_quality, _ = best_any
            raise ValueError(
                f"No union reaches full_cover_threshold={self.full_cover_threshold!r}; "
                f"best coverage was {best_cov:.6f} with quality_score={best_quality!r}."
            )

        return None