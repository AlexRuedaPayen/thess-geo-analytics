# geo/tile_selection.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import combinations
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from shapely.geometry import shape
from shapely.ops import transform as shp_transform

try:
    from pyproj import CRS, Transformer
except ImportError:  # pyproj strongly recommended for correct area math
    CRS = None
    Transformer = None


ItemLike = Union[Mapping[str, Any], Any]  # dict-like or pystac.Item


@dataclass(frozen=True)
class CoverageInfo:
    item: ItemLike
    cloud: float
    frac: float              # fraction of AOI covered by this item's footprint
    covered_geom: Any        # AOI ∩ footprint (in an area CRS if pyproj available)
    acq_date: date           # acquisition date (UTC)


class TileSelector:
    """
    Select least-cloudy items that cover the AOI.

    Selection options:
      1) Per-date: pick best combo for each acquisition date.
      2) Sliding window: pick best combo within a centered window around anchor dates.

    Coverage rules:
      - Prefer a single item that fully covers AOI (>= full_cover_threshold).
      - If not available, optionally consider pairs; prefer full-cover pairs.
      - Fallback to best-coverage single.

    Cloud scoring for pairs:
      Uses an AOI-weighted score that avoids double-counting overlap:
        score = (cloud_a * area(a_only) + cloud_b * area(b_only) + min(cloud_a, cloud_b) * area(overlap)) / aoi_area

      Note: cloud is scene-level metadata, not per-pixel; using min() on overlap is a pragmatic
      approximation consistent with mosaicking "take the better tile where both exist".
    """

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    def __init__(
        self,
        *,
        full_cover_threshold: float = 0.999,
        allow_pair: bool = True,
        min_intersection_frac: float = 1e-6,
        cloud_keys: Sequence[str] = ("cloud_cover", "eo:cloud_cover"),
        datetime_key: str = "datetime",
    ):
        self.full_cover_threshold = float(full_cover_threshold)
        self.allow_pair = bool(allow_pair)
        self.min_intersection_frac = float(min_intersection_frac)
        self.cloud_keys = tuple(cloud_keys)
        self.datetime_key = datetime_key

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def select_best_items_per_date(
        self,
        items: Sequence[ItemLike],
        aoi_geom_4326,
    ) -> Dict[date, List[ItemLike]]:
        """
        Returns {acq_date: [item] or [item1, item2]}.
        """
        by_date = self._group_by_date(items)
        selected: Dict[date, List[ItemLike]] = {}

        for d, day_items in by_date.items():
            infos, _, _, aoi_area_value = self._coverage_infos(day_items, aoi_geom_4326)
            if not infos:
                continue

            chosen_infos = self._select_best_combo(infos, aoi_area_value)
            selected[d] = [ci.item for ci in chosen_infos]

        return selected

    def select_best_items_sliding_window(
        self,
        items: Sequence[ItemLike],
        aoi_geom_4326,
        *,
        window_days: int = 15,
        step_days: int = 5,
    ) -> Dict[date, List[ItemLike]]:
        """
        Sliding window selection.

        Anchors every `step_days` between min_date..max_date.
        For each anchor date A, considers items with acquisition dates in:
            [A - floor(window_days/2), A + floor(window_days/2)]

        Returns {anchor_date: [item] or [item1, item2]}.
        """
        if not items:
            return {}

        # Work from unique acquisition dates present in the items
        dates = sorted({self._get_datetime(it).date() for it in items})
        if not dates:
            return {}

        min_d, max_d = dates[0], dates[-1]
        half = window_days // 2

        anchors: List[date] = []
        cur = min_d
        while cur <= max_d:
            anchors.append(cur)
            cur = cur + timedelta(days=step_days)

        selected: Dict[date, List[ItemLike]] = {}

        # Precompute coverage infos once for all items (more efficient + consistent)
        infos_all, _, _, aoi_area_value = self._coverage_infos(items, aoi_geom_4326)
        if not infos_all or aoi_area_value <= 0:
            return {}

        for anchor in anchors:
            lo = anchor - timedelta(days=half)
            hi = anchor + timedelta(days=half)

            window_infos = [ci for ci in infos_all if lo <= ci.acq_date <= hi]
            if not window_infos:
                continue

            chosen_infos = self._select_best_combo(
                window_infos,
                aoi_area_value,
                anchor_date=anchor,
            )
            selected[anchor] = [ci.item for ci in chosen_infos]

        return selected

    # ------------------------------------------------------------------
    # Internals: item access
    # ------------------------------------------------------------------
    def _get_prop(self, item: ItemLike, key: str, default=None):
        if hasattr(item, "properties"):
            return item.properties.get(key, default)
        return item.get("properties", {}).get(key, default)

    def _get_geometry(self, item: ItemLike) -> Dict[str, Any]:
        if hasattr(item, "geometry"):
            return item.geometry
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

    def _get_cloud(self, item: ItemLike) -> float:
        for k in self.cloud_keys:
            v = self._get_prop(item, k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        return float("inf")

    def _group_by_date(self, items: Sequence[ItemLike]) -> Dict[date, List[ItemLike]]:
        grouped: Dict[date, List[ItemLike]] = {}
        for it in items:
            d = self._get_datetime(it).date()
            grouped.setdefault(d, []).append(it)
        return grouped

    # ------------------------------------------------------------------
    # Internals: projection + coverage
    # ------------------------------------------------------------------
    def _to_area_crs(self, aoi_geom_4326):
        """
        Projects to a UTM zone based on AOI centroid for robust area/coverage comparisons.
        If pyproj is unavailable, returns (None, aoi_geom_4326) which uses degrees^2 areas.
        """
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
                    cloud=self._get_cloud(it),
                    frac=frac,
                    covered_geom=inter,
                    acq_date=self._get_datetime(it).date(),
                )
            )

        return infos, proj, aoi_area_geom, aoi_area_value

    # ------------------------------------------------------------------
    # Internals: scoring + choosing
    # ------------------------------------------------------------------
    def _select_best_combo(
        self,
        infos: Sequence[CoverageInfo],
        aoi_area_value: float,
        *,
        anchor_date: Optional[date] = None,
    ) -> List[CoverageInfo]:
        """
        Pick best [single] or [pair] among infos for a given pool.
        If anchor_date is provided, ties are broken by closeness to anchor.
        """
        if not infos or aoi_area_value <= 0:
            return []

        # 1) Best single full-cover
        full_singles = [ci for ci in infos if ci.frac >= self.full_cover_threshold]
        if full_singles:
            return [min(full_singles, key=lambda ci: self._single_sort_key(ci, anchor_date))]

        # 2) Best pair
        if self.allow_pair and len(infos) >= 2:
            best_pair = self._best_pair(infos, aoi_area_value, anchor_date=anchor_date)
            if best_pair is not None:
                return [best_pair[2], best_pair[3]]

        # 3) Fallback: best coverage single then least cloud then closeness
        best_single = max(
            infos,
            key=lambda ci: (ci.frac, -ci.cloud, -self._date_closeness_bonus(ci, anchor_date)),
        )
        return [best_single]

    def _single_sort_key(self, ci: CoverageInfo, anchor_date: Optional[date]):
        # lower cloud is better; if tie, higher coverage; if tie, closer to anchor
        return (
            float(ci.cloud),
            -float(ci.frac),
            self._date_distance_days(ci.acq_date, anchor_date) if anchor_date else 0,
        )

    def _date_distance_days(self, d: date, anchor: Optional[date]) -> int:
        if anchor is None:
            return 0
        return abs((d - anchor).days)

    def _date_closeness_bonus(self, ci: CoverageInfo, anchor_date: Optional[date]) -> float:
        # Larger bonus for closer dates (used as a tie-breaker only)
        if anchor_date is None:
            return 0.0
        return -float(abs((ci.acq_date - anchor_date).days))

    def _pair_cloud_score(self, a: CoverageInfo, b: CoverageInfo, aoi_area_value: float) -> float:
        """
        AOI-weighted cloud score, avoiding overlap double counting.
        Uses min(cloud) on overlap as a pragmatic mosaic-style approximation.
        """
        if aoi_area_value <= 0:
            return float("inf")

        overlap = a.covered_geom.intersection(b.covered_geom)
        a_only = a.covered_geom.difference(b.covered_geom)
        b_only = b.covered_geom.difference(a.covered_geom)

        area_overlap = float(overlap.area) if not overlap.is_empty else 0.0
        area_a_only = float(a_only.area) if not a_only.is_empty else 0.0
        area_b_only = float(b_only.area) if not b_only.is_empty else 0.0

        weighted = (
            a.cloud * area_a_only
            + b.cloud * area_b_only
            + min(a.cloud, b.cloud) * area_overlap
        )
        return float(weighted / aoi_area_value)

    def _best_pair(
        self,
        infos: Sequence[CoverageInfo],
        aoi_area_value: float,
        *,
        anchor_date: Optional[date] = None,
    ) -> Optional[Tuple[float, float, CoverageInfo, CoverageInfo]]:
        """
        Returns (covered_frac, score, a, b) for best pair by:
          - prefer pairs achieving full_cover_threshold
          - then lower AOI-weighted cloud score
          - then higher covered_frac
          - then closer to anchor_date (tie-break)
        """
        if aoi_area_value <= 0:
            return None

        best: Optional[Tuple[float, float, CoverageInfo, CoverageInfo]] = None

        for a, b in combinations(infos, 2):
            union_geom = a.covered_geom.union(b.covered_geom)
            cov_frac = float(union_geom.area) / aoi_area_value
            score = self._pair_cloud_score(a, b, aoi_area_value)

            if best is None:
                best = (cov_frac, score, a, b)
                continue

            best_cov, best_score, best_a, best_b = best

            a_full = cov_frac >= self.full_cover_threshold
            b_full = best_cov >= self.full_cover_threshold

            better = False
            if a_full and not b_full:
                better = True
            elif a_full == b_full:
                if score < best_score - 1e-12:
                    better = True
                elif abs(score - best_score) <= 1e-12 and cov_frac > best_cov + 1e-12:
                    better = True
                elif abs(score - best_score) <= 1e-12 and abs(cov_frac - best_cov) <= 1e-12 and anchor_date:
                    # tie-break: prefer closer average acquisition date to anchor
                    cur_dist = self._pair_anchor_distance_days(a, b, anchor_date)
                    best_dist = self._pair_anchor_distance_days(best_a, best_b, anchor_date)
                    if cur_dist < best_dist:
                        better = True

            if better:
                best = (cov_frac, score, a, b)

        return best

    def _pair_anchor_distance_days(self, a: CoverageInfo, b: CoverageInfo, anchor: date) -> float:
        # distance of average acquisition date to anchor
        da = a.acq_date
        db = b.acq_date
        mid = da + timedelta(days=(db - da).days / 2) if da <= db else db + timedelta(days=(da - db).days / 2)
        return abs((mid - anchor).days)

    # ------------------------------------------------------------------
    # Smoke test (for development only)
    # ------------------------------------------------------------------
    @staticmethod
    def smoke_test() -> None:
        from datetime import timezone
        from shapely.geometry import box, mapping

        print("=== TileSelector Smoke Test ===")

        def _item(item_id: str, geom, dt: datetime, cloud: float):
            return {
                "id": item_id,
                "geometry": mapping(geom),
                "properties": {
                    "datetime": dt.isoformat().replace("+00:00", "Z"),
                    "cloud_cover": cloud,
                },
            }

        # AOI: 2x2 square
        aoi = box(0, 0, 2, 2)

        selector = TileSelector(full_cover_threshold=0.999, allow_pair=True)

        # ---- Items across multiple dates ----
        dt0 = datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc)
        dt1 = datetime(2024, 1, 6, 9, 0, tzinfo=timezone.utc)
        dt2 = datetime(2024, 1, 11, 9, 0, tzinfo=timezone.utc)
        dt3 = datetime(2024, 1, 16, 9, 0, tzinfo=timezone.utc)

        # Date 1: two full-cover tiles, choose lowest cloud
        full_hi = _item("full_hi", box(0, 0, 2, 2), dt0, 20.0)
        full_lo = _item("full_lo", box(0, 0, 2, 2), dt0, 5.0)

        # Date 2: only pair can cover
        left = _item("left", box(0, 0, 1, 2), dt1, 2.0)
        right = _item("right", box(1, 0, 2, 2), dt1, 3.0)

        # Another date: full cover but higher cloud than best in window
        full_mid = _item("full_mid", box(0, 0, 2, 2), dt2, 8.0)

        # Another date: very low cloud but partial (should not beat a full-cover single if pair/full exists)
        tiny = _item("tiny", box(0, 0, 0.5, 0.5), dt3, 0.0)

        items = [full_hi, full_lo, left, right, full_mid, tiny]

        # ---- Per-date selection ----
        per_date = selector.select_best_items_per_date(items, aoi)
        print("Per-date selection:")
        for d in sorted(per_date):
            ids = [it["id"] for it in per_date[d]]
            print(f"  {d} -> {ids}")

        assert per_date[dt0.date()][0]["id"] == "full_lo", "Per-date: wrong full-cover choice"
        assert set(it["id"] for it in per_date[dt1.date()]) == {"left", "right"}, "Per-date: wrong pair choice"

        # ---- Sliding window selection (15 days, step 5) ----
        sliding = selector.select_best_items_sliding_window(items, aoi, window_days=15, step_days=5)
        print("Sliding-window selection (15d window, 5d step):")
        for d in sorted(sliding):
            ids = [it["id"] for it in sliding[d]]
            print(f"  {d} -> {ids}")

        # Anchor at 2024-01-01: window includes dt0..dt8 => should pick full_lo (full cover, 5.0 cloud)
        first_anchor = min(sliding.keys())
        assert sliding[first_anchor][0]["id"] == "full_lo", "Sliding: first anchor should pick full_lo"

        print("✓ Smoke test OK")


# Allow quick test: python -m geo.tile_selection
if __name__ == "__main__":
    TileSelector.smoke_test()
