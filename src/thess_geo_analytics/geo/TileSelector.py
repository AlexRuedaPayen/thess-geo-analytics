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
    acq_dt: datetime         # acquisition datetime (UTC, real timestamp)
    acq_date: date           # acquisition date (UTC)


@dataclass(frozen=True)
class SelectedScene:
    """
    Output for one anchor date (fictional grid date).
    - anchor_date: interpolated / regular-grid date you want in your time series
    - acq_dt: real acquisition timestamp of the chosen group
    - items: 1..k tiles used to cover the AOI
    - cloud_score: selection score across timestamps (MAX cloud among tiles in union)
    - coverage_frac: AOI coverage fraction achieved by the chosen union
    """
    anchor_date: date
    acq_dt: datetime
    items: List[ItemLike]
    cloud_score: float
    coverage_frac: float

@dataclass(frozen=True)
class RankedCandidate:
    """
    A ranked candidate for one anchor date.

    - acq_dt: real acquisition timestamp
    - items: chosen union of tiles for that timestamp
    - cloud_score: max cloud in the union (your rule)
    - coverage_frac: AOI coverage fraction achieved by the union
    - dist_days: |acq_date - anchor_date| in days (tie-break info)
    """
    anchor_date: date
    acq_dt: datetime
    items: List[ItemLike]
    cloud_score: float
    coverage_frac: float
    dist_days: int

@dataclass(frozen=True)
class AnchorCandidate:
    anchor_date: date
    acq_dt: datetime
    items: List[ItemLike]
    cloud_score: float
    coverage_frac: float
    dist_days: int

    def rank_candidates_for_anchor(
        self,
        items: Sequence[ItemLike],
        aoi_geom_4326,
        *,
        anchor_date: date,
        window_days: int = 15,
        top_k: int = 10,
    ) -> List[AnchorCandidate]:
        """
        Return ranked candidates (best -> worst) for a single anchor date.
        Uses the exact same union+scoring rules as select_regular_time_series.
        """
        if not items:
            return []

        half = window_days // 2
        lo = anchor_date - timedelta(days=half)
        hi = anchor_date + timedelta(days=half)

        infos_all, _, _, aoi_area_value = self._coverage_infos(items, aoi_geom_4326)
        if not infos_all or aoi_area_value <= 0:
            return []

        # group by real acquisition timestamp
        by_dt: Dict[datetime, List[CoverageInfo]] = {}
        for ci in infos_all:
            if lo <= ci.acq_date <= hi:
                by_dt.setdefault(ci.acq_dt, []).append(ci)

        out: List[AnchorCandidate] = []
        for dt, infos in by_dt.items():
            chosen = self._best_union_for_timestamp(infos, aoi_area_value)
            if chosen is None:
                continue

            cov_frac, chosen_infos = chosen
            cloud_score = self._union_cloud_score_max(chosen_infos)
            dist_days = abs((dt.date() - anchor_date).days)

            out.append(
                AnchorCandidate(
                    anchor_date=anchor_date,
                    acq_dt=dt,
                    items=[ci.item for ci in chosen_infos],
                    cloud_score=cloud_score,
                    coverage_frac=cov_frac,
                    dist_days=dist_days,
                )
            )

        out.sort(
            key=lambda c: (
                c.cloud_score,          # lower is better
                -c.coverage_frac,       # higher coverage is better
                c.dist_days,            # closer is better
                len(c.items),           # fewer tiles is better
                c.acq_dt,               # stable
            )
        )
        return out[: max(1, int(top_k))]


class TileSelector:
    """
    Build a *regular* time series over a period T by selecting the best *real* scene around
    each anchor date.

    RULE:
      1) Split period T into n equal intervals; define anchor dates d_i as the midpoint of each interval.
      2) For each anchor date d:
           - consider items acquired within a centered window [d - half_window, d + half_window]
           - group candidate items by their real acquisition timestamp (acq_dt)
           - for each timestamp group, find the best union of tiles that covers the AOI
             (single tile, then pairs, then triples... up to max_union_tiles)
           - score that best union by cloud_score = max(cloud of tiles in union)
           - pick timestamp group with minimum cloud_score
             (tie-break: higher coverage, then closer to anchor, then fewer tiles)
      3) Output one SelectedScene per anchor date (fictional anchor, real acquisition timestamp kept).
    """

    def __init__(
        self,
        *,
        full_cover_threshold: float = 0.999,
        allow_union: bool = True,
        max_union_tiles: int = 2,
        min_intersection_frac: float = 1e-6,
        cloud_keys: Sequence[str] = ("cloud_cover", "eo:cloud_cover"),
        datetime_key: str = "datetime",
    ):
        self.full_cover_threshold = float(full_cover_threshold)
        self.allow_union = bool(allow_union)
        self.max_union_tiles = int(max_union_tiles)
        self.min_intersection_frac = float(min_intersection_frac)
        self.cloud_keys = tuple(cloud_keys)
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
        """
        Rank the best timestamp-candidates for a SINGLE anchor date.

        Steps:
          - restrict items to ±window_days//2 around anchor_date (by acquisition date)
          - group by real acquisition timestamp (acq_dt)
          - per timestamp: compute best union of tiles (1..max_union_tiles)
          - score union by max cloud among tiles (cloud_score)
          - rank by:
              1) lower cloud_score
              2) higher coverage_frac
              3) closer dist_days
              4) fewer tiles
        """
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

        # filter to window + group by real acquisition datetime
        by_dt: Dict[datetime, List[CoverageInfo]] = {}
        for ci in infos_all:
            if lo <= ci.acq_date <= hi:
                by_dt.setdefault(ci.acq_dt, []).append(ci)

        ranked: List[RankedCandidate] = []

        for dt, infos in by_dt.items():
            chosen = self._best_union_for_timestamp(infos, aoi_area_value)
            if chosen is None:
                continue

            cov_frac, chosen_infos = chosen
            cloud_score = self._union_cloud_score_max(chosen_infos)
            dist_days = abs((dt.date() - anchor_date).days)

            ranked.append(
                RankedCandidate(
                    anchor_date=anchor_date,
                    acq_dt=dt,
                    items=[ci.item for ci in chosen_infos],
                    cloud_score=cloud_score,
                    coverage_frac=cov_frac,
                    dist_days=dist_days,
                )
            )

        ranked.sort(
            key=lambda c: (
                float(c.cloud_score),
                -float(c.coverage_frac),
                int(c.dist_days),
                len(c.items),
            )
        )

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
        """
        Build the regular-grid catalog:
          - anchors = midpoints of n_anchors equal subdivisions between [period_start, period_end]
          - for each anchor, pick best scene in ±window_days//2 around anchor.

        Returns list of SelectedScene (len <= n_anchors if some anchors have no candidates).
        """
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

        # Group by real acquisition timestamp once
        by_dt: Dict[datetime, List[CoverageInfo]] = {}
        for ci in infos_all:
            by_dt.setdefault(ci.acq_dt, []).append(ci)

        out: List[SelectedScene] = []

        for anchor in anchors:
            lo = anchor - timedelta(days=half)
            hi = anchor + timedelta(days=half)

            # stable ordering helps debugging
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

            cov_frac, cloud_score, chosen_dt, chosen_infos = best
            out.append(
                SelectedScene(
                    anchor_date=anchor,
                    acq_dt=chosen_dt,
                    items=[ci.item for ci in chosen_infos],
                    cloud_score=cloud_score,
                    coverage_frac=cov_frac,
                )
            )

        return out

    def debug_coverage(self, items: Sequence[ItemLike], aoi_geom_4326, n: int = 5) -> None:
        """
        Prints coverage fractions for first n items and top n by frac.
        Useful to confirm you're using the correct footprint geometry.
        """
        infos, proj, _, aoi_area_value = self._coverage_infos(items, aoi_geom_4326)
        print("[DBG] pyproj:", proj is not None)
        print("[DBG] AOI area:", aoi_area_value)

        if not infos:
            print("[DBG] No intersections at all (AOI/footprints mismatch?)")
            return

        print("\n[DBG] First infos:")
        for ci in infos[:n]:
            item_id = getattr(ci.item, "id", None) or (ci.item.get("id") if isinstance(ci.item, Mapping) else None)
            print(f"  id={item_id} acq={ci.acq_dt} cloud={ci.cloud} frac={ci.frac:.4f}")

        infos_sorted = sorted(infos, key=lambda x: x.frac, reverse=True)
        print("\n[DBG] Top by frac:")
        for ci in infos_sorted[:n]:
            item_id = getattr(ci.item, "id", None) or (ci.item.get("id") if isinstance(ci.item, Mapping) else None)
            print(f"  id={item_id} acq={ci.acq_dt} cloud={ci.cloud} frac={ci.frac:.4f}")

    # ------------------------------------------------------------------
    # Internals: anchors
    # ------------------------------------------------------------------
    def _make_midpoint_anchors(self, start: date, end: date, n: int) -> List[date]:
        """
        Split [start, end] into n equal intervals (in days),
        return midpoint date of each interval, avoiding duplicates.
        """
        total_days = (end - start).days
        if total_days <= 0:
            return [start] * n

        step = total_days / n
        anchors: List[date] = []
        seen: set[date] = set()

        for i in range(n):
            mid = (i + 0.5) * step
            d = start + timedelta(days=int(mid))  # floor for stability
            d = min(max(d, start), end)
            if d not in seen:
                anchors.append(d)
                seen.add(d)

        # If duplicates were dropped, fill by stepping forward
        cur = anchors[-1] if anchors else start
        while len(anchors) < n and cur < end:
            cur = min(cur + timedelta(days=1), end)
            if cur not in seen:
                anchors.append(cur)
                seen.add(cur)

        # If we still don't have enough (extreme case), pad with end
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
        """
        Prefer the most reliable footprint geometry available.
        Some STAC providers expose a more accurate footprint under proj:geometry.
        Fallback to item.geometry.
        """
        # pystac.Item-like
        if hasattr(item, "properties"):
            props = item.properties or {}
            g = props.get("proj:geometry")
            if isinstance(g, dict) and "type" in g and "coordinates" in g:
                return g
            return item.geometry

        # dict-like
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

    def _get_cloud(self, item: ItemLike) -> float:
        for k in self.cloud_keys:
            v = self._get_prop(item, k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        return float("inf")

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
                    cloud=self._get_cloud(it),
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
    ) -> Optional[Tuple[float, float, datetime, List[CoverageInfo]]]:
        """
        For each real acquisition timestamp dt:
          - compute best union of tiles that covers AOI
          - cloud_score = max(cloud among tiles in chosen union)

        Choose dt with minimal cloud_score (ties: higher coverage, closer to anchor, fewer tiles).
        Returns (coverage_frac, cloud_score, chosen_dt, chosen_infos).
        """
        best: Optional[Tuple[float, float, datetime, List[CoverageInfo]]] = None

        for dt in candidate_dts:
            infos = by_dt.get(dt, [])
            if not infos:
                continue

            chosen = self._best_union_for_timestamp(infos, aoi_area_value)
            if chosen is None:
                continue

            cov_frac, chosen_infos = chosen
            cloud_score = self._union_cloud_score_max(chosen_infos)

            if best is None:
                best = (cov_frac, cloud_score, dt, chosen_infos)
                continue

            best_cov, best_cloud, best_dt, best_infos = best

            # primary: lower cloud_score
            if cloud_score < best_cloud - 1e-12:
                best = (cov_frac, cloud_score, dt, chosen_infos)
                continue

            if abs(cloud_score - best_cloud) <= 1e-12:
                # tie 1: higher coverage
                if cov_frac > best_cov + 1e-12:
                    best = (cov_frac, cloud_score, dt, chosen_infos)
                    continue

                if abs(cov_frac - best_cov) <= 1e-12:
                    # tie 2: closer acquisition date to anchor
                    dist = abs((dt.date() - anchor).days)
                    best_dist = abs((best_dt.date() - anchor).days)
                    if dist < best_dist:
                        best = (cov_frac, cloud_score, dt, chosen_infos)
                        continue

                    if dist == best_dist:
                        # tie 3: fewer tiles
                        if len(chosen_infos) < len(best_infos):
                            best = (cov_frac, cloud_score, dt, chosen_infos)
                            continue

        return best

    # ------------------------------------------------------------------
    # Internals: unions within a timestamp group
    # ------------------------------------------------------------------
    def _best_union_for_timestamp(
        self,
        infos: Sequence[CoverageInfo],
        aoi_area_value: float,
    ) -> Optional[Tuple[float, List[CoverageInfo]]]:
        """
        Find best union of tiles within same acquisition timestamp.

        Preference:
          - any union with coverage >= full_cover_threshold wins over non-full
          - within same "full vs not full": minimize union cloud_score = max(cloud)
          - then maximize coverage
          - then fewer tiles

        If allow_union is False, only consider singles.
        """
        if not infos or aoi_area_value <= 0:
            return None

        max_k = 1 if not self.allow_union else max(1, self.max_union_tiles)

        best: Optional[Tuple[float, float, List[CoverageInfo]]] = None  # (cov_frac, cloud_score, infos)

        for k in range(1, min(max_k, len(infos)) + 1):
            for combo in combinations(infos, k):
                union_geom = combo[0].covered_geom
                for ci in combo[1:]:
                    union_geom = union_geom.union(ci.covered_geom)

                cov_frac = float(union_geom.area) / aoi_area_value
                if cov_frac < self.min_intersection_frac:
                    continue

                cloud_score = self._union_cloud_score_max(combo)

                if best is None:
                    best = (cov_frac, cloud_score, list(combo))
                    continue

                best_cov, best_cloud, best_combo = best

                full = cov_frac >= self.full_cover_threshold
                best_full = best_cov >= self.full_cover_threshold

                better = False
                if full and not best_full:
                    better = True
                elif full == best_full:
                    if cloud_score < best_cloud - 1e-12:
                        better = True
                    elif abs(cloud_score - best_cloud) <= 1e-12 and cov_frac > best_cov + 1e-12:
                        better = True
                    elif (
                        abs(cloud_score - best_cloud) <= 1e-12
                        and abs(cov_frac - best_cov) <= 1e-12
                        and len(combo) < len(best_combo)
                    ):
                        better = True

                if better:
                    best = (cov_frac, cloud_score, list(combo))

        if best is None:
            return None

        cov_frac, _, combo_infos = best
        return cov_frac, combo_infos

    def _union_cloud_score_max(self, combo: Sequence[CoverageInfo]) -> float:
        # Your rule: for a union, cloud score is MAX cloud among tiles in the union.
        return float(max(ci.cloud for ci in combo)) if combo else float("inf")
