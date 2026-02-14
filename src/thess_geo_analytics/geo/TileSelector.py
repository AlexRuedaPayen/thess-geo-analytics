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
    - anchor_date: the interpolated / regular-grid date you want in your time series
    - acq_dt: the real acquisition timestamp of the chosen scene group
    - items: 1..k tiles (usually 1 or 2) used to cover the AOI
    - cloud_score: the selection score used across timestamps (see rule below)
    - coverage_frac: AOI coverage fraction achieved by the chosen union
    """
    anchor_date: date
    acq_dt: datetime
    items: List[ItemLike]
    cloud_score: float
    coverage_frac: float


class TileSelector:
    """
    Build a *regular* time series over a period T by selecting the best *real* scene around
    each anchor date.

    RULE (your spec):
      1) Split period T into n equal intervals; define anchor dates d_i as the midpoint of each interval.
      2) For each anchor date d:
           - consider items acquired within a centered window [d - half_window, d + half_window]
           - group candidate items by their real acquisition timestamp (acq_dt)
           - for each timestamp group, find the best union of tiles that covers the AOI
             (single tile, then pairs, then optionally triples... up to max_union_tiles)
           - score that best union by cloud_score = max(cloud of tiles in union)
           - pick the timestamp group with the minimum cloud_score
             (tie-break: higher coverage, then closer to anchor, then fewer tiles)
      3) Output one SelectedScene per anchor date (fictional anchor, real acquisition timestamp kept).

    Notes:
      - cloud is scene-level metadata, not per-pixel. Using max() for unions is conservative and matches
        your "take the worst tile in the mosaic" rule.
      - For Thessaloniki AOI, unions should typically be 1 tile (sometimes 2).
    """

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
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

        Returns a list of SelectedScene (len <= n_anchors if some anchors have no candidates).
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

        # Precompute AOI coverage infos (in an area CRS if possible)
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

            # candidate timestamps within window
            candidate_dts = [dt for dt in by_dt.keys() if lo <= dt.date() <= hi]
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

    # ------------------------------------------------------------------
    # Internals: anchors
    # ------------------------------------------------------------------
    def _make_midpoint_anchors(self, start: date, end: date, n: int) -> List[date]:
        """
        Split [start, end] into n equal intervals (in days, inclusive endpoints),
        and return the midpoint date of each interval.
        """
        total_days = (end - start).days
        if total_days == 0:
            return [start] * n

        # interval length in floating days
        step = total_days / n
        anchors: List[date] = []

        for i in range(n):
            # interval i spans [start + i*step, start + (i+1)*step]
            mid = (i + 0.5) * step
            anchors.append(start + timedelta(days=int(round(mid))))

        # ensure anchors are within bounds
        anchors = [min(max(a, start), end) for a in anchors]
        return anchors

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
          - compute best union of tiles that covers AOI (or best-coverage fallback)
          - cloud_score = max(cloud among tiles in chosen union)
        Choose dt with minimal cloud_score (ties: higher coverage, closer to anchor, fewer tiles).
        Returns (coverage_frac, cloud_score, chosen_dt, chosen_infos)
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
        Find best union of tiles *within the same acquisition timestamp*.

        Preference:
          - any union with coverage >= full_cover_threshold wins over non-full
          - within same "full vs not full": minimize union cloud_score = max(cloud)
          - then maximize coverage
          - then fewer tiles

        If allow_union is False, only consider singles.
        """
        if not infos or aoi_area_value <= 0:
            return None

        max_k = 1
        if self.allow_union:
            max_k = max(1, self.max_union_tiles)

        best: Optional[Tuple[float, float, List[CoverageInfo]]] = None  # (cov_frac, cloud_score, infos)

        # enumerate unions of size 1..max_k
        for k in range(1, min(max_k, len(infos)) + 1):
            for combo in combinations(infos, k):
                union_geom = combo[0].covered_geom
                for ci in combo[1:]:
                    union_geom = union_geom.union(ci.covered_geom)

                cov_frac = float(union_geom.area) / aoi_area_value
                cloud_score = self._union_cloud_score_max(combo)

                if cov_frac < self.min_intersection_frac:
                    continue

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
        # Your rule: for a union, the cloud score is the MAX cloud among the tiles in the union.
        return float(max(ci.cloud for ci in combo)) if combo else float("inf")

    # ------------------------------------------------------------------
    # Smoke test
    # ------------------------------------------------------------------
    @staticmethod
    def smoke_test() -> None:
        from datetime import timezone
        from shapely.geometry import box, mapping

        print("=== TileSelector Smoke Test (regular anchors) ===")

        def _item(item_id: str, geom, dt: datetime, cloud: float):
            return {
                "id": item_id,
                "geometry": mapping(geom),
                "properties": {
                    "datetime": dt.isoformat().replace("+00:00", "Z"),
                    "cloud_cover": cloud,
                },
            }

        aoi = box(0, 0, 2, 2)

        # Two timestamps in window: pick the one with smaller MAX-cloud union
        dt_a = datetime(2024, 1, 10, 9, 0, tzinfo=timezone.utc)  # timestamp A
        dt_b = datetime(2024, 1, 12, 9, 0, tzinfo=timezone.utc)  # timestamp B

        # Timestamp A needs a pair to cover AOI: clouds 2 and 9 -> score max=9
        left_a = _item("left_a", box(0, 0, 1, 2), dt_a, 2.0)
        right_a = _item("right_a", box(1, 0, 2, 2), dt_a, 9.0)

        # Timestamp B single covers AOI with cloud 6 -> score=6 (should win)
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

        for s in selected:
            ids = [it["id"] for it in s.items]
            print(f"anchor={s.anchor_date} -> acq={s.acq_dt.date()} ids={ids} cloud_score={s.cloud_score:.2f} cov={s.coverage_frac:.3f}")

        assert any(s.cloud_score == 6.0 for s in selected), "Expected the full_b scene to be selected at least once"
        print("✓ Smoke test OK")


if __name__ == "__main__":
    TileSelector.smoke_test()

