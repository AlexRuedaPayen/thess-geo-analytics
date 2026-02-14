# geo/tile_selection.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
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


class TileSelector:
    """
    Selects the least-cloudy tile per date that covers the full AOI.
    If no single tile covers the AOI for a date, optionally selects the best pair
    whose union covers the AOI (or best available pair by coverage/cloud).
    """

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

    # ---------- Public API ----------

    def select_best_items_per_date(
        self,
        items: Sequence[ItemLike],
        aoi_geom_4326,
    ) -> Dict[date, List[ItemLike]]:
        """
        Returns {date: [item] or [item1, item2]}.
        """
        by_date = self._group_by_date(items)
        selected: Dict[date, List[ItemLike]] = {}

        for d, day_items in by_date.items():
            infos, proj, aoi_area_geom, aoi_area_value = self._coverage_infos(
                day_items, aoi_geom_4326
            )
            if not infos:
                continue

            # 1) Single item full cover -> least cloudy
            full = [ci for ci in infos if ci.frac >= self.full_cover_threshold]
            if full:
                best = min(full, key=lambda ci: (ci.cloud, -ci.frac))
                selected[d] = [best.item]
                continue

            # 2) Pair cover (union) -> prefer full cover; then lowest cloud sum; then highest coverage
            if self.allow_pair and len(infos) >= 2:
                best_pair = self._best_pair(infos, aoi_area_value)
                if best_pair is not None:
                    _, _, a, b = best_pair
                    selected[d] = [a.item, b.item]
                    continue

            # 3) Fallback: best single by coverage then least cloud
            best = max(infos, key=lambda ci: (ci.frac, -ci.cloud))
            selected[d] = [best.item]

        return selected

    # ---------- Internals ----------

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
                )
            )

        return infos, proj, aoi_area_geom, aoi_area_value

    def _best_pair(
        self,
        infos: Sequence[CoverageInfo],
        aoi_area_value: float,
    ) -> Optional[Tuple[float, float, CoverageInfo, CoverageInfo]]:
        """
        Returns (covered_frac, cloud_sum, a, b) for best pair by:
          - prefer pairs achieving full_cover_threshold
          - then lower cloud_sum
          - then higher covered_frac
        """
        if aoi_area_value <= 0:
            return None

        best: Optional[Tuple[float, float, CoverageInfo, CoverageInfo]] = None

        for a, b in combinations(infos, 2):
            union_geom = a.covered_geom.union(b.covered_geom)
            cov_frac = float(union_geom.area) / aoi_area_value
            cloud_sum = float(a.cloud + b.cloud)

            if best is None:
                best = (cov_frac, cloud_sum, a, b)
                continue

            best_cov, best_cloud, _, _ = best

            better = False
            a_full = cov_frac >= self.full_cover_threshold
            b_full = best_cov >= self.full_cover_threshold

            if a_full and not b_full:
                better = True
            elif a_full == b_full:
                if cloud_sum < best_cloud - 1e-9:
                    better = True
                elif abs(cloud_sum - best_cloud) <= 1e-9 and cov_frac > best_cov + 1e-9:
                    better = True

            if better:
                best = (cov_frac, cloud_sum, a, b)

        return best
    

if __name__ == "__main__":
    """
    Smoke test for TileSelector.
    Run:
        python -m geo.TileSelector
    """

    from datetime import datetime, timezone
    from shapely.geometry import box, mapping

    print("Running TileSelector smoke test...")

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

    # --- DATE 1: single best full-cover tile expected ---
    dt1 = datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc)

    full_hi_cloud = _item("full_hi_cloud", box(0, 0, 2, 2), dt1, 20.0)
    full_low_cloud = _item("full_low_cloud", box(0, 0, 2, 2), dt1, 5.0)
    partial = _item("partial", box(0, 0, 1, 2), dt1, 0.1)

    # --- DATE 2: pair required ---
    dt2 = datetime(2024, 1, 2, 9, 0, tzinfo=timezone.utc)

    left = _item("left", box(0, 0, 1, 2), dt2, 2.0)
    right = _item("right", box(1, 0, 2, 2), dt2, 3.0)
    tiny = _item("tiny", box(0, 0, 0.2, 0.2), dt2, 0.0)

    items = [
        full_hi_cloud,
        full_low_cloud,
        partial,
        left,
        right,
        tiny,
    ]

    result = selector.select_best_items_per_date(items, aoi)

    print("Selection result:")
    for d in sorted(result):
        ids = [it["id"] for it in result[d]]
        print(f"  {d} -> {ids}")

    # --- Assertions ---
    assert result[dt1.date()][0]["id"] == "full_low_cloud", \
        "Date 1 failed: wrong single-tile selection"

    assert set(it["id"] for it in result[dt2.date]())

