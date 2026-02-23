from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import json

import pandas as pd
from shapely.geometry import shape

from thess_geo_analytics.builders.SceneCatalogBuilder import SceneCatalogBuilder
from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION
from thess_geo_analytics.geo.TileSelector import TileSelector


# ---- EDIT THESE PATHS ----------------------------------------------------
AOI_PATH = Path("aoi/EL522_Thessaloniki.geojson")   # your AOI file (the FeatureCollection you pasted)
SCENES_CSV = Path("outputs/tables/scenes_catalog.csv")
COVERAGE_THRESHOLD = 0.95
# -------------------------------------------------------------------------


def main() -> None:
    # 1) Load AOI from FeatureCollection
    with AOI_PATH.open("r", encoding="utf-8") as f:
        aoi_fc = json.load(f)

    # your file is a FeatureCollection with one feature
    aoi_geom = shape(aoi_fc["features"][0]["geometry"])

    # 2) Load scenes_catalog.csv
    df = pd.read_csv(SCENES_CSV)
    if df.empty:
        print("[INFO] scenes_catalog.csv is empty, nothing to do.")
        return

    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    ids_in_csv = set(df["id"])

    date_start = df["datetime"].dt.date.min().isoformat()
    date_end = df["datetime"].dt.date.max().isoformat()
    print(f"[INFO] Date range in CSV: {date_start} → {date_end}")
    print(f"[INFO] Unique items in CSV: {len(ids_in_csv)}")

    # 3) Query STAC again to get items with geometry
    builder = SceneCatalogBuilder()
    params = StacQueryParams(
        collection=DEFAULT_COLLECTION,
        cloud_cover_max=100.0,   # loose; CSV already filtered before
        max_items=10000,
    )

    items, _ = builder.build_scene_items(
        aoi_path=AOI_PATH,
        date_start=date_start,
        date_end=date_end,
        params=params,
    )
    print(f"[INFO] STAC returned {len(items)} items before filtering by CSV IDs.")

    # keep only items present in scenes_catalog.csv
    filtered_items = []
    for it in items:
        if hasattr(it, "id"):
            it_id = str(it.id)
        else:
            it_id = str(it.get("id"))
        if it_id in ids_in_csv:
            filtered_items.append(it)

    print(f"[INFO] Items after filtering to CSV IDs: {len(filtered_items)}")
    if not filtered_items:
        print("[WARN] No matching items found; check AOI/date range/collection.")
        return

    # 4) Compute per-timestamp union coverage using TileSelector internals
    selector = TileSelector()
    infos, _, _, aoi_area_value = selector._coverage_infos(filtered_items, aoi_geom)

    if not infos or aoi_area_value <= 0:
        print("[WARN] No intersections found or AOI area is zero.")
        return

    by_ts = defaultdict(list)
    for ci in infos:
        by_ts[ci.acq_dt].append(ci)

    coverage_per_ts = {}
    for ts, cis in by_ts.items():
        union_geom = cis[0].covered_geom
        for ci in cis[1:]:
            union_geom = union_geom.union(ci.covered_geom)
        cov_frac = float(union_geom.area) / float(aoi_area_value)
        coverage_per_ts[ts] = {
            "coverage_frac": cov_frac,
            "n_tiles": len(cis),
        }

    # 5) Print summary
    thr = COVERAGE_THRESHOLD
    good = {ts: s for ts, s in coverage_per_ts.items() if s["coverage_frac"] >= thr}
    bad = {ts: s for ts, s in coverage_per_ts.items() if s["coverage_frac"] < thr}

    print("\n=== Timestamp union coverage over AOI ===")
    print(f"Total timestamps with any coverage: {len(coverage_per_ts)}")
    print(f"Timestamps with coverage ≥ {thr:.2f}: {len(good)}")
    print(f"Timestamps with coverage <  {thr:.2f}: {len(bad)}")

    print("\n--- Insufficient coverage (< threshold) ---")
    for ts, s in sorted(bad.items()):
        print(f"{ts} | cov={s['coverage_frac']:.4f} tiles={s['n_tiles']}")

    print("\n--- Sufficient coverage (≥ threshold) ---")
    for ts, s in sorted(good.items()):
        print(f"{ts} | cov={s['coverage_frac']:.4f} tiles={s['n_tiles']}")


if __name__ == "__main__":
    main()