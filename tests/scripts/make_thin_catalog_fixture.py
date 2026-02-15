# tests/scripts/make_thin_catalog_fixture.py
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION
from thess_geo_analytics.services.CdseSceneCatalogService import CdseSceneCatalogService


THIN_COLS = [
    "id",
    "datetime",
    "acq_date",
    "cloud_cover",
    "tile_id",
    "platform",
    "constellation",
    "collection",
    "geometry",  # JSON string (required by TileSelector)
]


def _tile_id_from_scene_id(scene_id: str) -> Optional[str]:
    # Sentinel-2 naming: ..._T34TGL_... => tile_id = "34TGL"
    try:
        parts = scene_id.split("_")
        t = next(p for p in parts if p.startswith("T") and len(p) >= 6)
        return t[1:6]
    except Exception:
        return None


def _items_to_thin_rows(items: List[Any], collection: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        props = getattr(it, "properties", {}) or {}
        dt = props.get("datetime")
        if dt is None and getattr(it, "datetime", None) is not None:
            dt = it.datetime.isoformat()

        # keep dt parseable + normalized later
        scene_id = getattr(it, "id", None) or ""
        cloud = props.get("eo:cloud_cover", props.get("cloud_cover", None))

        geom = getattr(it, "geometry", None)
        if not geom:
            # If geometry missing, skip (TileSelector can't work without it)
            continue

        rows.append(
            {
                "id": scene_id,
                "datetime": dt,
                "acq_date": None,  # filled after datetime parse
                "cloud_cover": cloud,
                "tile_id": _tile_id_from_scene_id(scene_id),
                "platform": props.get("platform"),
                "constellation": props.get("constellation"),
                "collection": collection,
                "geometry": json.dumps(geom),
            }
        )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Create a thin-but-geo STAC fixture for TileSelector smoke tests.")
    ap.add_argument("--aoi", required=True, help="AOI GeoJSON path")
    ap.add_argument("--out", required=True, help="Output CSV path (e.g. tests/fixtures/scenes_catalog_thin.csv)")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--cloud", type=float, default=20.0, help="Max cloud cover (default 20)")
    ap.add_argument("--max-items", type=int, default=5000, help="Max items from STAC (default 5000)")
    ap.add_argument("--collection", default=DEFAULT_COLLECTION, help=f"STAC collection (default {DEFAULT_COLLECTION})")
    args = ap.parse_args()

    aoi_path = Path(args.aoi)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("=== make_thin_catalog_fixture ===")
    print(f"[1/4] AOI: {aoi_path}")
    print(f"[1/4] Period: {args.start} .. {args.end}")
    print(f"[1/4] Filters: cloud<{args.cloud}, max_items={args.max_items}, collection={args.collection}")
    print(f"[1/4] Output: {out_path}")

    svc = CdseSceneCatalogService()
    params = StacQueryParams(
        collection=args.collection,
        cloud_cover_max=args.cloud,
        max_items=args.max_items,
    )

    print("[2/4] Querying STAC (items with geometry)...")
    items, _aoi_geom = svc.search_items(
        aoi_geojson_path=aoi_path,
        date_start=args.start,
        date_end=args.end,
        params=params,
    )
    print(f"[2/4] Items received: {len(items)}")

    print("[3/4] Building thin rows + normalizing datetime...")
    rows = _items_to_thin_rows(items, collection=args.collection)
    df = pd.DataFrame(rows, columns=THIN_COLS)

    if df.empty:
        df.to_csv(out_path, index=False)
        print(f"[OK] Wrote EMPTY fixture => {out_path}")
        return

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    df = df.dropna(subset=["datetime"]).reset_index(drop=True)
    df["acq_date"] = df["datetime"].dt.date.astype(str)
    df["cloud_cover"] = pd.to_numeric(df["cloud_cover"], errors="coerce")
    df = df.sort_values(["datetime", "cloud_cover"], ascending=[True, True]).reset_index(drop=True)

    df.to_csv(out_path, index=False)

    print("[4/4] Done.")
    print(f"[OK] Fixture rows: {len(df)}")
    print(f"[OK] Unique dates: {df['acq_date'].nunique()}")
    print(f"[OK] Columns: {list(df.columns)}")
    print(f"[OK] Wrote => {out_path}")


if __name__ == "__main__":
    main()
