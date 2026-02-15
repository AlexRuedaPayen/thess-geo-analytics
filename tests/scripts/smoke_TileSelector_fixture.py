# tests/scripts/smoke_TileSelector_fixture.py
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from shapely.geometry import shape

from thess_geo_analytics.geo.TileSelector import TileSelector
from thess_geo_analytics.services.CdseSceneCatalogService import CdseSceneCatalogService  # only for AOI loader


def _require_cols(df: pd.DataFrame, cols: List[str]) -> None:
    missing = set(cols) - set(df.columns)
    if missing:
        raise SystemExit(f"[ERR] Fixture missing columns: {missing}. Found: {list(df.columns)}")


def _fixture_to_itemlikes(df: pd.DataFrame) -> List[Dict[str, Any]]:
    # Produce dict-like items compatible with TileSelector (id, geometry, properties.datetime/cloud_cover)
    items: List[Dict[str, Any]] = []

    for _, r in df.iterrows():
        geom_str = r["geometry"]
        if pd.isna(geom_str) or not str(geom_str).strip():
            continue

        try:
            geom = json.loads(geom_str)
        except Exception:
            continue

        dt = r["datetime"]
        if pd.isna(dt):
            continue
        if hasattr(dt, "isoformat"):
            dt_str = dt.isoformat().replace("+00:00", "Z")
        else:
            dt_str = str(dt)

        cloud = r.get("cloud_cover", None)
        try:
            cloud = float(cloud) if pd.notna(cloud) else None
        except Exception:
            cloud = None

        items.append(
            {
                "id": str(r["id"]),
                "geometry": geom,
                "properties": {
                    "datetime": dt_str,
                    "cloud_cover": cloud,
                    "eo:cloud_cover": cloud,
                },
            }
        )

    return items


def main() -> None:
    ap = argparse.ArgumentParser(description="Smoke test TileSelector using a thin-but-geo fixture CSV.")
    ap.add_argument("--fixture", required=True, help="Fixture CSV path")
    ap.add_argument("--aoi", required=True, help="AOI GeoJSON path")
    ap.add_argument("--start", required=True, help="Period start YYYY-MM-DD (anchors grid)")
    ap.add_argument("--end", required=True, help="Period end YYYY-MM-DD (anchors grid)")
    ap.add_argument("--n", type=int, required=True, help="Number of anchors (regular time series points)")
    ap.add_argument("--window", type=int, default=21, help="Window days around anchor (default 21)")
    ap.add_argument("--full", type=float, default=0.999, help="Full cover threshold (default 0.999)")
    ap.add_argument("--max-union", type=int, default=2, help="Max tiles in union (default 2)")
    args = ap.parse_args()

    fixture_path = Path(args.fixture)
    aoi_path = Path(args.aoi)

    print("=== smoke_TileSelector_fixture ===")
    print(f"[1/6] Fixture: {fixture_path}")
    print(f"[1/6] AOI:     {aoi_path}")
    print(f"[1/6] Anchor period: {args.start} .. {args.end}  (n={args.n})")
    print(f"[1/6] Window: ±{args.window//2} days (window_days={args.window})")
    print(f"[1/6] Union: max_union_tiles={args.max_union}, full_cover_threshold={args.full}")

    print("[2/6] Loading fixture...")
    df = pd.read_csv(fixture_path)
    _require_cols(df, ["id", "datetime", "cloud_cover", "geometry"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    df = df.dropna(subset=["datetime"]).reset_index(drop=True)

    print(f"[2/6] Rows: {len(df)}")
    if len(df) > 0:
        print(f"[2/6] Datetime range: {df['datetime'].min()} .. {df['datetime'].max()}")

    print("[3/6] Loading AOI geometry (via src service loader)...")
    svc = CdseSceneCatalogService()
    aoi_geom_geojson = svc.load_aoi_geometry(aoi_path)
    aoi_shp = shape(aoi_geom_geojson)
    print(f"[3/6] AOI type: {aoi_shp.geom_type}, bounds: {aoi_shp.bounds}")

    print("[4/6] Converting fixture -> ItemLike dicts for TileSelector...")
    items = _fixture_to_itemlikes(df)
    print(f"[4/6] ItemLikes: {len(items)}")

    print("[5/6] Running TileSelector.select_regular_time_series ...")
    selector = TileSelector(
        full_cover_threshold=args.full,
        allow_union=True,
        max_union_tiles=args.max_union,
    )

    selected = selector.select_regular_time_series(
        items=items,
        aoi_geom_4326=aoi_shp,
        period_start=date.fromisoformat(args.start),
        period_end=date.fromisoformat(args.end),
        n_anchors=args.n,
        window_days=args.window,
    )

    print(f"[5/6] SelectedScene count: {len(selected)}")

    print("[6/6] Results per anchor:")
    for s in selected:
        ids = []
        for it in s.items:
            # it can be dict-like or pystac.Item; handle both
            if hasattr(it, "id"):
                ids.append(it.id)
            else:
                ids.append(it.get("id"))
        print(
            f"  anchor={s.anchor_date} -> acq={s.acq_dt.isoformat()} "
            f"tiles={len(ids)} ids={ids} cloud_score={s.cloud_score:.2f} cov={s.coverage_frac:.3f}"
        )

    # quick sanity prints
    if selected:
        covs = [s.coverage_frac for s in selected]
        clouds = [s.cloud_score for s in selected]
        print(f"[OK] Coverage frac: min={min(covs):.3f} median={pd.Series(covs).median():.3f} max={max(covs):.3f}")
        print(f"[OK] Cloud score:   min={min(clouds):.2f} median={pd.Series(clouds).median():.2f} max={max(clouds):.2f}")
    else:
        print("[WARN] No anchors produced selections (window too small, catalog too sparse, or geometry mismatch).")


if __name__ == "__main__":
    main()
