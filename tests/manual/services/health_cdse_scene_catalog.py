from __future__ import annotations

from pathlib import Path

from thess_geo_analytics.services.CdseSceneCatalogService import CdseSceneCatalogService
from thess_geo_analytics.core.params import StacQueryParams
from _common import print_header, print_kv, run_step, write_report, print_summary


def main() -> int:
    print_header("CDSE HEALTH — SCENE CATALOG SEARCH")

    results = []

    svc, r = run_step("Create CdseSceneCatalogService", lambda: CdseSceneCatalogService())
    results.append(r)
    if svc is None:
        write_report("health_cdse_scene_catalog", results)
        return 1

    # Pick an AOI file that exists in your repo
    aoi = Path("aoi/EL522_Thessaloniki.geojson")
    print_kv(aoi=str(aoi), aoi_exists=aoi.exists(), stac_url=getattr(svc, "stac_url", None))

    def _load_geom():
        return svc.load_aoi_geometry(aoi)
    geom, r = run_step("load_aoi_geometry()", _load_geom)
    results.append(r)

    def _search_items():
        return svc.search_items(
            aoi_geojson_path=aoi,
            date_start="2024-01-01",
            date_end="2024-01-31",
            params=StacQueryParams(
                cloud_cover_max=30.0,
                max_items=10,
            ),
        )

    out, r = run_step("search_items()", _search_items)
    results.append(r)

    items = []
    if out:
        items, _geom = out
        print_kv(items=len(items))

    def _to_df():
        return svc.items_to_dataframe(items)
    df, r = run_step("items_to_dataframe()", _to_df)
    results.append(r)

    if df is not None and not df.empty:
        print("\n[DF HEAD]")
        print(df.head(5).to_string(index=False))

    write_report("health_cdse_scene_catalog", results, extra={"items": len(items)})
    print_summary(results)
    return 0 if all(r.ok for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())