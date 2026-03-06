from __future__ import annotations

from thess_geo_analytics.services.CdseStacService import CdseStacService
from _common import print_header, print_kv, run_step, write_report, print_summary


def main() -> int:
    print_header("CDSE HEALTH — STAC ITEM FETCH")

    results = []

    def _catalogue_search_one():
        from pystac_client import Client
        client = Client.open("https://catalogue.dataspace.copernicus.eu/stac")
        search = client.search(
            collections=["sentinel-2-l2a"],
            datetime="2024-01-01T00:00:00Z/2024-01-31T23:59:59Z",
            bbox=[23.0, 40.3, 24.0, 41.0],
            query={"eo:cloud_cover": {"lt": 30}},
            max_items=1,
        )
        items = list(search.items())
        if not items:
            raise RuntimeError("Catalogue search returned 0 items")
        return items[0].id

    item_id, r = run_step("Catalogue search (pystac_client) → get item id", _catalogue_search_one)
    results.append(r)

    svc, r = run_step("Create CdseStacService", lambda: CdseStacService())
    results.append(r)
    if svc is None or item_id is None:
        write_report("health_cdse_stac_item", results)
        return 1

    print_kv(item_id=item_id, base_url=getattr(svc, "base_url", None))

    def _fetch():
        return svc.fetch_item("sentinel-2-l2a", item_id)

    item, r = run_step("fetch_item()", _fetch)
    results.append(r)

    if item:
        assets = item.get("assets") or {}
        print_kv(fetched_id=item.get("id"), n_assets=len(assets), assets_sample=list(assets.keys())[:12])

    write_report("health_cdse_stac_item", results)
    print_summary(results)
    return 0 if all(r.ok for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())