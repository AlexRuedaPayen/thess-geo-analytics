from __future__ import annotations

import json
import time
from pathlib import Path

from pystac_client import Client

from _common import (
    print_header,
    run_step,
    write_report,
    print_summary,
)
from DebugStacApiIO import DebugStacApiIO


class HealthCdseSceneCatalogDeep:
    def __init__(self) -> None:
        self.results = []

    def main(self) -> None:
        print_header("CDSE Scene Catalog (deep)")

        aoi_path = Path("aoi/EL522_Thessaloniki.geojson")

        client, res = run_step(
            "Client.open() with DebugStacApiIO",
            self._open_client,
        )
        self.results.append(res)
        if client is None:
            write_report("health_cdse_scene_catalog_deep", self.results)
            print_summary(self.results)
            return

        geom, res = run_step(
            "Load AOI geometry",
            lambda: self._load_geom(aoi_path),
        )
        self.results.append(res)
        if geom is None:
            write_report("health_cdse_scene_catalog_deep", self.results)
            print_summary(self.results)
            return

        out, res = run_step(
            "search.items() deep iteration",
            lambda: self._search_with_retry(client, geom),
        )
        self.results.append(res)

        extra = out if isinstance(out, dict) else {}
        write_report("health_cdse_scene_catalog_deep", self.results, extra=extra)
        print_summary(self.results)

    def _open_client(self) -> Client:
        io = DebugStacApiIO()
        return Client.open(
            "https://catalogue.dataspace.copernicus.eu/stac",
            stac_io=io,
        )

    def _load_geom(self, aoi_path: Path) -> dict:
        obj = json.loads(aoi_path.read_text(encoding="utf-8"))

        if obj.get("type") == "FeatureCollection":
            return obj["features"][0]["geometry"]
        if obj.get("type") == "Feature":
            return obj["geometry"]
        return obj

    def _search_with_retry(self, client: Client, geom: dict) -> dict:
        last_exc: Exception | None = None

        for attempt in range(3):
            try:
                search = client.search(
                    collections=["sentinel-2-l2a"],
                    intersects=geom,
                    datetime="2021-01-01T00:00:00Z/2021-02-01T00:00:00Z",
                    query={"eo:cloud_cover": {"lt": 80}},
                    limit=50,
                    max_items=200,
                )

                items = list(search.items())

                return {
                    "n_items": len(items),
                    "first_id": items[0].id if items else None,
                    "last_id": items[-1].id if items else None,
                }

            except Exception as e:
                last_exc = e
                print(f"[RETRY] search.items() failed ({attempt + 1}/3): {type(e).__name__}: {e}")
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))

        assert last_exc is not None
        raise last_exc


def main() -> None:
    HealthCdseSceneCatalogDeep().main()


if __name__ == "__main__":
    main()
    
    
    
