from __future__ import annotations

from typing import Any, Dict, List, Optional

from thess_geo_analytics.core.constants import CDSE_STAC_BASE_URL
from thess_geo_analytics.core.HttpClient import HttpClient, HttpConfig


class CdseStacService:
    """
    CDSE STAC (v1) low-level service.

    Supports:
      - POST /search
      - GET  /collections/{collection}/items/{id}
      - fetch_item(): robust get_item with fallback to /search by ids

    Notes:
      - CDSE has multiple STAC endpoints.
      - This service targets CDSE_STAC_BASE_URL (v1) and is best for item fetch.
      - For catalogue-style searching, you may prefer pystac_client against
        https://catalogue.dataspace.copernicus.eu/stac in your SceneCatalog service.
    """

    def __init__(self, base_url: str = CDSE_STAC_BASE_URL, http: HttpClient | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http or HttpClient(HttpConfig())

    # --------------------------------------------------------------
    # Search
    # --------------------------------------------------------------
    def search(
        self,
        *,
        collections: List[str],
        datetime_range: str,
        bbox: Optional[List[float]] = None,
        intersects: Optional[Dict[str, Any]] = None,
        query: Optional[dict] = None,
        ids: Optional[List[str]] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/search"

        payload: Dict[str, Any] = {
            "collections": collections,
            "datetime": datetime_range,
            "limit": min(int(limit), 100),
        }
        if bbox is not None:
            payload["bbox"] = bbox
        if intersects is not None:
            payload["intersects"] = intersects
        if query is not None:
            payload["query"] = query
        if ids is not None:
            payload["ids"] = ids

        r = self.http.post(url, json=payload)
        data = r.json()
        return data.get("features", [])

    # --------------------------------------------------------------
    # Item fetch (direct)
    # --------------------------------------------------------------
    def get_item(self, collection: str, item_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/collections/{collection}/items/{item_id}"
        r = self.http.get(url)
        return r.json()

    # --------------------------------------------------------------
    # Item fetch (robust)
    # --------------------------------------------------------------
    def fetch_item(self, collection: str, item_id: str) -> Dict[str, Any]:
        """
        Reliable item fetch:
          1) try direct GET item endpoint
          2) fallback to POST /search with ids=[item_id]
        """
        try:
            return self.get_item(collection, item_id)
        except Exception as e:
            feats = self.search(
                collections=[collection],
                datetime_range="1900-01-01T00:00:00Z/2100-01-01T00:00:00Z",
                ids=[item_id],
                limit=1,
            )
            if feats:
                return feats[0]
            raise RuntimeError(
                f"STAC could not fetch item '{item_id}' from collection '{collection}'. "
                f"Direct get failed; fallback search returned 0. Last error: {e}"
            )

    # --------------------------------------------------------------
    # Smoke test
    # --------------------------------------------------------------
    @staticmethod
    def smoke_test() -> None:
        """
        Smoke test strategy:
        - Use pystac_client against catalogue endpoint for search (most reliable)
        - Then use this service to fetch one returned item (tests get_item/fetch_item)
        """
        print("=== CdseStacService Smoke Test (merged) ===")

        # 1) Search with catalogue endpoint (reliable)
        from pystac_client import Client

        cat_url = "https://catalogue.dataspace.copernicus.eu/stac"
        client = Client.open(cat_url)

        search = client.search(
            collections=["sentinel-2-l2a"],
            datetime="2026-01-01T00:00:00Z/2026-01-31T23:59:59Z",
            bbox=[23.0, 40.3, 24.0, 41.0],
            query={"eo:cloud_cover": {"lt": 20}},
            max_items=1,
        )
        items = list(search.items())

        if not items:
            raise RuntimeError("No items returned from catalogue search; cannot smoke test item fetch.")

        item_id = items[0].id
        print("[OK] catalogue returned item:", item_id)

        # 2) Fetch with v1 endpoint (tests our merged service)
        svc = CdseStacService()
        item = svc.fetch_item("sentinel-2-l2a", item_id)

        print("[OK] fetched item id:", item.get("id"))
        assets = item.get("assets") or {}
        print("[OK] asset keys sample:", list(assets.keys())[:12])

        print("✓ Smoke test OK")


if __name__ == "__main__":
    CdseStacService.smoke_test()
