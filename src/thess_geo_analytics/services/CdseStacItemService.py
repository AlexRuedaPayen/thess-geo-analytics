# src/thess_geo_analytics/services/CdseStacItemService.py
from __future__ import annotations
import time
import requests

STAC_BASE = "https://stac.dataspace.copernicus.eu/v1"

class CdseStacItemService:
    def fetch_item(self, collection: str, item_id: str) -> dict:
        url = f"{STAC_BASE}/collections/{collection}/items/{item_id}"
        last_exc = None

        for attempt in range(4):
            try:
                r = requests.get(url, timeout=60)
                if r.status_code == 500:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_exc = e
                time.sleep(1.0 * (attempt + 1))

        search_url = f"{STAC_BASE}/search"
        payload = {
            "collections": [collection],
            "ids": [item_id],
            "limit": 1,
        }
        r2 = requests.post(search_url, json=payload, timeout=60)
        r2.raise_for_status()
        feats = r2.json().get("features", [])
        if feats:
            return feats[0]

        # 3) Still nothing: raise the last error with context
        raise RuntimeError(f"STAC could not fetch item '{item_id}' from collection '{collection}'. Last error: {last_exc}")
