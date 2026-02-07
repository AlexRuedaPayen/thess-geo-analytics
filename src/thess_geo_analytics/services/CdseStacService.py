from __future__ import annotations
import requests
from typing import Any, Dict, List, Optional

STAC_BASE = "https://stac.dataspace.copernicus.eu/v1"

class CdseStacService:
    def search(
        self,
        bbox: list[float],
        datetime_range: str,   # "YYYY-MM-DDT00:00:00Z/YYYY-MM-DDT00:00:00Z"
        collections: list[str] = ["sentinel-2-l2a"],
        max_items: int = 100,
        query: Optional[dict] = None,
    ) -> List[Dict[str, Any]]:
        url = f"{STAC_BASE}/search"
        payload: Dict[str, Any] = {
            "collections": collections,
            "bbox": bbox,
            "datetime": datetime_range,
            "limit": min(max_items, 100),
        }
        if query:
            payload["query"] = query

        r = requests.post(url, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        return data.get("features", [])

    def get_item(self, collection: str, item_id: str) -> Dict[str, Any]:
        url = f"{STAC_BASE}/collections/{collection}/items/{item_id}"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return r.json()
