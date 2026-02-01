from __future__ import annotations

from typing import Any, Dict
import requests


class CdseStacItemService:
  
    BASE_URL: str = "https://stac.dataspace.copernicus.eu/v1"

    def __init__(self) -> None:
        pass

    def fetch_item(self, collection: str, item_id: str) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/collections/{collection}/items/{item_id}"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return r.json()
