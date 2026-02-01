from __future__ import annotations
from typing import Any, Dict, Optional


class StacAssetResolver:


    def __init__(self) -> None:
        pass

    def _href(self, assets: Dict[str, Any], key: str) -> Optional[str]:
        obj = assets.get(key)
        if isinstance(obj, dict):
            return obj.get("href")
        return None

    def resolve_b04_b08_scl(self, item_json: Dict[str, Any]) -> Dict[str, Optional[str]]:
        assets = item_json.get("assets", {}) or {}

        href_b04 = (
            self._href(assets, "B04_10m")
            or self._href(assets, "B04_20m")
            or self._href(assets, "B04_60m")
        )

        href_b08 = (
            self._href(assets, "B08_10m")
            or self._href(assets, "B08_20m")
            or self._href(assets, "B08_60m")
        )

        # SCL usually at 20m; fall back to 60m if needed
        href_scl = self._href(assets, "SCL_20m") or self._href(assets, "SCL_60m")

        return {"href_b04": href_b04, "href_b08": href_b08, "href_scl": href_scl}
