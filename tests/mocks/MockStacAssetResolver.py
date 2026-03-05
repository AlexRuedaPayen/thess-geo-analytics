from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class MockStacAssetResolver:
    """
    Resolver for offline tests.

    - Accepts mock:// hrefs (or any scheme)
    - Looks up common keys your mock STAC might provide
    - Does NOT rewrite URLs or enforce http(s)
    """

    band_resolution: int = 10  # 10 or 20

    def _get_href(self, assets: Dict[str, Any], key: str) -> Optional[str]:
        a = assets.get(key)
        if isinstance(a, dict):
            href = a.get("href")
            if isinstance(href, str) and href:
                return href
        return None

    def resolve_b04_b08_scl(self, item_json: Dict[str, Any]) -> Dict[str, Optional[str]]:
        assets = item_json.get("assets") or {}

        b = int(self.band_resolution)

        # Prefer resolution-specific keys, but allow plain keys as fallback.
        href_b04 = (
            self._get_href(assets, f"B04_{b}m")
            or self._get_href(assets, "B04")
            or self._get_href(assets, f"b04_{b}m")
            or self._get_href(assets, "b04")
        )

        href_b08 = (
            self._get_href(assets, f"B08_{b}m")
            or self._get_href(assets, "B08")
            or self._get_href(assets, f"b08_{b}m")
            or self._get_href(assets, "b08")
        )

        # SCL usually 20m; still allow plain key fallback
        href_scl = (
            self._get_href(assets, "SCL_20m")
            or self._get_href(assets, "SCL")
            or self._get_href(assets, "scl_20m")
            or self._get_href(assets, "scl")
        )

        return {"href_b04": href_b04, "href_b08": href_b08, "href_scl": href_scl}