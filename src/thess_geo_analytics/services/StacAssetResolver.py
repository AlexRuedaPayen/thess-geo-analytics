from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence


class StacAssetResolver:
    """
    Resolve Sentinel-2 STAC assets for NDVI.

    Responsibilities:
      - Choose appropriate band resolution (10 m vs 20 m) for B04/B08/SCL.
      - Normalize hrefs to something downloadable by requests:
          * Prefer http(s) URLs
          * Check "alternate" hrefs
          * Rewrite s3://eodata/... -> https://eodata.dataspace.copernicus.eu/...

    Usage:
      resolver = StacAssetResolver(band_resolution=10 or 20)
      hrefs = resolver.resolve_b04_b08_scl(item_json)
      hrefs["href_b04"], hrefs["href_b08"], hrefs["href_scl"]
    """

    def __init__(self, band_resolution: int = 10) -> None:
        # target NDVI resolution (in metres)
        # deep mode: 10, dev mode: 20
        self.band_resolution = int(band_resolution)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _priority_keys(self, base: str, allowed_resolutions: Sequence[int]) -> List[str]:
        """
        Build an ordered list of asset keys like ["B04_20m", "B04_10m", "B04_60m"].

        The preferred resolution (self.band_resolution) is tried first,
        then the remaining allowed_resolutions in ascending order.
        """
        allowed = list(dict.fromkeys(int(r) for r in allowed_resolutions))  # dedupe

        prefs: List[int] = []
        if self.band_resolution in allowed:
            prefs.append(self.band_resolution)
        for r in sorted(allowed):
            if r not in prefs:
                prefs.append(r)

        return [f"{base}_{r}m" for r in prefs]

    def _select_download_href(self, asset: Dict[str, Any]) -> Optional[str]:
        """
        Given a single asset dict, return a download-friendly href:

          1) If asset["href"] is http(s) -> use it.
          2) Else, look in asset["alternate"] for http(s) href -> first match.
          3) Else, if href is s3://eodata/... -> rewrite to https://eodata.dataspace.copernicus.eu/...
          4) Else, return None (caller can handle missing href).

        This mirrors the old resolver behavior, but returns None instead
        of raising, so the manifest builder can continue and warn.
        """
        href = asset.get("href")

        # 1) direct http(s)
        if isinstance(href, str) and href.startswith(("http://", "https://")):
            return href

        # 2) alternates with http(s)
        alternates = asset.get("alternate", {})
        if isinstance(alternates, dict):
            for alt in alternates.values():
                if not isinstance(alt, dict):
                    continue
                alt_href = alt.get("href")
                if isinstance(alt_href, str) and alt_href.startswith(("http://", "https://")):
                    return alt_href

        # 3) s3://eodata/... -> https://eodata.dataspace.copernicus.eu/...
        if isinstance(href, str) and href.startswith("s3://eodata/"):
            # strip "s3://eodata/" and prepend HTTPS endpoint
            suffix = href[len("s3://eodata/") :]
            return f"https://eodata.dataspace.copernicus.eu/{suffix}"

        # 4) give up; caller decides what to do
        return None

    def _get_first_href(self, assets: Dict[str, Any], keys: Sequence[str]) -> Optional[str]:
        """
        Try each asset key in order, return the first usable download href,
        or None if nothing works.
        """
        for k in keys:
            asset = assets.get(k)
            if not isinstance(asset, dict):
                continue
            href = self._select_download_href(asset)
            if href is not None:
                return href
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def resolve_b04_b08_scl(self, item_json: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Resolve URLs for B04, B08, and SCL, respecting band_resolution.

        Returns a dict:
          {
            "href_b04": str | None,
            "href_b08": str | None,
            "href_scl": str | None,
          }

        In dev mode (20 m), this will *prefer* e.g. B04_20m, B08_20m, SCL_20m
        but will gracefully fall back to other available resolutions (10 or 60 m)
        if needed.
        """
        assets = item_json.get("assets", {}) or {}

        # B04/B08 can exist at 10m, 20m, sometimes 60m (resampled)
        b04_keys = self._priority_keys("B04", [10, 20, 60])
        b08_keys = self._priority_keys("B08", [10, 20, 60])

        # SCL is typically 20m (sometimes 60m)
        scl_keys = self._priority_keys("SCL", [20, 60])

        href_b04 = self._get_first_href(assets, b04_keys)
        href_b08 = self._get_first_href(assets, b08_keys)
        href_scl = self._get_first_href(assets, scl_keys)

        return {
            "href_b04": href_b04,
            "href_b08": href_b08,
            "href_scl": href_scl,
        }