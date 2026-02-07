from __future__ import annotations
from typing import Any, Dict

def _select_download_href(asset: Dict[str, Any]) -> str:
    href = asset.get("href")
    if isinstance(href, str) and href.startswith(("http://", "https://")):
        return href

    alternates = asset.get("alternate", {})
    if isinstance(alternates, dict):
        for _, alt in alternates.items():
            alt_href = alt.get("href")
            if isinstance(alt_href, str) and alt_href.startswith(("http://", "https://")):
                return alt_href

    if isinstance(href, str) and href.startswith("s3://eodata/"):
        return "https://eodata.dataspace.copernicus.eu/" + href[len("s3://eodata/") :]

    raise ValueError(f"No downloadable href found. href={href!r}")

class StacAssetResolver:
    def resolve_b04_b08_scl(self, item_json: dict) -> dict:
        assets = item_json.get("assets", {})

        def get_first(keys: list[str]) -> str:
            for k in keys:
                if k in assets:
                    return _select_download_href(assets[k])
            raise KeyError(f"None of keys {keys} found. Available: {list(assets.keys())[:60]}")

        # CDSE S2 L2A keys typically include resolution suffixes
        href_b04 = get_first(["B04_10m", "B04_20m", "B04_60m"])
        href_b08 = get_first(["B08_10m", "B08_20m", "B08_60m"])
        href_scl = get_first(["SCL_20m", "SCL_60m"])  # SCL is usually 20m

        return {"href_b04": href_b04, "href_b08": href_b08, "href_scl": href_scl}
