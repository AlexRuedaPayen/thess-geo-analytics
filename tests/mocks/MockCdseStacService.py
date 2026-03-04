from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class MockCdseStacService:
    """
    Offline STAC service replacement.

    It returns a STAC-like item JSON for any scene_id with assets
    that point to "mock://" hrefs.

    The downloader mock will understand those "mock://" hrefs and
    generate local GeoTIFFs.
    """

    # Optional: control which asset keys are produced
    band_resolution: int = 10

    def fetch_item(self, collection: str, item_id: str) -> Dict[str, Any]:
        # Provide the asset keys your StacAssetResolver expects:
        # - B04_10m or B04_20m
        # - B08_10m or B08_20m
        # - SCL_20m (usually)
        b_res = self.band_resolution
        scl_res = 20

        assets: Dict[str, Dict[str, Any]] = {
            f"B04_{b_res}m": {"href": f"mock://{item_id}/B04"},
            f"B08_{b_res}m": {"href": f"mock://{item_id}/B08"},
            f"SCL_{scl_res}m": {"href": f"mock://{item_id}/SCL"},
        }

        # Minimal STAC item
        return {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "collection": collection,
            "assets": assets,
            "properties": {},
            "geometry": None,
        }