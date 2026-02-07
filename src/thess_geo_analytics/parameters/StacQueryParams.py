from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class StacQueryParams:
    collection: str = "sentinel-2-l2a"
    cloud_cover_max: float = 20.0
    max_items: int = 200
