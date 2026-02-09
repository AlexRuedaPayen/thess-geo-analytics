from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from thess_geo_analytics.core.settings import DEFAULT_COLLECTION

@dataclass(frozen=True)
class StacQueryParams:
    collection: str = DEFAULT_COLLECTION
    cloud_cover_max: float = 20.0
    max_items: int = 200

    date_from: Optional[str] = None  # "YYYY-MM-DD"
    date_to: Optional[str] = None    # "YYYY-MM-DD"
