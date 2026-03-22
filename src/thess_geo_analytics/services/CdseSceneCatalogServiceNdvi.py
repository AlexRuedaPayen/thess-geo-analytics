from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION, VERBOSE
from thess_geo_analytics.services.BaseSceneCatalogService import BaseSceneCatalogService


class CdseSceneCatalogServiceNdvi(BaseSceneCatalogService):
    """
    Sentinel-2 / NDVI-oriented CDSE catalog service.
    """

    def build_query(self, params: StacQueryParams) -> Optional[Dict[str, Any]]:
        if VERBOSE:
            print("[INFO] Cloud cover <", params.cloud_cover_max)

        if params.cloud_cover_max is None:
            return None

        return {"eo:cloud_cover": {"lt": params.cloud_cover_max}}

    def items_to_dataframe(
        self,
        items: Sequence[Any],
        *,
        collection: str | None = None,
    ) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []

        for it in items:
            if hasattr(it, "properties"):
                props = it.properties or {}
                item_id = getattr(it, "id", None)
                dt = props.get("datetime")
                if not dt and getattr(it, "datetime", None):
                    dt = it.datetime.isoformat()

                rows.append(
                    {
                        "id": item_id,
                        "datetime": dt,
                        "cloud_cover": props.get("eo:cloud_cover", props.get("cloud_cover")),
                        "platform": props.get("platform"),
                        "constellation": props.get("constellation"),
                        "collection": collection or props.get("collection") or DEFAULT_COLLECTION,
                    }
                )
                continue

            props = it.get("properties", {}) if isinstance(it, dict) else {}
            rows.append(
                {
                    "id": it.get("id") if isinstance(it, dict) else None,
                    "datetime": props.get("datetime"),
                    "cloud_cover": props.get("eo:cloud_cover", props.get("cloud_cover")),
                    "platform": props.get("platform"),
                    "constellation": props.get("constellation"),
                    "collection": collection or props.get("collection") or DEFAULT_COLLECTION,
                }
            )

        df = pd.DataFrame(rows)

        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
            df = df.dropna(subset=["datetime"])
            df["cloud_cover"] = pd.to_numeric(df["cloud_cover"], errors="coerce")
            df = df.sort_values(["datetime", "cloud_cover"], ascending=[True, True]).reset_index(drop=True)

        return df