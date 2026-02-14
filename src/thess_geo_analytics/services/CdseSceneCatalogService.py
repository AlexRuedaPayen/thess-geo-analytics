from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from pystac_client import Client

from thess_geo_analytics.core.constants import CDSE_CATALOG_STAC_URL
from thess_geo_analytics.core.settings import DEFAULT_COLLECTION, VERBOSE
from thess_geo_analytics.core.params import StacQueryParams



class CdseSceneCatalogService:
    """
    Service to search Sentinel scenes via CDSE STAC catalogue and return:
      - raw STAC items (for geometry-aware processing like TileSelector)
      - tabular scene catalog (DataFrame) for downstream pipeline artifacts

    Owns:
      - reading AOI geometry from GeoJSON
      - querying STAC catalogue
      - converting STAC items -> DataFrame (id/datetime/cloud/etc.)

    Does NOT own:
      - tile selection strategy
      - downloading assets
      - writing outputs to disk
      - pipeline orchestration
    """

    def __init__(self, stac_url: str = CDSE_CATALOG_STAC_URL) -> None:
        self.stac_url = stac_url

    # ------------------------------------------------------------------
    # AOI geometry loader
    # ------------------------------------------------------------------
    def load_aoi_geometry(self, aoi_geojson_path: str | Path) -> Dict[str, Any]:
        """
        Loads AOI geometry from a GeoJSON file.
        Returns a GeoJSON geometry dict (EPSG:4326 expected).
        """
        p = Path(aoi_geojson_path)
        if not p.exists():
            raise FileNotFoundError(f"AOI file not found: {p}")

        with p.open("r", encoding="utf-8") as f:
            obj = json.load(f)

        geojson_type = obj.get("type")

        # Feature
        if geojson_type == "Feature":
            geom = obj.get("geometry")
            if not geom:
                raise ValueError(f"AOI Feature has no geometry: {p}")
            return geom

        # FeatureCollection
        if geojson_type == "FeatureCollection":
            features = obj.get("features", [])
            if not features:
                raise ValueError(f"AOI FeatureCollection has no features: {p}")
            geom = features[0].get("geometry")
            if not geom:
                raise ValueError(f"First feature has no geometry: {p}")
            return geom

        # Raw geometry object
        if "type" in obj and "coordinates" in obj:
            return obj

        raise ValueError(f"Unsupported GeoJSON type in {p}: {geojson_type}")

    # ------------------------------------------------------------------
    # STAC query (raw items)
    # ------------------------------------------------------------------
    def search_items(
        self,
        aoi_geojson_path: str | Path,
        date_start: str,
        date_end: str,
        params: Optional[StacQueryParams] = None,
    ) -> Tuple[List[Any], Dict[str, Any]]:
        """
        Queries CDSE STAC and returns:
          (items, aoi_geometry_geojson)

        Items are pystac.Item objects (from pystac_client search.items()).
        """
        p = params or StacQueryParams(collection=DEFAULT_COLLECTION)

        geom = self.load_aoi_geometry(aoi_geojson_path)

        if VERBOSE:
            print("[INFO] STAC URL:", self.stac_url)
            print("[INFO] Collection:", p.collection)
            print("[INFO] Date range:", f"{date_start}/{date_end}")
            print("[INFO] Cloud cover <", p.cloud_cover_max)
            print("[INFO] Max items:", p.max_items)

        client = Client.open(self.stac_url)

        query = (
            {"eo:cloud_cover": {"lt": p.cloud_cover_max}}
            if p.cloud_cover_max is not None
            else None
        )

        search = client.search(
            collections=[p.collection],
            intersects=geom,
            datetime=f"{date_start}/{date_end}",
            query=query,
            max_items=p.max_items,
        )

        items = list(search.items())
        return items, geom

    # ------------------------------------------------------------------
    # STAC items -> DataFrame
    # ------------------------------------------------------------------
    def items_to_dataframe(
        self,
        items: Sequence[Any],
        *,
        collection: str | None = None,
    ) -> pd.DataFrame:
        """
        Converts pystac Items (or dict-like items) into a standard catalog DataFrame.
        Expected output columns:
          id, datetime, cloud_cover, platform, constellation, collection
        """
        rows: List[Dict[str, Any]] = []

        for it in items:
            # Support pystac.Item (has .properties, .id, .datetime)
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

            # Support dict-like STAC-ish objects
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
            # Ensure numeric cloud_cover for sorting
            df["cloud_cover"] = pd.to_numeric(df["cloud_cover"], errors="coerce")
            df = df.sort_values(["datetime", "cloud_cover"], ascending=[True, True]).reset_index(drop=True)

        return df

    # ------------------------------------------------------------------
    # Backward-compatible API (DataFrame only)
    # ------------------------------------------------------------------
    def search_scenes(
        self,
        aoi_geojson_path: str | Path,
        date_start: str,
        date_end: str,
        params: Optional[StacQueryParams] = None,
    ) -> pd.DataFrame:
        """
        Backward-compatible method: queries items then converts to DataFrame.
        """
        p = params or StacQueryParams(collection=DEFAULT_COLLECTION)
        items, _ = self.search_items(
            aoi_geojson_path=aoi_geojson_path,
            date_start=date_start,
            date_end=date_end,
            params=p,
        )
        return self.items_to_dataframe(items, collection=p.collection)

    # ------------------------------------------------------------------
    # Rapid smoke test
    # ------------------------------------------------------------------
    @staticmethod
    def smoke_test() -> None:
        """
        Prerequisite:
          - file aoi/EL522_Thessaloniki.geojson exists
        """
        print("=== CdseSceneCatalogService Smoke Test ===")

        aoi = Path("aoi/EL522_Thessaloniki.geojson")
        if not aoi.exists():
            print("!! AOI file missing:", aoi)
            return

        svc = CdseSceneCatalogService()

        items, geom = svc.search_items(
            aoi_geojson_path=aoi,
            date_start="2026-01-01",
            date_end="2026-01-31",
            params=StacQueryParams(cloud_cover_max=30.0, max_items=10),
        )

        df = svc.items_to_dataframe(items, collection=DEFAULT_COLLECTION)

        print("Items found:", len(items))
        print("DF rows:", len(df))
        if not df.empty:
            print(df.head(5)[["id", "datetime", "cloud_cover"]])

        print("✓ Smoke test complete")


# --------------------------------------------------------------
# Run smoke test when directly executed
# --------------------------------------------------------------
if __name__ == "__main__":
    CdseSceneCatalogService.smoke_test()