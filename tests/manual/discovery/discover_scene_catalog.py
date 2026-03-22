from __future__ import annotations

from pathlib import Path
from typing import Any

from thess_geo_analytics.core.params import StacQueryParams
from thess_geo_analytics.services.CdseSceneCatalogServiceNdvi import (
    CdseSceneCatalogServiceNdvi,
)

# Uncomment when ready
from thess_geo_analytics.services.CdseSceneCatalogServiceVvVh import (
     CdseSceneCatalogServiceVvVh,
)


def _item_id(item: Any) -> str | None:
    if hasattr(item, "id"):
        return getattr(item, "id", None)
    if isinstance(item, dict):
        return item.get("id")
    return None


def _item_properties(item: Any) -> dict:
    if hasattr(item, "properties"):
        return getattr(item, "properties", {}) or {}
    if isinstance(item, dict):
        return item.get("properties", {}) or {}
    return {}


def _print_rule(title: str) -> None:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def _print_props(props: dict, keys: list[str]) -> None:
    for key in keys:
        print(f"  - {key}: {props.get(key)}")


def inspect_service(
    *,
    service,
    label: str,
    collection: str,
    aoi_path: Path,
    date_start: str,
    date_end: str,
    cloud_cover_max: float | None,
    max_items: int = 5,
) -> None:
    _print_rule(f"DISCOVERY: {label}")

    params = StacQueryParams(
        collection=collection,
        cloud_cover_max=cloud_cover_max,
        max_items=max_items,
    )

    print(f"Service:      {service.__class__.__name__}")
    print(f"Collection:   {collection}")
    print(f"AOI path:     {aoi_path}")
    print(f"Date range:   {date_start} -> {date_end}")
    print(f"Cloud max:    {cloud_cover_max}")
    print(f"Max items:    {max_items}")

    query = service.build_query(params)
    print(f"Built query:  {query}")

    items, geom = service.search_items(
        aoi_geojson_path=aoi_path,
        date_start=date_start,
        date_end=date_end,
        params=params,
    )

    print(f"Items found:  {len(items)}")
    print(f"AOI geom:     {geom.get('type')}")

    if not items:
        print("[WARN] No items returned.")
        return

    first = items[0]
    props = _item_properties(first)

    print("\nFirst item:")
    print(f"  - id: {_item_id(first)}")
    print(f"  - property count: {len(props)}")

    print("\nSelected properties:")
    interesting_keys = [
        "datetime",
        "platform",
        "constellation",
        "eo:cloud_cover",
        "cloud_cover",
        "sar:polarizations",
        "sar:instrument_mode",
        "sat:orbit_state",
        "instruments",
    ]
    _print_props(props, interesting_keys)

    print("\nAll property keys:")
    for key in sorted(props.keys()):
        print(f"  - {key}")

    print("\nDataFrame from service.items_to_dataframe():")
    df = service.items_to_dataframe(items, collection=collection)

    print(f"Rows:    {len(df)}")
    print(f"Columns: {list(df.columns)}")

    if not df.empty:
        print("\nHead:")
        print(df.head(min(5, len(df))).to_string(index=False))

        print("\nDtypes:")
        print(df.dtypes.to_string())

        print("\nNull counts:")
        print(df.isna().sum().to_string())
    else:
        print("[WARN] DataFrame is empty.")

    print("\nDone.")


def main() -> None:
    aoi = Path("aoi/EL522_Thessaloniki.geojson")
    if not aoi.exists():
        raise FileNotFoundError(
            f"AOI file not found: {aoi}. Run from repo root or update the path."
        )

    inspect_service(
        service=CdseSceneCatalogServiceNdvi(),
        label="NDVI / Sentinel-2",
        collection="sentinel-2-l2a",
        aoi_path=aoi,
        date_start="2024-01-01",
        date_end="2024-01-31",
        cloud_cover_max=30.0,
        max_items=5,
    )

    # Uncomment when the VV/VH service exists
    inspect_service(
        service=CdseSceneCatalogServiceVvVh(),
        label="VV_VH / Sentinel-1",
        collection="sentinel-1-grd",
        aoi_path=aoi,
        date_start="2024-01-01",
        date_end="2024-01-31",
        cloud_cover_max=None,
        max_items=5,
    )


if __name__ == "__main__":
    main()