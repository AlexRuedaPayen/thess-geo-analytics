from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from shapely.geometry import shape


UTC = timezone.utc



def load_yaml(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    return json.loads(p.read_text(encoding="utf-8"))


def load_geojson_geometry(path: str | Path):
    """
    Load a geometry from:
      - FeatureCollection
      - Feature
      - raw geometry object
    """
    obj = load_json(path)
    t = obj.get("type")

    if t == "FeatureCollection":
        return shape(obj["features"][0]["geometry"])
    if t == "Feature":
        return shape(obj["geometry"])
    return shape(obj)



def parse_iso_date(value: str | None, *, default: date | None = None) -> date:
    if value in (None, "", "null"):
        if default is None:
            raise ValueError("Missing date value and no default provided")
        return default
    return date.fromisoformat(value)


def to_utc_datetime_string(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat().replace("+00:00", "Z")


def ensure_utc_datetime(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)



def stable_unit_float(*values: Any, seed: int = 0) -> float:
    """
    Deterministic value in [0, 1], stable across runs and Python versions.

    Useful when you want reproducible synthetic variation without relying on
    Python's built-in hash randomization.
    """
    s = "|".join(str(v) for v in values)
    x = seed & 0xFFFFFFFF

    for ch in s.encode("utf-8"):
        x = ((x * 1664525) + ch + 1013904223) & 0xFFFFFFFF

    return x / 0xFFFFFFFF if x != 0xFFFFFFFF else 1.0


def stable_signed_float(*values: Any, seed: int = 0) -> float:
    """
    Deterministic value in [-1, 1].
    """
    u = stable_unit_float(*values, seed=seed)
    return 2.0 * u - 1.0


def get_item_id(item: Any) -> str | None:
    if hasattr(item, "id"):
        return getattr(item, "id", None)
    if isinstance(item, dict):
        return item.get("id")
    return None


def get_item_properties(item: Any) -> dict[str, Any]:
    if hasattr(item, "properties"):
        return getattr(item, "properties", {}) or {}
    if isinstance(item, dict):
        return item.get("properties", {}) or {}
    return {}


def get_item_property(item: Any, key: str, default: Any = None) -> Any:
    return get_item_properties(item).get(key, default)


def get_item_datetime(item: Any) -> datetime:
    props = get_item_properties(item)
    dt = props.get("datetime")

    if dt is None and hasattr(item, "datetime") and getattr(item, "datetime", None) is not None:
        raw_dt = getattr(item, "datetime")
        if isinstance(raw_dt, datetime):
            return ensure_utc_datetime(raw_dt)

    if isinstance(dt, datetime):
        return ensure_utc_datetime(dt)

    if isinstance(dt, str):
        return ensure_utc_datetime(datetime.fromisoformat(dt.replace("Z", "+00:00")))

    raise ValueError("Item has no valid datetime")


def write_json(obj: dict[str, Any], path: str | Path, *, indent: int = 2) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=indent), encoding="utf-8")
    return p