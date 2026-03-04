from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from shapely.geometry import shape, mapping

from thess_geo_analytics.core.params import StacQueryParams


class MockCdseSceneCatalogService:
    """
    Offline replacement for CdseSceneCatalogService.

    Goals:
      - deterministic
      - NEVER hits network
      - footprints fully cover AOI.buffer(0.05) used by pipeline
      - supports BOTH:
          (A) legacy ctor: n_timestamps/step_days/buffer_deg
          (B) realistic mode: generate ~params.max_items scenes, spread across [date_start..today]
      - creates multiple tiles per acquisition datetime (timestamp)
      - spreads timestamps evenly across the whole period (NOT only near the start)
    """

    def __init__(
        self,
        *,
        # Legacy knobs (optional)
        n_timestamps: int | None = None,
        step_days: int | None = None,
        buffer_deg: float = 0.05,
        # Optional: override tiles per timestamp
        tiles_per_timestamp: int | None = None,
    ) -> None:
        self.n_timestamps = n_timestamps
        self.step_days = step_days
        self.buffer_deg = float(buffer_deg)
        self.tiles_per_timestamp = tiles_per_timestamp

    # ---------------------------------------------------------
    # Helper
    # ---------------------------------------------------------
    def _load_aoi_geom(self, path: Path) -> Dict[str, Any]:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)

        t = obj.get("type")
        if t == "FeatureCollection":
            return obj["features"][0]["geometry"]
        if t == "Feature":
            return obj["geometry"]
        return obj

    # ---------------------------------------------------------
    # STAC-like search
    # ---------------------------------------------------------
    def search_items(
        self,
        aoi_geojson_path: Path,
        date_start: str,
        date_end: str,  # signature compatibility; intentionally ignored (pipeline end is "today")
        params: StacQueryParams,
    ) -> Tuple[List[Any], Dict[str, Any]]:

        # Read params like the real service would
        max_items = int(getattr(params, "max_items", 100) or 100)
        cloud_max = getattr(params, "cloud_cover_max", None)
        cloud_max = float(cloud_max) if cloud_max is not None else None

        # Load AOI + build a footprint that fully covers AOI.buffer(0.05)
        aoi_geom = self._load_aoi_geom(aoi_geojson_path)
        aoi = shape(aoi_geom)
        footprint = mapping(aoi.buffer(self.buffer_deg * 2))

        # Build time range: [start .. today UTC 00:00]
        start = datetime.fromisoformat(date_start).replace(tzinfo=timezone.utc)
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        # Guard: if start is in the future, just clamp to today
        if start > today:
            start = today

        total_days = max(1, (today - start).days)

        # ---------------------------------------------------------
        # Decide how many acquisition timestamps to generate
        # ---------------------------------------------------------
        if self.n_timestamps is not None:
            n_timestamps = max(1, int(self.n_timestamps))
        else:
            # Good default: enough timestamps so that max_items has decent temporal spread
            # Example: max_items=300 -> sqrt=17 -> at least 32 timestamps gives a nice spread.
            n_timestamps = max(32, int(math.sqrt(max_items) * 2))

        # Cap timestamps to available days (+1 allows same-day multiple timestamps if needed)
        n_timestamps = min(n_timestamps, total_days + 1)

        # ---------------------------------------------------------
        # Decide tiles per timestamp
        # ---------------------------------------------------------
        if self.tiles_per_timestamp is not None:
            tiles_per_ts = max(1, int(self.tiles_per_timestamp))
        else:
            tiles_per_ts = max(1, math.ceil(max_items / n_timestamps))

        # If legacy step_days given, we’ll use it to build timestamps, otherwise evenly spaced.
        timestamps: List[datetime] = []

        if self.step_days is not None:
            step = max(1, int(self.step_days))
            dt = start
            for _ in range(n_timestamps):
                if dt > today:
                    break
                timestamps.append(dt)
                dt = dt + timedelta(days=step)
        else:
            # Evenly space timestamps across [start..today]
            for i in range(n_timestamps):
                frac = i / max(1, (n_timestamps - 1))
                offset_days = int(frac * total_days)
                ts = start + timedelta(days=offset_days)

                # optional tiny deterministic jitter (keeps in-range)
                # this makes anchor/window behavior more realistic
                jitter = (i % 3)  # 0,1,2 repeating
                ts = min(ts + timedelta(days=jitter), today)

                timestamps.append(ts)

            # ensure non-decreasing order
            timestamps = sorted(timestamps)

        # ---------------------------------------------------------
        # Generate items: multiple tiles per timestamp until max_items
        # ---------------------------------------------------------
        items: List[Dict[str, Any]] = []

        for t, dt in enumerate(timestamps):
            for k in range(tiles_per_ts):
                if len(items) >= max_items:
                    break

                # deterministic cloud pattern
                cloud = float((t * 7 + k) % 50)

                # If cloud_max filters too much, we still want to produce scenes later via top-up
                if cloud_max is not None and cloud > cloud_max:
                    continue

                items.append(
                    {
                        "id": f"MOCK_{t:04d}_{k:03d}",
                        "geometry": footprint,
                        "properties": {
                            "datetime": dt.isoformat().replace("+00:00", "Z"),
                            "eo:cloud_cover": cloud,
                            "cloud_cover": cloud,
                            "platform": "sentinel-2a" if (k % 2 == 0) else "sentinel-2b",
                            "constellation": "sentinel-2",
                        },
                    }
                )

            if len(items) >= max_items:
                break

        # ---------------------------------------------------------
        # Top-up: if cloud_max was restrictive, fill remaining slots
        # while keeping temporal spread (cycle through timestamps)
        # ---------------------------------------------------------
        if len(items) < max_items:
            # Use timestamps again (evenly spread), but force cloud=0 so they pass any filter
            idx = 0
            while len(items) < max_items and timestamps:
                dt = timestamps[idx % len(timestamps)]
                items.append(
                    {
                        "id": f"MOCK_FILL_{len(items):05d}",
                        "geometry": footprint,
                        "properties": {
                            "datetime": dt.isoformat().replace("+00:00", "Z"),
                            "eo:cloud_cover": 0.0,
                            "cloud_cover": 0.0,
                            "platform": "sentinel-2a",
                            "constellation": "sentinel-2",
                        },
                    }
                )
                idx += 1

        return items[:max_items], aoi_geom

    # ---------------------------------------------------------
    # Convert to DataFrame (used by SceneCatalogBuilder)
    # ---------------------------------------------------------
    def items_to_dataframe(self, items, *, collection: str) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for it in items:
            props = it["properties"]
            rows.append(
                {
                    "id": it["id"],
                    "datetime": props["datetime"],
                    "cloud_cover": props.get("eo:cloud_cover", props.get("cloud_cover")),
                    "platform": props.get("platform"),
                    "constellation": props.get("constellation"),
                    "collection": collection,
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
            df = df.dropna(subset=["datetime"])
            df["cloud_cover"] = pd.to_numeric(df["cloud_cover"], errors="coerce")
            df = df.sort_values(["datetime", "cloud_cover"], ascending=[True, True]).reset_index(drop=True)

        return df