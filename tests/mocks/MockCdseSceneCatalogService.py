# tests/mocks/MockCdseSceneCatalogService.py
from __future__ import annotations

import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import pandas as pd
from shapely.affinity import translate
from shapely.geometry import shape, mapping, box

from thess_geo_analytics.core.params import StacQueryParams


class MockCdseSceneCatalogService:
    """
    Offline replacement for CdseSceneCatalogService.

    Produces deterministic STAC-like items:
      - timestamps spread across [date_start .. today] (UTC)
      - 2..11 tiles intersecting AOI per timestamp (configurable)
      - at least ONE tile per timestamp fully covers AOI.buffer(0.05) used by pipeline
      - honors params.max_items and params.cloud_cover_max (but won't "cluster at start")

    Designed so BuildSceneCatalogPipeline + TileSelector produce NON-empty outputs
    under normal test params.
    """

    def __init__(
        self,
        *,
        seed: int = 1337,
        tiles_min: int = 2,
        tiles_max: int = 11,
        revisit_days: int = 5,
        # extra padding beyond pipeline's buffer(0.05) to guarantee full cover
        full_cover_pad_deg: float = 0.02,
        # size of partial tiles around AOI bbox (multiplier)
        partial_bbox_expand: float = 0.10,
    ) -> None:
        if tiles_min < 1 or tiles_max < tiles_min:
            raise ValueError("tiles_min/tiles_max invalid")
        if revisit_days < 1:
            raise ValueError("revisit_days must be >= 1")

        self.seed = int(seed)
        self.tiles_min = int(tiles_min)
        self.tiles_max = int(tiles_max)
        self.revisit_days = int(revisit_days)
        self.full_cover_pad_deg = float(full_cover_pad_deg)
        self.partial_bbox_expand = float(partial_bbox_expand)

        self._rng = random.Random(self.seed)

    # ---------------------------------------------------------
    # Helpers
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

    def _parse_date_start(self, date_start: str) -> datetime:
        # Allow YYYY-MM-DD or full ISO; always return UTC-aware
        # If only date, interpret at 00:00:00 UTC.
        if "T" in date_start:
            dt = datetime.fromisoformat(date_start.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        d = datetime.fromisoformat(date_start).date()
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

    def _today_utc_floor(self) -> datetime:
        return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    def _candidate_timestamps(self, start: datetime, today: datetime) -> List[datetime]:
        # baseline revisit cadence timestamps (UTC, with small deterministic time jitter)
        days = max(0, (today - start).days)
        if days == 0:
            base = [start]
        else:
            n = (days // self.revisit_days) + 1
            base = [start + timedelta(days=i * self.revisit_days) for i in range(n)]

        # add deterministic time-of-day jitter (minutes) so timestamps look real
        out: List[datetime] = []
        for i, dt in enumerate(base):
            # jitter within the day but keep within [start..today]
            minutes = (i * 37) % (24 * 60)
            jittered = dt + timedelta(minutes=minutes)
            if jittered < start:
                jittered = start
            if jittered > today:
                jittered = today
            out.append(jittered)
        return out

    # ---------------------------------------------------------
    # STAC-like search
    # ---------------------------------------------------------
    def search_items(
        self,
        aoi_geojson_path: Path,
        date_start: str,
        date_end: str,  # kept for signature compatibility; pipeline end is "today"
        params: StacQueryParams,
    ) -> Tuple[List[Any], Dict[str, Any]]:

        max_items = int(getattr(params, "max_items", 1000) or 1000)
        cloud_max = getattr(params, "cloud_cover_max", None)
        cloud_max = None if cloud_max is None else float(cloud_max)

        aoi_geom = self._load_aoi_geom(aoi_geojson_path)
        aoi = shape(aoi_geom)

        start = self._parse_date_start(date_start)
        today = self._today_utc_floor()
        if today < start:
            today = start

        # AOI used by pipeline selection is aoi_shp.buffer(0.05)
        aoi_for_cover = aoi.buffer(0.05)

        # geometry for the guaranteed full-cover tile per timestamp
        full_cover_geom = mapping(aoi_for_cover.buffer(self.full_cover_pad_deg))

        # base bbox for generating partial tiles (still intersect AOI)
        minx, miny, maxx, maxy = aoi.bounds
        dx = (maxx - minx)
        dy = (maxy - miny)
        pad_x = max(1e-6, dx * self.partial_bbox_expand)
        pad_y = max(1e-6, dy * self.partial_bbox_expand)

        base_partial = box(minx - pad_x, miny - pad_y, maxx + pad_x, maxy + pad_y)

        # Build candidate timestamps and then SELECT a subset across the whole range
        ts_all = self._candidate_timestamps(start, today)
        if not ts_all:
            ts_all = [start]

        # Determine how many timestamps we should use to roughly reach max_items
        avg_tiles = (self.tiles_min + self.tiles_max) / 2.0
        target_ts = max(1, int(math.ceil(max_items / max(1.0, avg_tiles))))

        # If too many candidates, pick evenly spaced timestamps across the whole range
        if target_ts >= len(ts_all):
            ts_sel = ts_all
        else:
            # deterministic evenly spaced indices via linspace-like
            idxs = [round(i * (len(ts_all) - 1) / (target_ts - 1)) for i in range(target_ts)]
            # ensure unique & sorted
            idxs = sorted(set(int(i) for i in idxs))
            ts_sel = [ts_all[i] for i in idxs]

        items: List[Dict[str, Any]] = []

        # For each selected timestamp, generate k tiles (2..11),
        # ensuring ONE full-cover tile and the rest partial intersections.
        for t_idx, acq_dt in enumerate(ts_sel):
            if len(items) >= max_items:
                break

            k = self._rng.randint(self.tiles_min, self.tiles_max)

            # First tile = full cover (guaranteed has_full_cover)
            tiles_for_dt: List[Dict[str, Any]] = []
            tiles_for_dt.append(
                {
                    "id": f"MOCK_FULL_{t_idx:04d}_000",
                    "geometry": full_cover_geom,
                    "properties": {
                        "datetime": acq_dt.isoformat().replace("+00:00", "Z"),
                        "eo:cloud_cover": float((t_idx * 11) % 60),
                        "cloud_cover": float((t_idx * 11) % 60),
                        "platform": "sentinel-2a",
                        "constellation": "sentinel-2",
                    },
                }
            )

            # Remaining tiles = partial overlaps (still intersect)
            for j in range(1, k):
                # shift partial bbox around AOI in a deterministic pattern
                # offsets in degrees (small-ish)
                ox = ((t_idx * 0.01) + (j * 0.005)) * (1 if (j % 2 == 0) else -1)
                oy = ((t_idx * 0.007) + (j * 0.004)) * (1 if ((t_idx + j) % 2 == 0) else -1)

                geom = translate(base_partial, xoff=ox, yoff=oy)

                tiles_for_dt.append(
                    {
                        "id": f"MOCK_PART_{t_idx:04d}_{j:03d}",
                        "geometry": mapping(geom),
                        "properties": {
                            "datetime": acq_dt.isoformat().replace("+00:00", "Z"),
                            "eo:cloud_cover": float((t_idx * 13 + j * 7) % 80),
                            "cloud_cover": float((t_idx * 13 + j * 7) % 80),
                            "platform": "sentinel-2a" if (j % 2 == 0) else "sentinel-2b",
                            "constellation": "sentinel-2",
                        },
                    }
                )

            # Apply cloud filter per-item (like STAC would)
            if cloud_max is not None:
                tiles_for_dt = [
                    it
                    for it in tiles_for_dt
                    if float(it["properties"].get("eo:cloud_cover", 1e9)) <= cloud_max
                ]
                # If filtering removed everything (including full-cover), keep the full-cover tile with cloud=0
                if not tiles_for_dt:
                    tiles_for_dt = [
                        {
                            "id": f"MOCK_FULL_{t_idx:04d}_000",
                            "geometry": full_cover_geom,
                            "properties": {
                                "datetime": acq_dt.isoformat().replace("+00:00", "Z"),
                                "eo:cloud_cover": 0.0,
                                "cloud_cover": 0.0,
                                "platform": "sentinel-2a",
                                "constellation": "sentinel-2",
                            },
                        }
                    ]

            # Add until max_items
            for it in tiles_for_dt:
                items.append(it)
                if len(items) >= max_items:
                    break

        # If still short (e.g. very low cloud_max), top up by adding extra PARTIAL tiles
        # across the selected timestamps to reach exactly max_items.
        if len(items) < max_items:
            # distribute fill tiles across timestamps (round-robin)
            t_fill = 0
            while len(items) < max_items and ts_sel:
                acq_dt = ts_sel[t_fill % len(ts_sel)]
                j = len(items) % 1000
                geom = translate(base_partial, xoff=(j % 10) * 0.002, yoff=((j // 10) % 10) * -0.002)
                items.append(
                    {
                        "id": f"MOCK_FILL_{len(items):06d}",
                        "geometry": mapping(geom),
                        "properties": {
                            "datetime": acq_dt.isoformat().replace("+00:00", "Z"),
                            "eo:cloud_cover": 0.0,
                            "cloud_cover": 0.0,
                            "platform": "sentinel-2a",
                            "constellation": "sentinel-2",
                        },
                    }
                )
                t_fill += 1

        return items[:max_items], aoi_geom

    # ---------------------------------------------------------
    # Convert items to DataFrame
    # ---------------------------------------------------------
    def items_to_dataframe(self, items: Sequence[Mapping[str, Any]], *, collection: str) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []

        for it in items:
            props = it.get("properties", {}) or {}
            rows.append(
                {
                    "id": it.get("id"),
                    "datetime": props.get("datetime"),
                    "cloud_cover": props.get("eo:cloud_cover", props.get("cloud_cover")),
                    "platform": props.get("platform"),
                    "constellation": props.get("constellation"),
                    "collection": collection,
                }
            )

        df = pd.DataFrame(
            rows,
            columns=["id", "datetime", "cloud_cover", "platform", "constellation", "collection"],
        )

        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
            df["cloud_cover"] = pd.to_numeric(df["cloud_cover"], errors="coerce")
            df = df.dropna(subset=["datetime"]).sort_values(["datetime", "cloud_cover"]).reset_index(drop=True)

        return df