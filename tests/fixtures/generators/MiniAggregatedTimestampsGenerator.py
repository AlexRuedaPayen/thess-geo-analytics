from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import numpy as np
import rasterio
from rasterio.transform import from_origin


@dataclass(frozen=True)
class MiniAggregatedTimestampsConfig:
    """
    Minimal config to create a tiny aggregated-timestamps tree:

        <root>/
          aggregated/
            <sanitized_timestamp1>/
              ...B04.tif
              ...B08.tif
            <sanitized_timestamp2>/
              ...

    `timestamps` can be ISO-like strings (e.g. "2024-01-10T10:00:00Z").
    We sanitize them for filesystem safety (no ':' on Windows), but the
    sanitization is chosen so that NdviAggregatedCompositeBuilder._discover
    can still parse them correctly.
    """
    root: Path
    timestamps: List[str]
    height: int = 16
    width: int = 16


class MiniAggregatedTimestampsGenerator:
    """
    Creates a tiny aggregated root with a couple of timestamp folders
    containing synthetic B04/B08 rasters.
    """

    def __init__(self, cfg: MiniAggregatedTimestampsConfig) -> None:
        self.cfg = cfg

    def generate(self) -> Path:
        """
        Returns the aggregated root folder:

            aggregated_root = <root> / "aggregated"
        """
        aggregated_root = self.cfg.root / "aggregated"
        aggregated_root.mkdir(parents=True, exist_ok=True)

        for ts in self.cfg.timestamps:
            # ---- Windows-safe folder name ----
            # Replace ':' with '_' so it's a valid folder name.
            # NdviAggregatedCompositeBuilder._discover normalizes
            # by replacing '_' back to ':' (after switching ' ' to 'T'),
            # so this remains parseable.
            folder_name = ts.replace(":", "_")

            ts_dir = aggregated_root / folder_name
            ts_dir.mkdir(parents=True, exist_ok=True)

            self._write_band(ts_dir / "dummy_B04.tif", value=0.2)
            self._write_band(ts_dir / "dummy_B08.tif", value=0.8)

        return aggregated_root

    def _write_band(self, path: Path, *, value: float) -> None:
        h, w = self.cfg.height, self.cfg.width

        transform = from_origin(0.0, 0.0, 10.0, 10.0)
        profile = {
            "driver": "GTiff",
            "height": h,
            "width": w,
            "count": 1,
            "dtype": "float32",
            "crs": "EPSG:32634",
            "transform": transform,
            "nodata": -9999.0,
        }

        data = np.full((h, w), value, dtype=np.float32)

        path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(path, "w", **profile) as dst:
            dst.write(data, 1)


# ----------------------------------------------------------------------
# Simple CLI: allow you to just "run the generator"
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Generate a minimal aggregated-timestamps tree for testing "
            "(B04/B08 synthetic rasters)."
        )
    )
    parser.add_argument(
        "--root",
        type=str,
        default="tests/fixtures/generated/ndvi_aggregated",
        help="Root directory where 'aggregated/' will be created (default: %(default)s).",
    )
    parser.add_argument(
        "--timestamps",
        type=str,
        nargs="*",
        default=[
            "2024-01-10T10:00:00Z",
            "2024-01-20T10:00:00Z",
            "2024-02-05T10:00:00Z",
        ],
        help="List of timestamp labels (ISO-like) to generate.",
    )
    args = parser.parse_args()

    cfg = MiniAggregatedTimestampsConfig(
        root=Path(args.root),
        timestamps=list(args.timestamps),
    )
    gen = MiniAggregatedTimestampsGenerator(cfg)
    aggregated_root = gen.generate()
    print(f"[OK] Aggregated test data written under: {aggregated_root}")