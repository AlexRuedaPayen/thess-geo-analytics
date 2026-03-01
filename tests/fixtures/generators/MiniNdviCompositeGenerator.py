from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import numpy as np
import rasterio
from rasterio.transform import from_origin


@dataclass(frozen=True)
class MiniNdviCompositeConfig:
    """
    Create tiny NDVI composites:

        <root>/
          outputs/
            cogs/
              ndvi_<period>_<aoi_id>.tif

    These are consumed by BuildNdviMonthlyStatisticsPipeline.
    """
    root: Path
    aoi_id: str
    periods: List[str]
    height: int = 16
    width: int = 16


class MiniNdviCompositeGenerator:
    """
    Writes small NDVI GeoTIFFs with values in [-1, 1].
    """

    def __init__(self, cfg: MiniNdviCompositeConfig) -> None:
        self.cfg = cfg

    def generate(self) -> List[Path]:
        """
        Returns list of created COG paths.
        """
        outputs_root = self.cfg.root / "outputs"
        cogs_dir = outputs_root / "cogs"
        cogs_dir.mkdir(parents=True, exist_ok=True)

        created: List[Path] = []

        transform = from_origin(0.0, 0.0, 10.0, 10.0)
        base_profile = {
            "driver": "GTiff",
            "height": self.cfg.height,
            "width": self.cfg.width,
            "count": 1,
            "dtype": "float32",
            "crs": "EPSG:32634",
            "transform": transform,
            "nodata": -9999.0,
        }

        for i, period in enumerate(self.cfg.periods):
            out_path = cogs_dir / f"ndvi_{period}_{self.cfg.aoi_id}.tif"

            # Slightly different NDVI per period, but always in [-1, 1]
            base_val = -0.2 + 0.2 * i
            data = np.full(
                (self.cfg.height, self.cfg.width),
                base_val,
                dtype=np.float32,
            )

            with rasterio.open(out_path, "w", **base_profile) as dst:
                dst.write(data, 1)

            created.append(out_path)

        return created


# ----------------------------------------------------------------------
# Simple CLI: run the NDVI COG generator directly
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate minimal NDVI composite COGs for testing."
    )
    parser.add_argument(
        "--root",
        type=str,
        default="tests/fixtures/generated/ndvi_monthly",
        help="Root directory where outputs/cogs/ will be created (default: %(default)s).",
    )
    parser.add_argument(
        "--aoi-id",
        type=str,
        default="el522",
        help="AOI identifier to embed in filenames (default: %(default)s).",
    )
    parser.add_argument(
        "--periods",
        type=str,
        nargs="*",
        default=["2024-01", "2024-02", "2024-03"],
        help="List of periods (YYYY-MM or YYYY-Qn) to generate.",
    )
    args = parser.parse_args()

    cfg = MiniNdviCompositeConfig(
        root=Path(args.root),
        aoi_id=args.aoi_id,
        periods=list(args.periods),
    )
    gen = MiniNdviCompositeGenerator(cfg)
    created = gen.generate()
    print("[OK] Generated NDVI composites:")
    for p in created:
        print("  ", p)