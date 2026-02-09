from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import numpy as np
import pandas as pd
import rasterio

from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildNdviMonthlyStatsParams:
    month: str
    aoi_id: str = "el522"


class BuildNdviMonthlyStatsPipeline:
    def run(self, params: BuildNdviMonthlyStatsParams) -> Path:
        tif_path = RepoPaths.OUTPUTS / "cogs" / f"ndvi_{params.month}_{params.aoi_id}.tif"
        if not tif_path.exists():
            raise FileNotFoundError(f"Monthly NDVI raster not found: {tif_path}")

        with rasterio.open(tif_path) as ds:
            nd = ds.read(1).astype(np.float32)
            nodata = ds.nodata
            if nodata is not None:
                nd = np.where(nd == nodata, np.nan, nd)

        valid = ~np.isnan(nd)
        valid_count = int(valid.sum())
        total_count = int(nd.size)

        if valid_count == 0:
            raise ValueError("Composite contains no valid pixels — cannot compute stats.")

        out_row = {
            "month": params.month,
            "mean_ndvi": float(np.nanmean(nd)),
            "median_ndvi": float(np.nanmedian(nd)),
            "valid_pixel_ratio": float(valid_count / total_count),
            "count_valid_pixels": valid_count,
        }

        stats_path = RepoPaths.table("ndvi_monthly_stats.csv")
        stats_path.parent.mkdir(parents=True, exist_ok=True)

        if stats_path.exists():
            df = pd.read_csv(stats_path)
            df = df[df["month"] != params.month]
            df = pd.concat([df, pd.DataFrame([out_row])], ignore_index=True)
        else:
            df = pd.DataFrame([out_row])

        df.to_csv(stats_path, index=False)
        print(f"[OK] NDVI stats written → {stats_path}")

        return stats_path

    @staticmethod
    def smoke_test() -> None:
        print("=== BuildNdviMonthlyStatsPipeline Smoke Test ===")
        print("[SKIP] Needs an existing monthly composite tif.")
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildNdviMonthlyStatsPipeline.smoke_test()
