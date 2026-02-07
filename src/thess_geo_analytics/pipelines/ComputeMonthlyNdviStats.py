from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path
import rasterio

from thess_geo_analytics.utils.RepoPaths import RepoPaths


class ComputeMonthlyNdviStats:
    """
    Computes summary metrics for the monthly NDVI composite:
      - mean NDVI (valid pixels)
      - median NDVI
      - valid pixel ratio
      - count of valid pixels
    Writes/updates a CSV table:
        outputs/tables/ndvi_monthly_stats.csv
    """

    def run(self, month: str, aoi_id: str = "el522") -> Path:
        tif_path = RepoPaths.OUTPUTS / "cogs" / f"ndvi_{month}_{aoi_id}.tif"
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

        mean_ndvi = float(np.nanmean(nd))
        median_ndvi = float(np.nanmedian(nd))
        valid_ratio = valid_count / total_count

        out_row = {
            "month": month,
            "mean_ndvi": mean_ndvi,
            "median_ndvi": median_ndvi,
            "valid_pixel_ratio": valid_ratio,
            "count_valid_pixels": valid_count,
        }

        stats_path = RepoPaths.table("ndvi_monthly_stats.csv")
        stats_path.parent.mkdir(parents=True, exist_ok=True)

        if stats_path.exists():
            df = pd.read_csv(stats_path)
            df = df[df["month"] != month]    # avoid duplicates
            df = pd.concat([df, pd.DataFrame([out_row])], ignore_index=True)
        else:
            df = pd.DataFrame([out_row])

        df.to_csv(stats_path, index=False)
        print(f"[INFO] NDVI stats written → {stats_path}")

        return stats_path


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m thess_geo_analytics.pipelines.ComputeMonthlyNdviStats YYYY-MM")

    ComputeMonthlyNdviStats().run(sys.argv[1])
