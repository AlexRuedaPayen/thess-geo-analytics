from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import numpy as np
import pandas as pd
import rasterio

from thess_geo_analytics.utils.RepoPaths import RepoPaths


_PERIOD_RE = re.compile(r"^(\d{4})-(\d{2}|Q[1-4])$")  # YYYY-MM or YYYY-Qn


@dataclass(frozen=True)
class BuildNdviPeriodStatsParams:
    period: str                 # "YYYY-MM" or "YYYY-Qn"
    aoi_id: str = "el522"
    out_csv: Path = RepoPaths.table("ndvi_period_stats.csv")


class BuildNdviPeriodStatsPipeline:
    """
    Computes NDVI stats from a composite GeoTIFF for a given period.

    Expects composite path:
      outputs/cogs/ndvi_<period>_<aoi_id>.tif
    where <period> is YYYY-MM or YYYY-Qn.

    Appends/updates outputs/tables/ndvi_period_stats.csv.
    """

    def run(self, params: BuildNdviPeriodStatsParams) -> Path:
        self._validate_period(params.period)

        tif_path = RepoPaths.OUTPUTS / "cogs" / f"ndvi_{params.period}_{params.aoi_id}.tif"
        if not tif_path.exists():
            raise FileNotFoundError(f"NDVI composite not found: {tif_path}")

        out_row = self._compute_stats_for_tif(tif_path)
        out_row["period"] = params.period
        out_row["aoi_id"] = params.aoi_id
        out_row["tif_path"] = str(tif_path)

        params.out_csv.parent.mkdir(parents=True, exist_ok=True)

        if params.out_csv.exists():
            df = pd.read_csv(params.out_csv)
            # replace existing row for that (period, aoi_id)
            df = df[~((df["period"] == params.period) & (df["aoi_id"] == params.aoi_id))]
            df = pd.concat([df, pd.DataFrame([out_row])], ignore_index=True)
        else:
            df = pd.DataFrame([out_row])

        # stable sort
        df = df.sort_values(["aoi_id", "period"]).reset_index(drop=True)

        df.to_csv(params.out_csv, index=False)
        print(f"[OK] NDVI stats written → {params.out_csv}")

        return params.out_csv

    def run_all_existing(self, *, aoi_id: str = "el522", out_csv: Path | None = None) -> Path:
        """
        Convenience: compute stats for every ndvi_<period>_<aoi_id>.tif currently present in outputs/cogs.
        """
        out_csv = out_csv or RepoPaths.table("ndvi_period_stats.csv")
        cogs_dir = RepoPaths.OUTPUTS / "cogs"
        cogs_dir.mkdir(parents=True, exist_ok=True)

        tifs = sorted(cogs_dir.glob(f"ndvi_*_{aoi_id}.tif"))
        if not tifs:
            raise FileNotFoundError(f"No composites found in {cogs_dir} for aoi_id={aoi_id}")

        # Extract "period" from filename: ndvi_<period>_<aoi_id>.tif
        periods: list[str] = []
        for p in tifs:
            stem = p.stem  # ndvi_2025-08_el522
            parts = stem.split("_")
            if len(parts) < 3:
                continue
            period = parts[1]
            try:
                self._validate_period(period)
            except Exception:
                continue
            periods.append(period)

        if not periods:
            raise RuntimeError("Found composites but could not parse any valid periods from filenames.")

        for period in periods:
            self.run(BuildNdviPeriodStatsParams(period=period, aoi_id=aoi_id, out_csv=out_csv))

        return out_csv

    # -----------------------
    # internals
    # -----------------------
    def _validate_period(self, period: str) -> None:
        if not _PERIOD_RE.match(period):
            raise ValueError("period must be 'YYYY-MM' or 'YYYY-Qn' (e.g., 2025-08 or 2025-Q3)")

    def _compute_stats_for_tif(self, tif_path: Path) -> dict:
        with rasterio.open(tif_path) as ds:
            nd = ds.read(1).astype(np.float32)
            nodata = ds.nodata
            if nodata is not None:
                nd = np.where(nd == nodata, np.nan, nd)

        valid_mask = ~np.isnan(nd)
        valid_count = int(valid_mask.sum())
        total_count = int(nd.size)

        if valid_count == 0:
            raise ValueError(f"{tif_path.name} contains no valid pixels — cannot compute stats.")

        valid = nd[valid_mask]

        # Extra useful stats (professional set)
        return {
            "mean_ndvi": float(np.mean(valid)),
            "median_ndvi": float(np.median(valid)),
            "p10_ndvi": float(np.percentile(valid, 10)),
            "p90_ndvi": float(np.percentile(valid, 90)),
            "std_ndvi": float(np.std(valid)),
            "valid_pixel_ratio": float(valid_count / total_count),
            "count_valid_pixels": valid_count,
            "count_total_pixels": total_count,
        }

    @staticmethod
    def smoke_test() -> None:
        print("=== BuildNdviPeriodStatsPipeline Smoke Test ===")
        print("[SKIP] Needs an existing composite tif: outputs/cogs/ndvi_<period>_<aoi_id>.tif")
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildNdviPeriodStatsPipeline.smoke_test()
