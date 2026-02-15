from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import numpy as np
import pandas as pd

from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildNdviTimeSeriesParams:
    """Build an NDVI time series from monthly composite rasters.

    Monthly composite COGs are expected at:
      outputs/cogs/ndvi_YYYY-MM_<aoi_id>.tif
    """

    aoi_id: str = "el522"


class BuildNdviTimeSeriesPipeline:
    _MONTH_RE = re.compile(r"^ndvi_(\d{4}-\d{2})_(?P<aoi>[^.]+)\.tif$")

    def run(self, params: BuildNdviTimeSeriesParams) -> tuple[Path, Path]:
        cogs_dir = RepoPaths.OUTPUTS / "cogs"
        if not cogs_dir.exists():
            raise FileNotFoundError(f"COGs folder not found: {cogs_dir}")

        # Discover monthly composite rasters for the AOI.
        tif_paths: list[Path] = []
        for p in sorted(cogs_dir.glob(f"ndvi_*_{params.aoi_id}.tif")):
            if self._MONTH_RE.match(p.name):
                tif_paths.append(p)



        if not tif_paths:
            raise FileNotFoundError(
                f"No monthly NDVI composites found for aoi_id='{params.aoi_id}' in {cogs_dir}"
            )

        months = [self._MONTH_RE.match(p.name).group(1) for p in tif_paths]  # type: ignore[union-attr]

        
        import pdb
        pdb.set_trace()

        # Prefer reusing monthly stats CSV if available.
        stats_csv = RepoPaths.table("ndvi_monthly_stats.csv")
        df_stats: pd.DataFrame | None = None
        if stats_csv.exists():
            tmp = pd.read_csv(stats_csv)
            if "month" in tmp.columns:
                df_stats = tmp[tmp["month"].astype(str).isin(months)].copy()

        # If CSV is missing/incomplete, compute stats from rasters.
        if df_stats is None:
            df_stats = pd.DataFrame(columns=["month", "mean_ndvi", "median_ndvi"])
            missing_months = months
        else:
            have = set(df_stats["month"].astype(str).tolist())
            missing_months = [m for m in months if m not in have]

        if missing_months:
            # Compute only what's missing.
            import rasterio  # local import so non-raster use-cases don't pay import cost

            rows: list[dict] = []
            for month, tif_path in zip(months, tif_paths):
                if month not in missing_months:
                    continue

                with rasterio.open(tif_path) as ds:
                    nd = ds.read(1).astype(np.float32)
                    nodata = ds.nodata
                    if nodata is not None:
                        nd = np.where(nd == nodata, np.nan, nd)

                valid = ~np.isnan(nd)
                if int(valid.sum()) == 0:
                    raise ValueError(f"Composite contains no valid pixels: {tif_path}")

                rows.append(
                    {
                        "month": month,
                        "mean_ndvi": float(np.nanmean(nd)),
                        "median_ndvi": float(np.nanmedian(nd)),
                    }
                )

            if rows:
                df_stats = pd.concat([df_stats, pd.DataFrame(rows)], ignore_index=True)

        # Build a clean, typed time series.
        df_ts = (
            df_stats[["month", "mean_ndvi", "median_ndvi"]]
            .dropna(subset=["month"])
            .assign(month=lambda d: pd.to_datetime(d["month"], format="%Y-%m"))
            .sort_values("month")
            .reset_index(drop=True)
        )

        # Repository uses "ndvi" everywhere, but the ticket text uses "nvdi".
        # We write the canonical file and also a compatibility copy.
        out_parquet = RepoPaths.table("ndvi_timeseries.parquet")
        out_parquet.parent.mkdir(parents=True, exist_ok=True)
        df_ts.to_parquet(out_parquet, index=False)
        print(f"[OK] NDVI time series parquet written → {out_parquet}")

        out_parquet_compat = RepoPaths.table("nvdi_timeseries.parquet")
        try:
            df_ts.to_parquet(out_parquet_compat, index=False)
            print(f"[OK] Compatibility parquet written → {out_parquet_compat}")
        except Exception:
            # Non-fatal: keep canonical output even if compatibility write fails.
            pass

        # Plot.
        out_fig = RepoPaths.figure("ndvi_timeseries.png")
        out_fig.parent.mkdir(parents=True, exist_ok=True)
        self._plot(df_ts, out_fig)
        print(f"[OK] NDVI time series plot written → {out_fig}")

        return out_parquet, out_fig

    @staticmethod
    def _plot(df_ts: pd.DataFrame, out_path: Path) -> None:
        import matplotlib.pyplot as plt

        if df_ts.empty:
            raise ValueError("Time series is empty — nothing to plot.")

        fig = plt.figure(figsize=(10, 4.8))
        ax = fig.add_subplot(1, 1, 1)
        ax.plot(df_ts["month"], df_ts["mean_ndvi"], label="Mean NDVI")
        ax.plot(df_ts["month"], df_ts["median_ndvi"], label="Median NDVI")
        ax.set_title("NDVI monthly time series")
        ax.set_xlabel("Month")
        ax.set_ylabel("NDVI")
        ax.grid(True, alpha=0.25)
        ax.legend()
        fig.tight_layout()
        fig.savefig(out_path, dpi=200)
        plt.close(fig)


if __name__ == "__main__":
    BuildNdviTimeSeriesPipeline().run(BuildNdviTimeSeriesParams())