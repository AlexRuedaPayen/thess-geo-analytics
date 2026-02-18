from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd

from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildNdviTimeSeriesParams:
    aoi_id: str = "el522"

    # Source (reuse output from BuildNdviPeriodStatsPipeline)
    in_stats_csv: Path = RepoPaths.table("ndvi_period_stats.csv")

    # Outputs (ticket expects "nvdi" spelling)
    out_parquet: Path = RepoPaths.table("nvdi_timeseries.parquet")
    out_parquet_canonical: Path = RepoPaths.table("ndvi_timeseries.parquet")
    out_fig: Path = RepoPaths.figure("ndvi_timeseries.png")


class BuildNdviTimeSeriesPipeline:
    _MONTH_RE = re.compile(r"^\d{4}-\d{2}$")

    def run(self, params: BuildNdviTimeSeriesParams) -> tuple[Path, Path]:
        if not params.in_stats_csv.exists():
            raise FileNotFoundError(
                f"Missing stats table: {params.in_stats_csv}\n"
                f"Run period stats first to create it."
            )

        df = pd.read_csv(params.in_stats_csv)

        required = {"period", "aoi_id", "mean_ndvi", "median_ndvi"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"{params.in_stats_csv} missing columns: {sorted(missing)}. Found: {sorted(df.columns)}"
            )

        # Filter AOI + monthly only
        df = df[df["aoi_id"].astype(str) == str(params.aoi_id)].copy()
        df = df[df["period"].astype(str).str.match(self._MONTH_RE, na=False)].copy()

        if df.empty:
            raise RuntimeError(
                "No MONTHLY rows found in ndvi_period_stats.csv for this AOI.\n"
                "This usually means you only generated quarterly composites/stats.\n"
                "Generate monthly composites and run period stats again."
            )

        # Build time series
        df_ts = (
            df[["period", "mean_ndvi", "median_ndvi"]]
            .rename(columns={"period": "time"})
            .assign(time=lambda d: pd.to_datetime(d["time"], format="%Y-%m"))
            .sort_values("time")
            .reset_index(drop=True)
        )

        # Save outputs/tables/*.parquet
        params.out_parquet.parent.mkdir(parents=True, exist_ok=True)
        df_ts.to_parquet(params.out_parquet, index=False)
        df_ts.to_parquet(params.out_parquet_canonical, index=False)

        # Plot to outputs/figures/
        params.out_fig.parent.mkdir(parents=True, exist_ok=True)
        self._plot(df_ts, params.out_fig)

        print(f"[OK] Parquet → {params.out_parquet}")
        print(f"[OK] Parquet → {params.out_parquet_canonical}")
        print(f"[OK] Plot    → {params.out_fig}")
        return params.out_parquet, params.out_fig

    @staticmethod
    def _plot(df_ts: pd.DataFrame, out_path: Path) -> None:
        import matplotlib.pyplot as plt

        fig = plt.figure(figsize=(10, 4.8))
        ax = fig.add_subplot(1, 1, 1)

        ax.plot(df_ts["time"], df_ts["mean_ndvi"], label="Mean NDVI")
        ax.plot(df_ts["time"], df_ts["median_ndvi"], label="Median NDVI")

        ax.set_title("NDVI time series (monthly composites)")
        ax.set_xlabel("Time")
        ax.set_ylabel("NDVI")
        ax.grid(True, alpha=0.25)
        ax.legend()

        fig.tight_layout()
        fig.savefig(out_path, dpi=200)
        plt.close(fig)


if __name__ == "__main__":
    BuildNdviTimeSeriesPipeline().run(BuildNdviTimeSeriesParams())
