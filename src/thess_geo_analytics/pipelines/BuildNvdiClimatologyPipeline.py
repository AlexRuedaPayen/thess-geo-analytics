from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd

from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildNdviClimatologyParams:
    aoi_id: str = "el522"

    # Reuse outputs from previous stats step
    in_stats_csv: Path = RepoPaths.table("ndvi_period_stats.csv")

    # Ticket output spelling
    out_csv: Path = RepoPaths.table("nvdi_climatology.csv")
    # Optional canonical copy
    out_csv_canonical: Path = RepoPaths.table("ndvi_climatology.csv")

    out_fig: Path = RepoPaths.figure("ndvi_climatology.png")


class BuildNdviClimatologyPipeline:
    _MONTH_RE = re.compile(r"^\d{4}-\d{2}$")        # YYYY-MM
    _QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")     # YYYY-Qn

    def run(self, params: BuildNdviClimatologyParams) -> tuple[Path, Path]:
        if not params.in_stats_csv.exists():
            raise FileNotFoundError(
                f"Missing stats table: {params.in_stats_csv}. "
                "Run period stats first."
            )

        df = pd.read_csv(params.in_stats_csv)

        required = {"period", "aoi_id", "mean_ndvi", "median_ndvi"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"{params.in_stats_csv} missing columns: {sorted(missing)}. Found: {sorted(df.columns)}"
            )

        # Filter AOI
        df = df[df["aoi_id"].astype(str) == str(params.aoi_id)].copy()

        # Split monthly vs quarterly
        df_month = df[df["period"].astype(str).str.match(self._MONTH_RE, na=False)].copy()
        df_q = df[df["period"].astype(str).str.match(self._QUARTER_RE, na=False)].copy()

        if df_month.empty and df_q.empty:
            raise RuntimeError(
                "No monthly OR quarterly rows found in ndvi_period_stats.csv for this AOI."
            )

        if not df_month.empty:
            clim, mode = self._monthly_climatology(df_month)
        else:
            clim, mode = self._quarterly_climatology(df_q)

        # Save CSVs
        params.out_csv.parent.mkdir(parents=True, exist_ok=True)
        clim.to_csv(params.out_csv, index=False)
        clim.to_csv(params.out_csv_canonical, index=False)

        # Plot
        params.out_fig.parent.mkdir(parents=True, exist_ok=True)
        self._plot(clim, params.out_fig, mode=mode)

        print(f"[OK] Climatology CSV → {params.out_csv}")
        print(f"[OK] Climatology CSV → {params.out_csv_canonical}")
        print(f"[OK] Figure          → {params.out_fig}")
        return params.out_csv, params.out_fig

    # -----------------------
    # climatology builders
    # -----------------------
    @staticmethod
    def _monthly_climatology(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
        # Parse month timestamp and extract month-of-year
        df = df.assign(
            time=pd.to_datetime(df["period"], format="%Y-%m"),
            month_of_year=lambda d: d["time"].dt.month,
            year=lambda d: d["time"].dt.year,
        )

        n_years = df["year"].nunique()
        if n_years < 3:
            print(
                f"[WARN] Only {n_years} year(s) of MONTHLY data available. "
                "Seasonal curve will be computed but may not represent a robust climatology."
            )

        clim = (
            df.groupby("month_of_year", as_index=False)
            .agg(
                mean_ndvi_clim=("mean_ndvi", "mean"),
                median_ndvi_clim=("median_ndvi", "median"),
                n_periods=("median_ndvi", "size"),
                n_years=("year", "nunique"),
            )
            .sort_values("month_of_year")
            .reset_index(drop=True)
        )

        clim["label"] = pd.to_datetime(clim["month_of_year"], format="%m").dt.strftime("%b")
        return clim, "monthly"

    @staticmethod
    def _quarterly_climatology(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
        # period: YYYY-Qn
        # Extract quarter-of-year and year
        parts = df["period"].astype(str).str.split("-", expand=True)
        df = df.assign(
            year=parts[0].astype(int),
            quarter_of_year=parts[1].str.replace("Q", "", regex=False).astype(int),
        )

        n_years = df["year"].nunique()
        if n_years < 3:
            print(
                f"[WARN] Only {n_years} year(s) of QUARTERLY data available. "
                "Seasonal curve will be computed but may not represent a robust climatology."
            )

        clim = (
            df.groupby("quarter_of_year", as_index=False)
            .agg(
                mean_ndvi_clim=("mean_ndvi", "mean"),
                median_ndvi_clim=("median_ndvi", "median"),
                n_periods=("median_ndvi", "size"),
                n_years=("year", "nunique"),
            )
            .sort_values("quarter_of_year")
            .reset_index(drop=True)
        )

        clim["label"] = "Q" + clim["quarter_of_year"].astype(str)
        return clim, "quarterly"

    # -----------------------
    # plotting
    # -----------------------
    @staticmethod
    def _plot(clim: pd.DataFrame, out_path: Path, *, mode: str) -> None:
        import matplotlib.pyplot as plt

        fig = plt.figure(figsize=(9.5, 4.8))
        ax = fig.add_subplot(1, 1, 1)

        if mode == "monthly":
            x = clim["month_of_year"]
            ax.set_xlabel("Month of year")
            ax.set_xticks(range(1, 13))
            ax.set_xticklabels(clim["label"])
            title = "NDVI climatology (calendar-month baseline)"
        else:
            x = clim["quarter_of_year"]
            ax.set_xlabel("Quarter of year")
            ax.set_xticks(range(1, 5))
            ax.set_xticklabels(clim["label"])
            title = "NDVI climatology (calendar-quarter baseline, fallback)"

        ax.plot(x, clim["mean_ndvi_clim"], marker="o", label="Mean NDVI (climatology)")
        ax.plot(x, clim["median_ndvi_clim"], marker="o", label="Median NDVI (climatology)")

        ax.set_title(title)
        ax.set_ylabel("NDVI")
        ax.grid(True, alpha=0.25)
        ax.legend()

        fig.tight_layout()
        fig.savefig(out_path, dpi=200)
        plt.close(fig)


if __name__ == "__main__":
    BuildNdviClimatologyPipeline().run(BuildNdviClimatologyParams())
