from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd

from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildNdviAnomaliesParams:
    aoi_id: str = "el522"

    # Inputs (reuse outputs)
    in_stats_csv: Path = RepoPaths.table("ndvi_period_stats.csv")
    in_climatology_csv: Path = RepoPaths.table("nvdi_climatology.csv")  # ticket spelling
    in_climatology_csv_fallback: Path = RepoPaths.table("ndvi_climatology.csv")

    # Outputs
    out_csv: Path = RepoPaths.table("nvdi_anomalies.csv")
    out_fig: Path = RepoPaths.figure("ndvi_anomalies.png")


class BuildNdviAnomaliesPipeline:
    _MONTH_RE = re.compile(r"^\d{4}-\d{2}$")        # YYYY-MM
    _QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")     # YYYY-Qn

    def run(self, params: BuildNdviAnomaliesParams) -> tuple[Path, Path]:
        # --- Load stats
        if not params.in_stats_csv.exists():
            raise FileNotFoundError(
                f"Missing stats table: {params.in_stats_csv}. Run period stats first."
            )
        df_stats = pd.read_csv(params.in_stats_csv)

        required_stats = {"period", "aoi_id", "mean_ndvi", "median_ndvi"}
        missing_stats = required_stats - set(df_stats.columns)
        if missing_stats:
            raise ValueError(
                f"{params.in_stats_csv} missing columns: {sorted(missing_stats)}. "
                f"Found: {sorted(df_stats.columns)}"
            )

        df_stats = df_stats[df_stats["aoi_id"].astype(str) == str(params.aoi_id)].copy()
        if df_stats.empty:
            raise RuntimeError(f"No rows for aoi_id={params.aoi_id} in {params.in_stats_csv}")

        # --- Load climatology (nvdi preferred, ndvi fallback)
        clim_path = params.in_climatology_csv if params.in_climatology_csv.exists() else params.in_climatology_csv_fallback
        if not clim_path.exists():
            raise FileNotFoundError(
                f"Missing climatology CSV. Expected {params.in_climatology_csv} (preferred) "
                f"or {params.in_climatology_csv_fallback}."
            )
        df_clim = pd.read_csv(clim_path)

        required_clim = {"mean_ndvi_clim", "median_ndvi_clim"}
        missing_clim = required_clim - set(df_clim.columns)
        if missing_clim:
            raise ValueError(
                f"{clim_path} missing columns: {sorted(missing_clim)}. Found: {sorted(df_clim.columns)}"
            )

        # Determine climatology mode by available key columns
        if "month_of_year" in df_clim.columns:
            mode = "monthly"
        elif "quarter_of_year" in df_clim.columns:
            mode = "quarterly"
        else:
            raise ValueError(
                f"{clim_path} must contain either 'month_of_year' or 'quarter_of_year' column."
            )

        # --- Build anomalies according to mode
        if mode == "monthly":
            out = self._build_monthly_anomalies(df_stats, df_clim)
            title = "NDVI anomalies (monthly baseline)"
        else:
            out = self._build_quarterly_anomalies(df_stats, df_clim)
            title = "NDVI anomalies (quarterly baseline, fallback)"

        # --- Save + plot
        params.out_csv.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(params.out_csv, index=False)

        params.out_fig.parent.mkdir(parents=True, exist_ok=True)
        self._plot(out, params.out_fig, title=title)

        print(f"[OK] Anomalies CSV → {params.out_csv}")
        print(f"[OK] Figure        → {params.out_fig}")
        return params.out_csv, params.out_fig

    # -----------------------
    # monthly mode
    # -----------------------
    def _build_monthly_anomalies(self, df_stats: pd.DataFrame, df_clim: pd.DataFrame) -> pd.DataFrame:
        # keep only monthly periods
        df = df_stats[df_stats["period"].astype(str).str.match(self._MONTH_RE, na=False)].copy()
        if df.empty:
            raise RuntimeError(
                "Climatology is monthly but stats contain no monthly periods. "
                "Generate monthly composites/stats or rebuild climatology with quarterly fallback."
            )

        df = df.assign(
            time=pd.to_datetime(df["period"], format="%Y-%m"),
            month_of_year=lambda d: d["time"].dt.month,
        )

        clim = df_clim[["month_of_year", "mean_ndvi_clim", "median_ndvi_clim"]].copy()
        merged = df.merge(clim, on="month_of_year", how="left")

        if merged["mean_ndvi_clim"].isna().any():
            missing = merged.loc[merged["mean_ndvi_clim"].isna(), "month_of_year"].unique().tolist()
            raise RuntimeError(f"Missing climatology rows for month_of_year={missing}")

        merged["anomaly_mean"] = merged["mean_ndvi"] - merged["mean_ndvi_clim"]
        merged["anomaly_median"] = merged["median_ndvi"] - merged["median_ndvi_clim"]

        out = (
            merged[[
                "time", "period", "mean_ndvi", "median_ndvi",
                "mean_ndvi_clim", "median_ndvi_clim",
                "anomaly_mean", "anomaly_median",
                "month_of_year"
            ]]
            .sort_values("time")
            .reset_index(drop=True)
        )
        return out

    # -----------------------
    # quarterly mode
    # -----------------------
    def _build_quarterly_anomalies(self, df_stats: pd.DataFrame, df_clim: pd.DataFrame) -> pd.DataFrame:
        # keep only quarterly periods
        df = df_stats[df_stats["period"].astype(str).str.match(self._QUARTER_RE, na=False)].copy()
        if df.empty:
            raise RuntimeError(
                "Climatology is quarterly but stats contain no quarterly periods."
            )

        parts = df["period"].astype(str).str.split("-", expand=True)
        df = df.assign(
            year=parts[0].astype(int),
            quarter_of_year=parts[1].str.replace("Q", "", regex=False).astype(int),
        )

        # time = quarter start
        start_month = {1: 1, 2: 4, 3: 7, 4: 10}
        df["time"] = df.apply(lambda r: pd.Timestamp(int(r["year"]), start_month[int(r["quarter_of_year"])], 1), axis=1)

        clim = df_clim[["quarter_of_year", "mean_ndvi_clim", "median_ndvi_clim"]].copy()
        merged = df.merge(clim, on="quarter_of_year", how="left")

        if merged["mean_ndvi_clim"].isna().any():
            missing = merged.loc[merged["mean_ndvi_clim"].isna(), "quarter_of_year"].unique().tolist()
            raise RuntimeError(f"Missing climatology rows for quarter_of_year={missing}")

        merged["anomaly_mean"] = merged["mean_ndvi"] - merged["mean_ndvi_clim"]
        merged["anomaly_median"] = merged["median_ndvi"] - merged["median_ndvi_clim"]

        out = (
            merged[[
                "time", "period", "mean_ndvi", "median_ndvi",
                "mean_ndvi_clim", "median_ndvi_clim",
                "anomaly_mean", "anomaly_median",
                "quarter_of_year"
            ]]
            .sort_values("time")
            .reset_index(drop=True)
        )
        return out

    # -----------------------
    # plotting
    # -----------------------
    @staticmethod
    def _plot(df: pd.DataFrame, out_path: Path, *, title: str) -> None:
        import matplotlib.pyplot as plt

        fig = plt.figure(figsize=(10, 4.8))
        ax = fig.add_subplot(1, 1, 1)

        ax.axhline(0.0, linewidth=1.0)

        ax.plot(df["time"], df["anomaly_mean"], label="Anomaly (mean NDVI)")
        ax.plot(df["time"], df["anomaly_median"], label="Anomaly (median NDVI)")

        ax.set_title(title)
        ax.set_xlabel("Time")
        ax.set_ylabel("NDVI anomaly (NDVI - climatology)")
        ax.grid(True, alpha=0.25)
        ax.legend()

        fig.tight_layout()
        fig.savefig(out_path, dpi=200)
        plt.close(fig)


if __name__ == "__main__":
    BuildNdviAnomaliesPipeline().run(BuildNdviAnomaliesParams())
