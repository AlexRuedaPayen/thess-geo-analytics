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

    # If monthly stats exist but some months are missing,
    # fill those missing months with the corresponding quarterly stats.
    fill_missing_months_from_quarters: bool = True

    # Outputs (ticket expects "nvdi" spelling)
    out_parquet: Path = RepoPaths.table("nvdi_timeseries.parquet")
    out_parquet_canonical: Path = RepoPaths.table("ndvi_timeseries.parquet")
    out_fig: Path = RepoPaths.figure("ndvi_timeseries.png")


class BuildNdviTimeSeriesPipeline:
    _MONTH_RE = re.compile(r"^\d{4}-\d{2}$")        # YYYY-MM
    _QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")     # YYYY-Qn

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
            df_ts = self._build_monthly_series_with_optional_quarter_fill(
                df_month=df_month,
                df_q=df_q,
                fill_missing=params.fill_missing_months_from_quarters,
            )
            series_label = "monthly (with quarterly fill)" if (
                params.fill_missing_months_from_quarters and not df_q.empty
            ) else "monthly"
        else:
            # Fallback: use quarterly series directly
            df_ts = self._build_quarterly_series(df_q)
            series_label = "quarterly (fallback)"

        # Save outputs/tables/*.parquet
        params.out_parquet.parent.mkdir(parents=True, exist_ok=True)
        df_ts.to_parquet(params.out_parquet, index=False)
        df_ts.to_parquet(params.out_parquet_canonical, index=False)

        # Plot to outputs/figures/
        params.out_fig.parent.mkdir(parents=True, exist_ok=True)
        self._plot(df_ts, params.out_fig, series_label=series_label)

        print(f"[OK] Parquet → {params.out_parquet}")
        print(f"[OK] Parquet → {params.out_parquet_canonical}")
        print(f"[OK] Plot    → {params.out_fig}")
        return params.out_parquet, params.out_fig

    # -----------------------
    # helpers
    # -----------------------
    @staticmethod
    def _build_quarterly_series(df_q: pd.DataFrame) -> pd.DataFrame:
        # period: YYYY-Qn -> time = quarter start date
        # Q1 -> Jan 1, Q2 -> Apr 1, Q3 -> Jul 1, Q4 -> Oct 1
        q_start_month = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}

        def quarter_start(period: str) -> pd.Timestamp:
            year, q = period.split("-")
            return pd.Timestamp(int(year), q_start_month[q], 1)

        out = (
            df_q[["period", "mean_ndvi", "median_ndvi"]]
            .assign(time=lambda d: d["period"].astype(str).map(quarter_start))
            .sort_values("time")
            .reset_index(drop=True)
        )
        return out[["time", "mean_ndvi", "median_ndvi"]]

    def _build_monthly_series_with_optional_quarter_fill(
        self,
        df_month: pd.DataFrame,
        df_q: pd.DataFrame,
        fill_missing: bool,
    ) -> pd.DataFrame:
        # Monthly base
        m = (
            df_month[["period", "mean_ndvi", "median_ndvi"]]
            .rename(columns={"period": "month"})
            .assign(time=lambda d: pd.to_datetime(d["month"], format="%Y-%m"))
            .sort_values("time")
            .reset_index(drop=True)
        )

        if (not fill_missing) or df_q.empty:
            return m[["time", "mean_ndvi", "median_ndvi"]]

        # Build full monthly index between min and max observed months
        full_months = pd.date_range(m["time"].min(), m["time"].max(), freq="MS")
        full = pd.DataFrame({"time": full_months})

        m2 = full.merge(m[["time", "mean_ndvi", "median_ndvi"]], on="time", how="left")

        # Prepare quarter lookup table: quarter -> stats
        q = df_q[["period", "mean_ndvi", "median_ndvi"]].copy()

        def month_to_quarter_label(t: pd.Timestamp) -> str:
            qn = (t.month - 1) // 3 + 1
            return f"{t.year}-Q{qn}"

        q["q_period"] = q["period"].astype(str)
        q = q.rename(columns={"mean_ndvi": "q_mean_ndvi", "median_ndvi": "q_median_ndvi"})[[
            "q_period", "q_mean_ndvi", "q_median_ndvi"
        ]]

        # Map each month to its quarter period
        m2["q_period"] = m2["time"].map(month_to_quarter_label)

        # Join quarter stats and fill missing months
        m2 = m2.merge(q, on="q_period", how="left")

        # Fill only where monthly is missing
        m2["mean_ndvi"] = m2["mean_ndvi"].fillna(m2["q_mean_ndvi"])
        m2["median_ndvi"] = m2["median_ndvi"].fillna(m2["q_median_ndvi"])

        # Drop helper cols
        m2 = m2.drop(columns=["q_mean_ndvi", "q_median_ndvi"])

        # If after fill we still have NaNs, keep them (plot will show gaps) or drop?
        # For your acceptance, it's better to drop rows with no data.
        m2 = m2.dropna(subset=["mean_ndvi", "median_ndvi"]).reset_index(drop=True)

        return m2[["time", "mean_ndvi", "median_ndvi"]]

    @staticmethod
    def _plot(df_ts: pd.DataFrame, out_path: Path, *, series_label: str) -> None:
        import matplotlib.pyplot as plt

        fig = plt.figure(figsize=(10, 4.8))
        ax = fig.add_subplot(1, 1, 1)

        ax.plot(df_ts["time"], df_ts["mean_ndvi"], label="Mean NDVI")
        ax.plot(df_ts["time"], df_ts["median_ndvi"], label="Median NDVI")

        ax.set_title(f"NDVI time series ({series_label})")
        ax.set_xlabel("Time")
        ax.set_ylabel("NDVI")
        ax.grid(True, alpha=0.25)
        ax.legend()

        fig.tight_layout()
        fig.savefig(out_path, dpi=200)
        plt.close(fig)


if __name__ == "__main__":
    BuildNdviTimeSeriesPipeline().run(BuildNdviTimeSeriesParams())
