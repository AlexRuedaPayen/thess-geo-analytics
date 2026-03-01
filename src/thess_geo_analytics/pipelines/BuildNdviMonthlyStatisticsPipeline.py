from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import List, Tuple, Dict, Any

import numpy as np
import pandas as pd
import rasterio

from thess_geo_analytics.utils.RepoPaths import RepoPaths


# -----------------------
# Regex helpers
# -----------------------
_PERIOD_RE = re.compile(r"^(\d{4})-(\d{2}|Q[1-4])$")  # YYYY-MM or YYYY-Qn
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")              # YYYY-MM
_QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")           # YYYY-Qn


@dataclass(frozen=True)
class BuildNdviMonthlyStatisticsParams:
    """
    High-level parameters for NDVI monthly statistics.

    This pipeline does two things in one pass:

      1) Computes per-period NDVI stats for every composite in:
         <cogs_dir>/ndvi_<period>_<aoi_id>.tif
         where <period> is YYYY-MM or YYYY-Qn.

         → writes ndvi_period_stats.csv

      2) Builds a continuous NDVI time series (monthly) with optional
         fallback to quarterly stats for missing months, and plots it.

         → writes:
              - nvdi_timeseries.parquet        (legacy spelling)
              - ndvi_timeseries.parquet        (canonical)
              - ndvi_timeseries.png
    """
    aoi_id: str = "el522"

    # Period stats table
    stats_csv: Path = RepoPaths.table("ndvi_period_stats.csv")

    # If monthly stats exist but some months are missing,
    # fill those missing months with the corresponding quarterly stats.
    fill_missing_months_from_quarters: bool = True

    # Time-series outputs (kept compatible with previous pipelines)
    out_parquet: Path = RepoPaths.table("nvdi_timeseries.parquet")
    out_parquet_canonical: Path = RepoPaths.table("ndvi_timeseries.parquet")
    out_fig: Path = RepoPaths.figure("ndvi_timeseries.png")


class BuildNdviMonthlyStatisticsPipeline:
    """
    Single, self-contained pipeline:

      ndvi_<period>_<aoi>.tif  →  ndvi_period_stats.csv
                                →  ndvi_timeseries.parquet + PNG plot
    """

    # -----------------------
    # Public API
    # -----------------------
    def run(self, params: BuildNdviMonthlyStatisticsParams) -> Tuple[Path, Path]:
        # 1) Build per-period stats for all existing composites
        df_stats = self._build_period_stats_for_all_existing(params)

        # 2) Build monthly time series (with optional quarterly fill) + plot
        df_ts = self._build_time_series_from_stats(df_stats, params)

        # Ensure output dirs exist
        params.out_parquet.parent.mkdir(parents=True, exist_ok=True)
        params.out_parquet_canonical.parent.mkdir(parents=True, exist_ok=True)
        params.out_fig.parent.mkdir(parents=True, exist_ok=True)

        # Save Parquet (legacy + canonical)
        df_ts.to_parquet(params.out_parquet, index=False)
        df_ts.to_parquet(params.out_parquet_canonical, index=False)

        # Plot
        self._plot_time_series(df_ts, params.out_fig, series_label="monthly NDVI")

        print(f"[OK] Period stats CSV        → {params.stats_csv}")
        print(f"[OK] Time series Parquet     → {params.out_parquet}")
        print(f"[OK] Time series Parquet (canonical) → {params.out_parquet_canonical}")
        print(f"[OK] Time series figure      → {params.out_fig}")

        return params.out_parquet, params.out_fig

    @staticmethod
    def smoke_test() -> None:
        print("=== BuildNdviMonthlyStatisticsPipeline Smoke Test ===")
        print("This pipeline expects:")
        print("  - ndvi_<period>_<aoi_id>.tif composites in outputs/cogs/")
        print("  - period naming: YYYY-MM or YYYY-Qn (e.g. 2025-08, 2025-Q3)")
        print("✓ Smoke test OK (orchestration only, no data checked).")

    # -----------------------
    # Internal helpers
    # -----------------------
    @staticmethod
    def _cogs_dir() -> Path:
        """
        Single place to determine where NDVI composites live.

        Tests can override this by monkeypatching RepoPaths or this helper.
        """
        return RepoPaths.OUTPUTS / "cogs"

    # -----------------------
    # Step 1: period stats
    # -----------------------
    def _build_period_stats_for_all_existing(
        self,
        params: BuildNdviMonthlyStatisticsParams,
    ) -> pd.DataFrame:
        cogs_dir = self._cogs_dir()
        cogs_dir.mkdir(parents=True, exist_ok=True)

        tifs = sorted(cogs_dir.glob(f"ndvi_*_{params.aoi_id}.tif"))
        if not tifs:
            raise FileNotFoundError(
                f"No composites found in {cogs_dir} for aoi_id={params.aoi_id} "
                f"(expected ndvi_<period>_{params.aoi_id}.tif)."
            )

        rows: List[Dict[str, Any]] = []

        for tif_path in tifs:
            period = self._extract_period_from_stem(tif_path.stem)
            if period is None:
                # Skip files that don't match naming convention
                continue

            self._validate_period(period)

            stats = self._compute_stats_for_tif(tif_path)
            stats["period"] = period
            stats["aoi_id"] = params.aoi_id
            stats["tif_path"] = str(tif_path)

            rows.append(stats)

        if not rows:
            raise RuntimeError(
                "Found composites but could not extract any valid periods "
                "from filenames (ndvi_<period>_<aoi>.tif)."
            )

        df = pd.DataFrame(rows)

        # Stable sort: first by aoi, then period
        df = df.sort_values(["aoi_id", "period"]).reset_index(drop=True)

        # Save CSV
        params.stats_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(params.stats_csv, index=False)
        print(f"[OK] NDVI period stats written → {params.stats_csv}")

        return df

    @staticmethod
    def _extract_period_from_stem(stem: str) -> str | None:
        """
        Extract <period> from a stem of the form: ndvi_<period>_<aoi_id>
        """
        parts = stem.split("_")
        if len(parts) < 3:
            return None
        return parts[1]

    @staticmethod
    def _validate_period(period: str) -> None:
        if not _PERIOD_RE.match(period):
            raise ValueError(
                f"Invalid period '{period}'. Expected 'YYYY-MM' or 'YYYY-Qn' "
                f"(e.g. 2025-08, 2025-Q3)."
            )

    @staticmethod
    def _compute_stats_for_tif(tif_path: Path) -> Dict[str, Any]:
        with rasterio.open(tif_path) as ds:
            nd = ds.read(1).astype(np.float32)
            nodata = ds.nodata
            if nodata is not None:
                nd = np.where(nd == nodata, np.nan, nd)

        valid_mask = ~np.isnan(nd)
        valid_count = int(valid_mask.sum())
        total_count = int(nd.size)

        if valid_count == 0:
            raise ValueError(
                f"{tif_path.name} contains no valid pixels — cannot compute stats."
            )

        valid = nd[valid_mask]

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

    # -----------------------
    # Step 2: time series
    # -----------------------
    def _build_time_series_from_stats(
        self,
        df_stats: pd.DataFrame,
        params: BuildNdviMonthlyStatisticsParams,
    ) -> pd.DataFrame:
        # Ensure required columns
        required = {"period", "aoi_id", "mean_ndvi", "median_ndvi"}
        missing = required - set(df_stats.columns)
        if missing:
            raise ValueError(
                f"ndvi_period_stats is missing columns: {sorted(missing)}. "
                f"Available: {sorted(df_stats.columns)}"
            )

        # Filter AOI
        df = df_stats[df_stats["aoi_id"].astype(str) == str(params.aoi_id)].copy()
        if df.empty:
            raise RuntimeError(
                f"No rows found in ndvi_period_stats for aoi_id={params.aoi_id}."
            )

        # Split monthly vs quarterly
        df_month = df[df["period"].astype(str).str.match(_MONTH_RE, na=False)].copy()
        df_q = df[df["period"].astype(str).str.match(_QUARTER_RE, na=False)].copy()

        if df_month.empty and df_q.empty:
            raise RuntimeError(
                "No monthly OR quarterly rows found in ndvi_period_stats "
                "for this AOI."
            )

        if not df_month.empty:
            df_ts = self._build_monthly_series_with_optional_quarter_fill(
                df_month=df_month,
                df_q=df_q,
                fill_missing=params.fill_missing_months_from_quarters,
            )
        else:
            # Fallback: use quarterly series directly
            df_ts = self._build_quarterly_series(df_q)

        if df_ts.empty:
            raise RuntimeError("Time series construction produced an empty dataframe.")

        return df_ts

    @staticmethod
    def _build_quarterly_series(df_q: pd.DataFrame) -> pd.DataFrame:
        """
        Convert quarterly periods (YYYY-Qn) to a time series where each record
        is placed at the quarter start date:

            Q1 → Jan 1, Q2 → Apr 1, Q3 → Jul 1, Q4 → Oct 1
        """
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
        """
        Build a monthly NDVI time series:

          - Base: monthly periods from df_month
          - If fill_missing is True and quarterly stats exist:
              fill missing months with the corresponding quarter's stats.
        """
        # Base monthly table
        m = (
            df_month[["period", "mean_ndvi", "median_ndvi"]]
            .rename(columns={"period": "month"})
            .assign(time=lambda d: pd.to_datetime(d["month"], format="%Y-%m"))
            .sort_values("time")
            .reset_index(drop=True)
        )

        if (not fill_missing) or df_q.empty:
            return m[["time", "mean_ndvi", "median_ndvi"]]

        # Full range of monthly timestamps from min to max observed
        full_months = pd.date_range(m["time"].min(), m["time"].max(), freq="MS")
        full = pd.DataFrame({"time": full_months})

        m2 = full.merge(m[["time", "mean_ndvi", "median_ndvi"]], on="time", how="left")

        # Quarter lookup
        q = df_q[["period", "mean_ndvi", "median_ndvi"]].copy()

        def month_to_quarter_label(t: pd.Timestamp) -> str:
            qn = (t.month - 1) // 3 + 1
            return f"{t.year}-Q{qn}"

        q["q_period"] = q["period"].astype(str)
        q = q.rename(
            columns={
                "mean_ndvi": "q_mean_ndvi",
                "median_ndvi": "q_median_ndvi",
            }
        )[["q_period", "q_mean_ndvi", "q_median_ndvi"]]

        # Map each month to its quarter
        m2["q_period"] = m2["time"].map(month_to_quarter_label)

        # Join quarter stats
        m2 = m2.merge(q, on="q_period", how="left")

        # Fill only where monthly is missing
        m2["mean_ndvi"] = m2["mean_ndvi"].fillna(m2["q_mean_ndvi"])
        m2["median_ndvi"] = m2["median_ndvi"].fillna(m2["q_median_ndvi"])

        # Drop helper columns
        m2 = m2.drop(columns=["q_mean_ndvi", "q_median_ndvi"])

        # Drop months that still have no data after fill
        m2 = m2.dropna(subset=["mean_ndvi", "median_ndvi"]).reset_index(drop=True)

        return m2[["time", "mean_ndvi", "median_ndvi"]]

    # -----------------------
    # Plotting
    # -----------------------
    @staticmethod
    def _plot_time_series(df_ts: pd.DataFrame, out_path: Path, *, series_label: str) -> None:
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