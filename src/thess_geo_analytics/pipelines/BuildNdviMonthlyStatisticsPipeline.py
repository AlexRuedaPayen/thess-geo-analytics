from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import List, Tuple, Dict, Any, Optional

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
    Parameters for NDVI monthly statistics.

    - stats_csv: where to write the per-period stats table
    - out_parquet/out_parquet_canonical/out_fig: timeseries outputs
    """
    aoi_id: str = "el522"

    stats_csv: Optional[Path] = None
    fill_missing_months_from_quarters: bool = True

    out_parquet: Optional[Path] = None
    out_parquet_canonical: Optional[Path] = None
    out_fig: Optional[Path] = None


class BuildNdviMonthlyStatisticsPipeline:
    """
    ndvi_<period>_<aoi>.tif in outputs/cogs/
      -> outputs/tables/ndvi_period_stats.csv
      -> outputs/tables/nvdi_timeseries.parquet + ndvi_timeseries.parquet
      -> outputs/figures/ndvi_timeseries.png
    """

    # -----------------------
    # Public API
    # -----------------------
    def run(self, params: BuildNdviMonthlyStatisticsParams) -> Tuple[Path, Path]:
        # Resolve defaults WITHOUT mutating params (params is frozen)
        stats_csv = params.stats_csv or RepoPaths.table("ndvi_period_stats.csv")
        out_parquet = params.out_parquet or RepoPaths.table("nvdi_timeseries.parquet")
        out_parquet_canonical = params.out_parquet_canonical or RepoPaths.table("ndvi_timeseries.parquet")
        out_fig = params.out_fig or RepoPaths.figure("ndvi_timeseries.png")

        # 1) Build per-period stats for all existing composites
        df_stats = self._build_period_stats_for_all_existing(
            aoi_id=params.aoi_id,
            stats_csv=stats_csv,
        )

        # 2) Build monthly time series (with optional quarterly fill) + plot
        df_ts = self._build_time_series_from_stats(
            df_stats=df_stats,
            aoi_id=params.aoi_id,
            fill_missing=params.fill_missing_months_from_quarters,
        )

        # Ensure output dirs exist
        out_parquet.parent.mkdir(parents=True, exist_ok=True)
        out_parquet_canonical.parent.mkdir(parents=True, exist_ok=True)
        out_fig.parent.mkdir(parents=True, exist_ok=True)

        # Save Parquet (legacy + canonical)
        df_ts.to_parquet(out_parquet, index=False)
        df_ts.to_parquet(out_parquet_canonical, index=False)

        # Plot
        self._plot_time_series(df_ts, out_fig, series_label="monthly NDVI")

        print(f"[OK] Period stats CSV              → {stats_csv}")
        print(f"[OK] Time series Parquet (legacy) → {out_parquet}")
        print(f"[OK] Time series Parquet          → {out_parquet_canonical}")
        print(f"[OK] Time series figure           → {out_fig}")

        return out_parquet, out_fig

    # -----------------------
    # Paths
    # -----------------------
    @staticmethod
    def _cogs_dir() -> Path:
        # IMPORTANT: must be under THESS_RUN_ROOT for integration tests
        return RepoPaths.run_root() / "outputs" / "cogs"

    # -----------------------
    # Step 1: period stats
    # -----------------------
    def _build_period_stats_for_all_existing(
        self,
        *,
        aoi_id: str,
        stats_csv: Path,
    ) -> pd.DataFrame:
        cogs_dir = self._cogs_dir()

        if not cogs_dir.exists():
            raise FileNotFoundError(f"NDVI composites directory not found: {cogs_dir}")

        all_tifs = sorted(cogs_dir.glob(f"ndvi_*_{aoi_id}.tif"))
        if not all_tifs:
            raise FileNotFoundError(
                f"No ndvi_*_{aoi_id}.tif files found in {cogs_dir}. "
                f"Expected base composites like ndvi_<period>_{aoi_id}.tif."
            )

        # Accept only: ndvi_<YYYY-MM|YYYY-Qn>_<aoi_id>.tif
        pattern = re.compile(
            rf"^ndvi_"
            r"(\d{4}-(\d{2}|Q[1-4]))"
            rf"_{re.escape(aoi_id)}$"
        )

        rows: List[Dict[str, Any]] = []

        for tif_path in all_tifs:
            stem = tif_path.stem
            m = pattern.match(stem)
            if not m:
                continue

            period = m.group(1)
            if not _PERIOD_RE.match(period):
                continue

            stats = self._compute_stats_for_tif(tif_path)
            stats["period"] = period
            stats["aoi_id"] = aoi_id
            stats["tif_path"] = str(tif_path)
            rows.append(stats)

        if not rows:
            raise RuntimeError(
                f"Found ndvi_*_{aoi_id}.tif files, but none matched "
                "ndvi_<YYYY-MM|YYYY-Qn>_<aoi_id>.tif. "
                "Are you only producing anomaly/climatology products?"
            )

        df = pd.DataFrame(rows).sort_values(["aoi_id", "period"]).reset_index(drop=True)

        stats_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(stats_csv, index=False)
        print(f"[OK] NDVI period stats written → {stats_csv}")

        return df

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
            raise ValueError(f"{tif_path.name} contains no valid pixels — cannot compute stats.")

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
        *,
        df_stats: pd.DataFrame,
        aoi_id: str,
        fill_missing: bool,
    ) -> pd.DataFrame:
        required = {"period", "aoi_id", "mean_ndvi", "median_ndvi"}
        missing = required - set(df_stats.columns)
        if missing:
            raise ValueError(
                f"ndvi_period_stats missing columns: {sorted(missing)} "
                f"(available: {sorted(df_stats.columns)})"
            )

        df = df_stats[df_stats["aoi_id"].astype(str) == str(aoi_id)].copy()
        if df.empty:
            raise RuntimeError(f"No rows found in ndvi_period_stats for aoi_id={aoi_id}.")

        df_month = df[df["period"].astype(str).str.match(_MONTH_RE, na=False)].copy()
        df_q = df[df["period"].astype(str).str.match(_QUARTER_RE, na=False)].copy()

        if df_month.empty and df_q.empty:
            raise RuntimeError("No monthly OR quarterly rows found in ndvi_period_stats for this AOI.")

        if not df_month.empty:
            df_ts = self._build_monthly_series_with_optional_quarter_fill(
                df_month=df_month,
                df_q=df_q,
                fill_missing=fill_missing,
            )
        else:
            df_ts = self._build_quarterly_series(df_q)

        if df_ts.empty:
            raise RuntimeError("Time series construction produced an empty dataframe.")

        return df_ts

    @staticmethod
    def _build_quarterly_series(df_q: pd.DataFrame) -> pd.DataFrame:
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
        *,
        df_month: pd.DataFrame,
        df_q: pd.DataFrame,
        fill_missing: bool,
    ) -> pd.DataFrame:
        m = (
            df_month[["period", "mean_ndvi", "median_ndvi"]]
            .rename(columns={"period": "month"})
            .assign(time=lambda d: pd.to_datetime(d["month"], format="%Y-%m"))
            .sort_values("time")
            .reset_index(drop=True)
        )

        if (not fill_missing) or df_q.empty:
            return m[["time", "mean_ndvi", "median_ndvi"]]

        full_months = pd.date_range(m["time"].min(), m["time"].max(), freq="MS")
        full = pd.DataFrame({"time": full_months})
        m2 = full.merge(m[["time", "mean_ndvi", "median_ndvi"]], on="time", how="left")

        q = df_q[["period", "mean_ndvi", "median_ndvi"]].copy()

        def month_to_quarter_label(t: pd.Timestamp) -> str:
            qn = (t.month - 1) // 3 + 1
            return f"{t.year}-Q{qn}"

        q = q.rename(columns={"period": "q_period", "mean_ndvi": "q_mean_ndvi", "median_ndvi": "q_median_ndvi"})
        m2["q_period"] = m2["time"].map(month_to_quarter_label)
        m2 = m2.merge(q[["q_period", "q_mean_ndvi", "q_median_ndvi"]], on="q_period", how="left")

        m2["mean_ndvi"] = m2["mean_ndvi"].fillna(m2["q_mean_ndvi"])
        m2["median_ndvi"] = m2["median_ndvi"].fillna(m2["q_median_ndvi"])

        m2 = m2.drop(columns=["q_mean_ndvi", "q_median_ndvi"])
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