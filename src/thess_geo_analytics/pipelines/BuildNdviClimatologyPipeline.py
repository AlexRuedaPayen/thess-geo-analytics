from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, Any, List

import numpy as np
import pandas as pd
import rasterio

from thess_geo_analytics.utils.RepoPaths import RepoPaths


# -----------------------
# Regex helpers
# -----------------------
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")         # YYYY-MM
_QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")      # YYYY-Qn
_PERIOD_RE = re.compile(r"^(\d{4})-(\d{2}|Q[1-4])$")  # YYYY-MM or YYYY-Qn


@dataclass(frozen=True)
class BuildNdviClimatologyParams:
    """
    High-level parameters for NDVI climatology.

    This pipeline can:

      • Prefer using a precomputed per-period stats table:
          outputs/tables/ndvi_period_stats.csv

      • If that table is missing, it will directly scan:
          outputs/cogs/ndvi_<period>_<aoi_id>.tif

        to derive the stats needed to build a climatology.

    Outputs:
      • outputs/tables/nvdi_climatology.csv        (legacy spelling)
      • outputs/tables/ndvi_climatology.csv        (canonical)
      • outputs/figures/ndvi_climatology.png       (seasonal curve)
    """

    aoi_id: str = "el522"

    # Preferred source: stats table (usually produced by MonthlyStatistics pipeline)
    in_stats_csv: Path = RepoPaths.table("ndvi_period_stats.csv")

    # If stats CSV is missing, allow direct read from NDVI composites
    allow_fallback_from_cogs: bool = True

    # Ticket output spelling
    out_csv: Path = RepoPaths.table("nvdi_climatology.csv")
    # Optional canonical copy
    out_csv_canonical: Path = RepoPaths.table("ndvi_climatology.csv")

    out_fig: Path = RepoPaths.figure("ndvi_climatology.png")


class BuildNdviClimatologyPipeline:
    """
    Self-contained NDVI climatology pipeline:

      EITHER:
        ndvi_period_stats.csv
          → monthly or quarterly climatology
          → CSV(s) + PNG

      OR (fallback if CSV missing and allow_fallback_from_cogs=True):
        ndvi_<period>_<aoi>.tif in outputs/cogs
          → internal per-period stats
          → climatology
          → CSV(s) + PNG
    """

    # -----------------------
    # Public API
    # -----------------------
    def run(self, params: BuildNdviClimatologyParams) -> tuple[Path, Path]:
        # 1) Load or derive per-period stats
        df_stats = self._load_or_build_period_stats(params)

        # 2) Filter AOI and compute climatology (monthly or quarterly)
        df_clim, mode = self._build_climatology(df_stats, params)

        # 3) Save CSVs
        params.out_csv.parent.mkdir(parents=True, exist_ok=True)
        df_clim.to_csv(params.out_csv, index=False)
        df_clim.to_csv(params.out_csv_canonical, index=False)

        # 4) Plot
        params.out_fig.parent.mkdir(parents=True, exist_ok=True)
        self._plot(df_clim, params.out_fig, mode=mode)

        print(f"[OK] Climatology CSV           → {params.out_csv}")
        print(f"[OK] Climatology CSV (canonical) → {params.out_csv_canonical}")
        print(f"[OK] Figure                    → {params.out_fig}")
        return params.out_csv, params.out_fig

    @staticmethod
    def smoke_test() -> None:
        print("=== BuildNdviClimatologyPipeline Smoke Test ===")
        print("Will use ndvi_period_stats.csv if present;")
        print("otherwise, can fall back to outputs/cogs/ndvi_<period>_<aoi>.tif")
        print("to derive per-period stats before building climatology.")
        print("✓ Smoke test OK (orchestration only, no data checked).")

    # -----------------------
    # Step 1: load or build per-period stats
    # -----------------------
    def _load_or_build_period_stats(
        self,
        params: BuildNdviClimatologyParams,
    ) -> pd.DataFrame:
        if params.in_stats_csv.exists():
            print(f"[INFO] Using existing stats table: {params.in_stats_csv}")
            df = pd.read_csv(params.in_stats_csv)
            return df

        if not params.allow_fallback_from_cogs:
            raise FileNotFoundError(
                f"Missing stats table: {params.in_stats_csv} and "
                f"fallback from cogs has been disabled."
            )

        print(
            f"[INFO] Stats table not found: {params.in_stats_csv}\n"
            f"[INFO] Falling back to compute per-period stats from NDVI composites in outputs/cogs."
        )
        return self._build_period_stats_from_cogs(params)

    def _build_period_stats_from_cogs(
        self,
        params: BuildNdviClimatologyParams,
    ) -> pd.DataFrame:
        """
        Scan outputs/cogs for ndvi_<period>_<aoi_id>.tif and compute the
        same per-period stats that would normally live in ndvi_period_stats.csv.
        """
        cogs_dir = RepoPaths.OUTPUTS / "cogs"
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

        print("[INFO] Built per-period stats directly from cogs (not saved to CSV).")
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
    # Step 2: build climatology
    # -----------------------
    def _build_climatology(
        self,
        df_stats: pd.DataFrame,
        params: BuildNdviClimatologyParams,
    ) -> tuple[pd.DataFrame, str]:
        required = {"period", "aoi_id", "mean_ndvi", "median_ndvi"}
        missing = required - set(df_stats.columns)
        if missing:
            raise ValueError(
                f"Stats table missing columns: {sorted(missing)}. "
                f"Available: {sorted(df_stats.columns)}"
            )

        # Filter AOI
        df = df_stats[df_stats["aoi_id"].astype(str) == str(params.aoi_id)].copy()

        if df.empty:
            raise RuntimeError(
                f"No rows found in stats data for aoi_id={params.aoi_id}."
            )

        # Split monthly vs quarterly
        df_month = df[df["period"].astype(str).str.match(_MONTH_RE, na=False)].copy()
        df_q = df[df["period"].astype(str).str.match(_QUARTER_RE, na=False)].copy()

        if df_month.empty and df_q.empty:
            raise RuntimeError(
                "No monthly OR quarterly rows found in stats data for this AOI."
            )

        if not df_month.empty:
            clim, mode = self._monthly_climatology(df_month)
        else:
            clim, mode = self._quarterly_climatology(df_q)

        return clim, mode

    @staticmethod
    def _monthly_climatology(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
        """
        Build a calendar-month NDVI climatology:

          • For each month-of-year (1..12), aggregate all matching
            periods (YYYY-MM) across years.
        """
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

        # Human-friendly labels: Jan, Feb, ...
        clim["label"] = pd.to_datetime(clim["month_of_year"], format="%m").dt.strftime("%b")
        return clim, "monthly"

    @staticmethod
    def _quarterly_climatology(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
        """
        Build a calendar-quarter NDVI climatology:

          • For each quarter-of-year (1..4), aggregate all matching
            periods (YYYY-Qn) across years.
        """
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

            # Use the actual months present in the climatology,
            # not a hard-coded 1..12
            ax.set_xticks(clim["month_of_year"])
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
    BuildNdviClimatologyPipeline.smoke_test()