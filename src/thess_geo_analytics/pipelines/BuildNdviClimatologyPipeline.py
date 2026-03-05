from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, Any, List, Tuple

import numpy as np
import pandas as pd
import rasterio

from thess_geo_analytics.utils.RepoPaths import RepoPaths


# -----------------------
# Regex helpers
# -----------------------
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")
_PERIOD_RE = re.compile(r"^(\d{4})-(\d{2}|Q[1-4])$")


@dataclass(frozen=True)
class BuildNdviClimatologyParams:
    aoi_id: str = "el522"

    in_stats_csv: Path | None = None
    allow_fallback_from_cogs: bool = True

    out_csv: Path | None = None
    out_csv_canonical: Path | None = None
    out_fig: Path | None = None


class BuildNdviClimatologyPipeline:

    # -----------------------
    # Public API
    # -----------------------
    def run(self, params: BuildNdviClimatologyParams) -> Tuple[Path, Path]:

        in_stats_csv = params.in_stats_csv or RepoPaths.table("ndvi_period_stats.csv")
        out_csv = params.out_csv or RepoPaths.table("nvdi_climatology.csv")
        out_csv_canonical = params.out_csv_canonical or RepoPaths.table("ndvi_climatology.csv")
        out_fig = params.out_fig or RepoPaths.figure("ndvi_climatology.png")

        # 1 — load stats
        df_stats = self._load_or_build_period_stats(
            aoi_id=params.aoi_id,
            in_stats_csv=in_stats_csv,
            allow_fallback=params.allow_fallback_from_cogs,
        )

        # 2 — build climatology
        df_clim, mode = self._build_climatology(df_stats, params.aoi_id)

        # 3 — save outputs
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        out_csv_canonical.parent.mkdir(parents=True, exist_ok=True)

        df_clim.to_csv(out_csv, index=False)
        df_clim.to_csv(out_csv_canonical, index=False)

        out_fig.parent.mkdir(parents=True, exist_ok=True)
        self._plot(df_clim, out_fig, mode)

        print(f"[OK] Climatology CSV → {out_csv}")
        print(f"[OK] Climatology CSV (canonical) → {out_csv_canonical}")
        print(f"[OK] Figure → {out_fig}")

        return out_csv, out_fig

    # -----------------------
    # Step 1
    # -----------------------
    def _load_or_build_period_stats(
        self,
        *,
        aoi_id: str,
        in_stats_csv: Path,
        allow_fallback: bool,
    ) -> pd.DataFrame:

        if in_stats_csv.exists():
            print(f"[INFO] Using existing stats table: {in_stats_csv}")
            return pd.read_csv(in_stats_csv)

        if not allow_fallback:
            raise FileNotFoundError(f"Missing stats table: {in_stats_csv}")

        print("[INFO] Falling back to compute stats from NDVI COGs")

        return self._build_period_stats_from_cogs(aoi_id)

    def _build_period_stats_from_cogs(self, aoi_id: str) -> pd.DataFrame:

        cogs_dir = RepoPaths.outputs("cogs")

        if not cogs_dir.exists():
            raise FileNotFoundError(f"NDVI composites directory not found: {cogs_dir}")

        all_tifs = sorted(cogs_dir.glob(f"ndvi_*_{aoi_id}.tif"))

        if not all_tifs:
            raise FileNotFoundError(
                f"No composites found in {cogs_dir} for AOI {aoi_id}"
            )

        rows: List[Dict[str, Any]] = []

        pattern = re.compile(
            rf"^ndvi_"
            r"(\d{4}-(\d{2}|Q[1-4]))"
            rf"_{re.escape(aoi_id)}$"
        )

        for tif_path in all_tifs:

            m = pattern.match(tif_path.stem)
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
            raise RuntimeError("No valid NDVI composites found.")

        df = pd.DataFrame(rows)

        return df.sort_values(["aoi_id", "period"]).reset_index(drop=True)

    # -----------------------
    # Stats helper
    # -----------------------
    @staticmethod
    def _compute_stats_for_tif(tif_path: Path) -> Dict[str, Any]:

        with rasterio.open(tif_path) as ds:
            nd = ds.read(1).astype(np.float32)

            nodata = ds.nodata
            if nodata is not None:
                nd = np.where(nd == nodata, np.nan, nd)

        valid = nd[~np.isnan(nd)]

        if valid.size == 0:
            raise ValueError(f"{tif_path.name} contains no valid pixels")

        return dict(
            mean_ndvi=float(np.mean(valid)),
            median_ndvi=float(np.median(valid)),
            p10_ndvi=float(np.percentile(valid, 10)),
            p90_ndvi=float(np.percentile(valid, 90)),
            std_ndvi=float(np.std(valid)),
            valid_pixel_ratio=float(valid.size / nd.size),
            count_valid_pixels=int(valid.size),
            count_total_pixels=int(nd.size),
        )

    # -----------------------
    # Step 2 — climatology
    # -----------------------
    def _build_climatology(
        self,
        df_stats: pd.DataFrame,
        aoi_id: str,
    ) -> Tuple[pd.DataFrame, str]:

        df = df_stats[df_stats["aoi_id"].astype(str) == str(aoi_id)].copy()

        if df.empty:
            raise RuntimeError(f"No stats rows for AOI {aoi_id}")

        df_month = df[df["period"].str.match(_MONTH_RE)]
        df_q = df[df["period"].str.match(_QUARTER_RE)]

        if not df_month.empty:
            return self._monthly_climatology(df_month), "monthly"

        if not df_q.empty:
            return self._quarterly_climatology(df_q), "quarterly"

        raise RuntimeError("No valid monthly or quarterly stats")

    # -----------------------
    # Monthly climatology
    # -----------------------
    @staticmethod
    def _monthly_climatology(df: pd.DataFrame) -> pd.DataFrame:

        df = df.assign(
            time=pd.to_datetime(df["period"], format="%Y-%m"),
            month_of_year=lambda d: d["time"].dt.month,
        )

        clim = (
            df.groupby("month_of_year", as_index=False)
            .agg(
                mean_ndvi_clim=("mean_ndvi", "mean"),
                median_ndvi_clim=("median_ndvi", "median"),
                n_periods=("mean_ndvi", "size"),
            )
            .sort_values("month_of_year")
        )

        clim["label"] = pd.to_datetime(clim["month_of_year"], format="%m").dt.strftime("%b")

        return clim

    # -----------------------
    # Quarterly climatology
    # -----------------------
    @staticmethod
    def _quarterly_climatology(df: pd.DataFrame) -> pd.DataFrame:

        parts = df["period"].str.split("-", expand=True)

        df = df.assign(
            quarter_of_year=parts[1].str.replace("Q", "").astype(int)
        )

        clim = (
            df.groupby("quarter_of_year", as_index=False)
            .agg(
                mean_ndvi_clim=("mean_ndvi", "mean"),
                median_ndvi_clim=("median_ndvi", "median"),
                n_periods=("mean_ndvi", "size"),
            )
            .sort_values("quarter_of_year")
        )

        clim["label"] = "Q" + clim["quarter_of_year"].astype(str)

        return clim

    # -----------------------
    # Plot
    # -----------------------
    @staticmethod
    def _plot(df: pd.DataFrame, out_path: Path, mode: str):

        import matplotlib.pyplot as plt

        fig = plt.figure(figsize=(9, 4.5))
        ax = fig.add_subplot(1, 1, 1)

        if mode == "monthly":
            x = df["month_of_year"]
            ax.set_xticks(x)
            ax.set_xticklabels(df["label"])
            ax.set_xlabel("Month")
        else:
            x = df["quarter_of_year"]
            ax.set_xticks(x)
            ax.set_xticklabels(df["label"])
            ax.set_xlabel("Quarter")

        ax.plot(x, df["mean_ndvi_clim"], marker="o", label="Mean NDVI")
        ax.plot(x, df["median_ndvi_clim"], marker="o", label="Median NDVI")

        ax.set_ylabel("NDVI")
        ax.set_title("NDVI climatology")
        ax.grid(True, alpha=0.25)
        ax.legend()

        fig.tight_layout()
        fig.savefig(out_path, dpi=200)
        plt.close(fig)