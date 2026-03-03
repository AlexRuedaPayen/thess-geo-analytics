from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re

import numpy as np
import rasterio
from rasterio.enums import Resampling

from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class BuildNdviAnomalyMapsParams:
    """
    Parameters for building pixel-wise NDVI anomaly rasters.

    We support *both* monthly and quarterly NDVI composites:

      - ndvi_YYYY-MM_<aoi_id>.tif
      - ndvi_YYYY-Qn_<aoi_id>.tif

    For each "period-of-year" we compute a per-pixel climatology:

      - monthly:  month_of_year in [1..12]
      - quarterly: quarter_of_year in [1..4]

    and then anomalies:

      anomaly(period) = ndvi(period) - climatology(period_of_year)
    """

    aoi_id: str = "el522"

    # Directory with NDVI composites (both monthly and quarterly)
    cogs_dir: Path = RepoPaths.OUTPUTS / "cogs"

    # Limit years used in climatology / anomaly
    year_start: Optional[int] = None  # inclusive, optional
    year_end: Optional[int] = None    # inclusive, optional

    # nodata in NDVI COGs (fallback if not defined in metadata)
    nodata: float = -9999.0

    # Minimum number of distinct years before we "trust" climatology
    min_years_for_climatology: int = 3

    # If False and climatology tifs already exist, re-use them.
    recompute_climatology: bool = False

    verbose: bool = False


class BuildNdviAnomalyMapsPipeline:
    """
    Build NDVI anomaly maps from NDVI composites.

    Inputs (in outputs/cogs/):
      - ndvi_YYYY-MM_<aoi_id>.tif     (monthly composites)
      - ndvi_YYYY-Qn_<aoi_id>.tif     (quarterly composites)

    Outputs (also in outputs/cogs/):
      - ndvi_climatology_median_MM_<aoi_id>.tif          (monthly climatology)
      - ndvi_climatology_median_Qn_<aoi_id>.tif          (quarterly climatology)
      - ndvi_anomaly_YYYY-MM_<aoi_id>.tif                (monthly anomalies)
      - ndvi_anomaly_YYYY-Qn_<aoi_id>.tif                (quarterly anomalies)

    plus PNG previews in outputs/figures/:
      - ndvi_anomaly_<period>_<aoi_id}_preview.png
    """

    # NOTE: double braces {{ }} so that .format only replaces {aoi}
    _MONTHLY_RE_TEMPLATE = r"^ndvi_(\d{{4}})-(\d{{2}})_{aoi}\.tif$"
    _QUARTERLY_RE_TEMPLATE = r"^ndvi_(\d{{4}})-(Q[1-4])_{aoi}\.tif$"

    def run(self, params: BuildNdviAnomalyMapsParams) -> list[tuple[str, Path, Path]]:
        if not params.cogs_dir.exists():
            raise FileNotFoundError(f"COGs directory not found: {params.cogs_dir}")

        # 1) Discover monthly and quarterly composites
        monthly, quarterly = self._discover_composites(params)

        if not monthly and not quarterly:
            raise RuntimeError(
                f"No NDVI composites (monthly or quarterly) found under {params.cogs_dir} "
                f"for AOI {params.aoi_id}."
            )

        if params.verbose:
            print(f"[INFO] Monthly composites found:   {len(monthly)}")
            print(f"[INFO] Quarterly composites found: {len(quarterly)}")

        # 2) Build / load climatologies
        clim_month = self._build_or_load_monthly_climatology(monthly, params)
        clim_quarter = self._build_or_load_quarterly_climatology(quarterly, params)

        # 3) Build anomalies for each composite
        results: list[tuple[str, Path, Path]] = []

        # Monthly anomalies
        for label, (year, month, tif_path) in sorted(monthly.items()):
            clim_tif = clim_month.get(month)
            if clim_tif is None:
                if params.verbose:
                    print(f"[WARN] No monthly climatology for month={month:02d}, skipping {label}.")
                continue

            anom_tif, anom_png = self._build_anomaly_for_period(
                label=label,
                comp_path=tif_path,
                clim_path=clim_tif,
                params=params,
            )
            results.append((label, anom_tif, anom_png))

        # Quarterly anomalies
        for label, (year, quarter, tif_path) in sorted(quarterly.items()):
            clim_tif = clim_quarter.get(quarter)
            if clim_tif is None:
                if params.verbose:
                    print(f"[WARN] No quarterly climatology for Q{quarter}, skipping {label}.")
                continue

            anom_tif, anom_png = self._build_anomaly_for_period(
                label=label,
                comp_path=tif_path,
                clim_path=clim_tif,
                params=params,
            )
            results.append((label, anom_tif, anom_png))

        if params.verbose:
            print(f"[OK] total anomaly rasters produced: {len(results)}")

        return results

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------
    def _discover_composites(
        self,
        params: BuildNdviAnomalyMapsParams,
    ) -> tuple[
        Dict[str, Tuple[int, int, Path]],   # monthly: label -> (year, month, path)
        Dict[str, Tuple[int, int, Path]],   # quarterly: label -> (year, quarter, path)
    ]:
        """
        Scan cogs_dir for files of the form:
          - ndvi_YYYY-MM_<aoi>.tif
          - ndvi_YYYY-Qn_<aoi>.tif

        Returns:
          monthly  : { "YYYY-MM": (year, month, path), ... }
          quarterly: { "YYYY-Qn": (year, quarter, path), ... }
        """
        monthly: Dict[str, Tuple[int, int, Path]] = {}
        quarterly: Dict[str, Tuple[int, int, Path]] = {}

        monthly_re = re.compile(
            self._MONTHLY_RE_TEMPLATE.format(aoi=re.escape(params.aoi_id))
        )
        quarterly_re = re.compile(
            self._QUARTERLY_RE_TEMPLATE.format(aoi=re.escape(params.aoi_id))
        )

        for p in params.cogs_dir.glob("ndvi_*.tif"):
            name = p.name

            m_m = monthly_re.match(name)
            if m_m:
                year = int(m_m.group(1))
                month = int(m_m.group(2))

                if params.year_start is not None and year < params.year_start:
                    continue
                if params.year_end is not None and year > params.year_end:
                    continue

                label = f"{year:04d}-{month:02d}"
                monthly[label] = (year, month, p)
                continue

            m_q = quarterly_re.match(name)
            if m_q:
                year = int(m_q.group(1))
                q_label = m_q.group(2)  # "Q1".."Q4"
                quarter = int(q_label[1])

                if params.year_start is not None and year < params.year_start:
                    continue
                if params.year_end is not None and year > params.year_end:
                    continue

                label = f"{year:04d}-{q_label}"
                quarterly[label] = (year, quarter, p)
                continue

        return monthly, quarterly

    # ------------------------------------------------------------------
    # Climatology helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _climatology_tif_for_month(month: int, params: BuildNdviAnomalyMapsParams) -> Path:
        return params.cogs_dir / f"ndvi_climatology_median_{month:02d}_{params.aoi_id}.tif"

    @staticmethod
    def _climatology_tif_for_quarter(quarter: int, params: BuildNdviAnomalyMapsParams) -> Path:
        return params.cogs_dir / f"ndvi_climatology_median_Q{quarter}_{params.aoi_id}.tif"

    def _build_or_load_monthly_climatology(
        self,
        monthly: Dict[str, Tuple[int, int, Path]],
        params: BuildNdviAnomalyMapsParams,
    ) -> Dict[int, Path]:
        """
        For each month-of-year m, compute or load:
          median over all years of NDVI(YYYY-m) per pixel.

        Returns:
          { month_of_year: Path_to_climatology_tif, ... }
        """
        climatology: Dict[int, Path] = {}

        # Reuse existing files if allowed
        if not params.recompute_climatology:
            for m in range(1, 13):
                tif = self._climatology_tif_for_month(m, params)
                if tif.exists():
                    climatology[m] = tif

        # Collect paths + years by month-of-year
        month_paths: Dict[int, List[Path]] = {}
        month_years: Dict[int, List[int]] = {}

        for label, (year, month, tif_path) in monthly.items():
            month_paths.setdefault(month, []).append(tif_path)
            month_years.setdefault(month, []).append(year)

        # Build climatology per month, block-wise
        for m, paths in month_paths.items():
            if m in climatology and not params.recompute_climatology:
                # already have GeoTIFF on disk
                continue

            years = month_years[m]
            n_years = len(set(years))
            if n_years < params.min_years_for_climatology and params.verbose:
                print(
                    f"[WARN] month={m:02d}: only {n_years} year(s) of data. "
                    "Pixel-wise climatology may be noisy."
                )

            template_path = paths[0]
            out_tif = self._climatology_tif_for_month(m, params)
            out_tif.parent.mkdir(parents=True, exist_ok=True)

            # Open datasets for all paths for this month
            datasets = [rasterio.open(p) for p in paths]
            try:
                # Assume all have identical grid; use first as template
                with rasterio.open(template_path) as tmpl:
                    profile = tmpl.profile.copy()

                profile.update(
                    dtype="float32",
                    count=1,
                    nodata=params.nodata,
                    tiled=True,
                    compress="deflate",
                )

                with rasterio.open(out_tif, "w", **profile) as dst:
                    for (ji, window) in dst.block_windows(1):
                        h, w = window.height, window.width
                        block_stack: List[np.ndarray] = []

                        for ds in datasets:
                            arr = ds.read(1, window=window).astype(np.float32)
                            nodata = ds.nodata if ds.nodata is not None else params.nodata
                            arr = np.where(arr == nodata, np.nan, arr)
                            block_stack.append(arr)

                        stack = np.stack(block_stack, axis=0)  # (time, h, w)
                        clim_block = np.nanmedian(stack, axis=0).astype(np.float32)

                        out_block = np.where(
                            np.isnan(clim_block),
                            params.nodata,
                            clim_block,
                        ).astype(np.float32)

                        dst.write(out_block, 1, window=window)

                    dst.build_overviews([2, 4, 8, 16], Resampling.nearest)
                    dst.update_tags(ns="rio_overview", resampling="nearest")

                climatology[m] = out_tif

                if params.verbose:
                    print(f"[OK] monthly climatology month={m:02d} → {out_tif}")
            finally:
                for ds in datasets:
                    ds.close()

        return climatology

    def _build_or_load_quarterly_climatology(
        self,
        quarterly: Dict[str, Tuple[int, int, Path]],
        params: BuildNdviAnomalyMapsParams,
    ) -> Dict[int, Path]:
        """
        For each quarter-of-year q, compute or load:
          median over all years of NDVI(YYYY-Qq) per pixel.

        Returns:
          { quarter_of_year: Path_to_climatology_tif, ... }
        """
        climatology: Dict[int, Path] = {}

        if not params.recompute_climatology:
            for q in range(1, 5):
                tif = self._climatology_tif_for_quarter(q, params)
                if tif.exists():
                    climatology[q] = tif

        quarter_paths: Dict[int, List[Path]] = {}
        quarter_years: Dict[int, List[int]] = {}

        for label, (year, quarter, tif_path) in quarterly.items():
            quarter_paths.setdefault(quarter, []).append(tif_path)
            quarter_years.setdefault(quarter, []).append(year)

        for q, paths in quarter_paths.items():
            if q in climatology and not params.recompute_climatology:
                continue

            years = quarter_years[q]
            n_years = len(set(years))
            if n_years < params.min_years_for_climatology and params.verbose:
                print(
                    f"[WARN] quarter=Q{q}: only {n_years} year(s) of data. "
                    "Pixel-wise climatology may be noisy."
                )

            template_path = paths[0]
            out_tif = self._climatology_tif_for_quarter(q, params)
            out_tif.parent.mkdir(parents=True, exist_ok=True)

            datasets = [rasterio.open(p) for p in paths]
            try:
                with rasterio.open(template_path) as tmpl:
                    profile = tmpl.profile.copy()

                profile.update(
                    dtype="float32",
                    count=1,
                    nodata=params.nodata,
                    tiled=True,
                    compress="deflate",
                )

                with rasterio.open(out_tif, "w", **profile) as dst:
                    for (ji, window) in dst.block_windows(1):
                        h, w = window.height, window.width
                        block_stack: List[np.ndarray] = []

                        for ds in datasets:
                            arr = ds.read(1, window=window).astype(np.float32)
                            nodata = ds.nodata if ds.nodata is not None else params.nodata
                            arr = np.where(arr == nodata, np.nan, arr)
                            block_stack.append(arr)

                        stack = np.stack(block_stack, axis=0)
                        clim_block = np.nanmedian(stack, axis=0).astype(np.float32)

                        out_block = np.where(
                            np.isnan(clim_block),
                            params.nodata,
                            clim_block,
                        ).astype(np.float32)

                        dst.write(out_block, 1, window=window)

                    dst.build_overviews([2, 4, 8, 16], Resampling.nearest)
                    dst.update_tags(ns="rio_overview", resampling="nearest")

                climatology[q] = out_tif

                if params.verbose:
                    print(f"[OK] quarterly climatology Q{q} → {out_tif}")
            finally:
                for ds in datasets:
                    ds.close()

        return climatology

    # ------------------------------------------------------------------
    # Anomaly builder
    # ------------------------------------------------------------------
    def _build_anomaly_for_period(
        self,
        *,
        label: str,          # "YYYY-MM" or "YYYY-Qn"
        comp_path: Path,
        clim_path: Path,
        params: BuildNdviAnomalyMapsParams,
    ) -> tuple[Path, Path]:
        """
        Build anomaly for one period:

          anomaly = ndvi_current - ndvi_climatology(period_of_year)
        """
        ndvi_arr, profile = self._read_ndvi_with_profile(comp_path, params)
        clim_arr, _ = self._read_ndvi_as_float(clim_path, params)

        if ndvi_arr.shape != clim_arr.shape:
            raise ValueError(
                f"Shape mismatch between composite {comp_path} and climatology {clim_path}: "
                f"{ndvi_arr.shape} vs {clim_arr.shape}"
            )

        anomaly = ndvi_arr - clim_arr

        out_tif = params.cogs_dir / f"ndvi_anomaly_{label}_{params.aoi_id}.tif"
        self._write_anomaly_geotiff(out_tif, anomaly, profile, params)

        out_png = RepoPaths.figure(f"ndvi_anomaly_{label}_{params.aoi_id}_preview.png")
        self._write_anomaly_png(out_png, anomaly)

        if params.verbose:
            print(f"[OK] anomaly {label}: GeoTIFF  → {out_tif}")
            print(f"[OK] anomaly {label}: Preview → {out_png}")

        return out_tif, out_png

    # ------------------------------------------------------------------
    # Raster helpers
    # ------------------------------------------------------------------
    def _read_ndvi_as_float(
        self,
        path: Path,
        params: BuildNdviAnomalyMapsParams,
    ) -> tuple[np.ndarray, float]:
        """
        Read NDVI raster as float32, converting nodata → NaN.
        Returns (array, nodata_value).
        """
        with rasterio.open(path) as ds:
            arr = ds.read(1).astype(np.float32)
            nodata = ds.nodata if ds.nodata is not None else params.nodata

        arr = np.where(arr == nodata, np.nan, arr)
        return arr, nodata

    def _read_ndvi_with_profile(
        self,
        path: Path,
        params: BuildNdviAnomalyMapsParams,
    ) -> tuple[np.ndarray, dict]:
        """
        Read NDVI raster as float32 + full rasterio profile,
        converting nodata → NaN in the returned array.
        """
        with rasterio.open(path) as ds:
            profile = ds.profile.copy()
            arr = ds.read(1).astype(np.float32)
            nodata = ds.nodata if ds.nodata is not None else params.nodata

        profile.update(
            dtype="float32",
            count=1,
            nodata=params.nodata,
            tiled=True,
            compress="deflate",
        )

        arr = np.where(arr == nodata, np.nan, arr)
        return arr, profile

    def _write_climatology_geotiff(
        self,
        *,
        template_path: Path,
        out_path: Path,
        arr: np.ndarray,
        params: BuildNdviAnomalyMapsParams,
    ) -> None:
        """
        Use a template GeoTIFF (any NDVI for that period) for profile
        so we preserve CRS, transform, etc.
        """
        with rasterio.open(template_path) as ds:
            profile = ds.profile.copy()

        profile.update(
            dtype="float32",
            count=1,
            nodata=params.nodata,
            tiled=True,
            compress="deflate",
        )

        out = np.where(np.isnan(arr), params.nodata, arr)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(out.astype(np.float32), 1)
            dst.build_overviews([2, 4, 8, 16], Resampling.nearest)
            dst.update_tags(ns="rio_overview", resampling="nearest")

    def _write_anomaly_geotiff(
        self,
        out_path: Path,
        arr: np.ndarray,
        profile: dict,
        params: BuildNdviAnomalyMapsParams,
    ) -> None:
        """
        Write anomaly GeoTIFF using the provided profile.
        """
        out = np.where(np.isnan(arr), params.nodata, arr)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(out.astype(np.float32), 1)
            dst.build_overviews([2, 4, 8, 16], Resampling.nearest)
            dst.update_tags(ns="rio_overview", resampling="nearest")

    def _write_anomaly_png(self, out_path: Path, arr: np.ndarray) -> None:
        """
        Simple preview PNG for anomaly.
        Uses a symmetric color range around 0 so positive / negative anomalies stand out.
        """
        import matplotlib.pyplot as plt

        out_path.parent.mkdir(parents=True, exist_ok=True)

        data = np.copy(arr)
        data[np.isnan(data)] = 0.0

        plt.figure(figsize=(10, 8))
        plt.imshow(data, vmin=-0.5, vmax=0.5, cmap="RdBu_r")
        plt.colorbar(label="NDVI anomaly")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(out_path, dpi=150, bbox_inches="tight", pad_inches=0)
        plt.close()