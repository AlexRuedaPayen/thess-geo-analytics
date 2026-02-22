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
class NdviAnomalyMapsConfig:
    cogs_dir: Path = RepoPaths.OUTPUTS / "cogs"
    aoi_id: str = "el522"

    # Limit years used in climatology / anomaly
    year_start: Optional[int] = None
    year_end: Optional[int] = None

    # nodata in NDVI COGs
    nodata: float = -9999.0

    # Minimum number of years before we feel good about climatology
    min_years_for_climatology: int = 3

    # If False and climatology tifs already exist, re-use them
    recompute_climatology: bool = False

    verbose: bool = False


class NdviAnomalyMapsBuilder:
    """
    Builds NDVI pixel-wise anomaly rasters:

        anomaly(YYYY-MM) = NDVI(YYYY-MM) - median_monthly_climatology(month_of_year)

    Inputs:  ndvi_YYYY-MM_<aoi>.tif  (monthly composites)
    Outputs: ndvi_climatology_median_MM_<aoi>.tif (per month-of-year)
             ndvi_anomaly_YYYY-MM_<aoi>.tif
             preview PNG: ndvi_anomaly_YYYY-MM_<aoi>.png
    """

    _MONTHLY_RE_TEMPLATE = r"^ndvi_(\d{{4}})-(\d{{2}})_{aoi}\.tif$"
    _CLIM_RE_TEMPLATE    = r"^ndvi_climatology_median_(\d{{2}})_{aoi}\.tif$"

    def __init__(self, cfg: NdviAnomalyMapsConfig) -> None:
        self.cfg = cfg

        self._monthly_pattern = re.compile(
            self._MONTHLY_RE_TEMPLATE.format(aoi=re.escape(self.cfg.aoi_id))
        )
        self._clim_pattern = re.compile(
            self._CLIM_RE_TEMPLATE.format(aoi=re.escape(self.cfg.aoi_id))
        )

    # -----------------------
    # Public API
    # -----------------------
    def run_all(self) -> list[tuple[str, Path, Path]]:
        """
        Returns: list of (label, anomaly_tif, anomaly_png)
                 where label is YYYY-MM.
        """
        if not self.cfg.cogs_dir.exists():
            raise FileNotFoundError(f"COGs directory not found: {self.cfg.cogs_dir}")

        monthly = self._find_monthly_composites()

        if not monthly:
            raise RuntimeError(
                f"No monthly NDVI composites found under {self.cfg.cogs_dir} "
                f"for AOI {self.cfg.aoi_id}."
            )

        # Build or load per-month-of-year climatology rasters
        climatology = self._build_or_load_climatology(monthly)

        # Build anomalies for each monthly composite
        results: list[tuple[str, Path, Path]] = []

        for label, (year, month, tif_path) in sorted(monthly.items()):
            month_of_year = month
            clim_tif = climatology.get(month_of_year)
            if clim_tif is None:
                # No climatology for this month, skip
                if self.cfg.verbose:
                    print(f"[WARN] No climatology found for month={month_of_year:02d}, skipping {label}.")
                continue

            anom_tif, anom_png = self._build_anomaly_for_month(
                label=label,
                comp_path=tif_path,
                clim_path=clim_tif,
            )
            results.append((label, anom_tif, anom_png))

        if self.cfg.verbose:
            print(f"[OK] total anomaly rasters produced: {len(results)}")

        return results

    # -----------------------
    # Discovery
    # -----------------------
    def _find_monthly_composites(self) -> Dict[str, Tuple[int, int, Path]]:
        """
        Scan cogs_dir for files of the form:
            ndvi_YYYY-MM_<aoi>.tif

        Returns:
            { "YYYY-MM": (year, month, path), ... }
        """
        out: Dict[str, Tuple[int, int, Path]] = {}

        for p in self.cfg.cogs_dir.glob("ndvi_*.tif"):
            m = self._monthly_pattern.match(p.name)
            if not m:
                continue

            year = int(m.group(1))
            month = int(m.group(2))

            # Apply year filters
            if self.cfg.year_start is not None and year < self.cfg.year_start:
                continue
            if self.cfg.year_end is not None and year > self.cfg.year_end:
                continue

            label = f"{year:04d}-{month:02d}"
            out[label] = (year, month, p)

        if self.cfg.verbose:
            print(f"[INFO] Monthly NDVI composites found: {len(out)}")

        return out

    # -----------------------
    # Climatology
    # -----------------------
    def _climatology_tif_for_month(self, month: int) -> Path:
        """
        Path for climatology GeoTIFF for a given month-of-year.
        """
        return self.cfg.cogs_dir / f"ndvi_climatology_median_{month:02d}_{self.cfg.aoi_id}.tif"

    def _build_or_load_climatology(
        self,
        monthly: Dict[str, Tuple[int, int, Path]],
    ) -> Dict[int, Path]:
        """
        For each month-of-year m, compute or load:

            median over all years of NDVI(YYYY-m) per pixel.

        Returns:
            { month_of_year: Path_to_climatology_tif, ... }
        """
        # First, check existing climatology rasters if recompute_climatology=False
        climatology: Dict[int, Path] = {}

        if not self.cfg.recompute_climatology:
            for m in range(1, 13):
                tif = self._climatology_tif_for_month(m)
                if tif.exists():
                    climatology[m] = tif

        # If we have all 12 already and recompute_climatology=False – we’re done
        all_months_with_data = {month for _, (year, month, _) in monthly.items()}
        if not self.cfg.recompute_climatology and all(
            (m not in all_months_with_data) or (m in climatology) for m in range(1, 13)
        ):
            if self.cfg.verbose:
                print("[INFO] Reusing existing climatology GeoTIFFs.")
            return climatology

        # Otherwise, compute climatology for months we’re missing
        # Collect arrays per month-of-year
        month_arrays: Dict[int, List[np.ndarray]] = {}
        month_years: Dict[int, List[int]] = {}

        for label, (year, month, tif_path) in monthly.items():
            arr, _ = self._read_ndvi_as_float(tif_path)
            month_arrays.setdefault(month, []).append(arr)
            month_years.setdefault(month, []).append(year)

        # For each month, compute median across years
        for m, arr_list in month_arrays.items():
            if m in climatology and not self.cfg.recompute_climatology:
                # already have tif, skip
                continue

            years = month_years[m]
            n_years = len(set(years))
            if n_years < self.cfg.min_years_for_climatology and self.cfg.verbose:
                print(
                    f"[WARN] month={m:02d}: only {n_years} year(s) of data. "
                    "Pixel-wise climatology may be noisy."
                )

            stack = np.stack(arr_list, axis=0)  # shape: (time, H, W)
            clim_arr = np.nanmedian(stack, axis=0).astype(np.float32)

            # Write climatology raster
            out_tif = self._climatology_tif_for_month(m)
            self._write_climatology_geotiff(
                template_path=month_arrays[m] and monthly[next(
                    label for label, (yy, mm, _) in monthly.items() if mm == m
                )][2],
                out_path=out_tif,
                arr=clim_arr,
            )

            climatology[m] = out_tif

            if self.cfg.verbose:
                print(f"[OK] climatology month={m:02d} → {out_tif}")

        return climatology

    # -----------------------
    # Anomaly for one month
    # -----------------------
    def _build_anomaly_for_month(
        self,
        *,
        label: str,
        comp_path: Path,
        clim_path: Path,
    ) -> tuple[Path, Path]:
        """
        Build anomaly for one YYYY-MM:

            anomaly = ndvi_current - ndvi_climatology_month
        """
        ndvi_arr, profile = self._read_ndvi_with_profile(comp_path)
        clim_arr, _ = self._read_ndvi_as_float(clim_path)

        if ndvi_arr.shape != clim_arr.shape:
            raise ValueError(
                f"Shape mismatch between composite {comp_path} and climatology {clim_path}: "
                f"{ndvi_arr.shape} vs {clim_arr.shape}"
            )

        # Compute anomaly, preserving NaNs where either is NaN
        anomaly = ndvi_arr - clim_arr
        # If either input was NaN, result is NaN automatically

        # Write anomaly GeoTIFF
        out_tif = self.cfg.cogs_dir / f"ndvi_anomaly_{label}_{self.cfg.aoi_id}.tif"
        self._write_anomaly_geotiff(out_tif, anomaly, profile)

        # Write preview PNG
        out_png = RepoPaths.figure(f"ndvi_anomaly_{label}_{self.cfg.aoi_id}_preview.png")
        self._write_anomaly_png(out_png, anomaly)

        if self.cfg.verbose:
            print(f"[OK] anomaly {label}: {out_tif}")
            print(f"[OK] anomaly {label} preview: {out_png}")

        return out_tif, out_png

    # -----------------------
    # Raster helpers
    # -----------------------
    def _read_ndvi_as_float(self, path: Path) -> tuple[np.ndarray, float]:
        """
        Read NDVI raster as float32, converting nodata → NaN.
        Returns (array, nodata_value).
        """
        with rasterio.open(path) as ds:
            arr = ds.read(1).astype(np.float32)
            nodata = ds.nodata if ds.nodata is not None else self.cfg.nodata

        arr = np.where(arr == nodata, np.nan, arr)
        return arr, nodata

    def _read_ndvi_with_profile(self, path: Path) -> tuple[np.ndarray, dict]:
        """
        Read NDVI raster as float32 + full rasterio profile,
        converting nodata → NaN in the returned array.
        """
        with rasterio.open(path) as ds:
            profile = ds.profile.copy()
            arr = ds.read(1).astype(np.float32)
            nodata = ds.nodata if ds.nodata is not None else self.cfg.nodata

        profile.update(
            dtype="float32",
            count=1,
            nodata=self.cfg.nodata,
            tiled=True,
            compress="deflate",
        )

        arr = np.where(arr == nodata, np.nan, arr)
        return arr, profile

    def _write_climatology_geotiff(
        self,
        template_path: Path,
        out_path: Path,
        arr: np.ndarray,
    ) -> None:
        """
        Use the template GeoTIFF (any monthly NDVI for that month) for profile
        so we preserve CRS, transform, etc.
        """
        with rasterio.open(template_path) as ds:
            profile = ds.profile.copy()

        profile.update(
            dtype="float32",
            count=1,
            nodata=self.cfg.nodata,
            tiled=True,
            compress="deflate",
        )

        out = np.where(np.isnan(arr), self.cfg.nodata, arr)

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
    ) -> None:
        """
        Write anomaly GeoTIFF using the provided profile.
        """
        out = np.where(np.isnan(arr), self.cfg.nodata, arr)

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

        # mask NaNs for plotting
        data = np.copy(arr)
        data[np.isnan(data)] = 0.0

        plt.figure(figsize=(10, 8))
        plt.imshow(data, vmin=-0.5, vmax=0.5, cmap="RdBu_r")
        plt.colorbar(label="NDVI anomaly")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(out_path, dpi=150, bbox_inches="tight", pad_inches=0)
        plt.close()