import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.features import geometry_mask

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.services.SentinelHubNdviService import SentinelHubNdviService, NdviRequestParams

@dataclass(frozen=True)
class NdviMonthlyParams:
    month: str # "YYYY-MM"
    resolution_m: int = 10
    maxcc: float = 0.2

class NdviMonthlyCompositePipeline:
    def __init__(self, aoi_path: Path, params: NdviMonthlyParams) -> None:
        self.aoi_path = aoi_path
        self.params = params
        self.ndvi_service = SentinelHubNdviService()

    def _month_interval(self) -> Tuple[date, date]:
        y, m = self.params.month.split("-")
        y_i = int(y)
        m_i = int(m)

        start = date(y_i, m_i, 1)
        if m_i == 12:
            end = date(y_i + 1, 1, 1)
        else:
            end = date(y_i, m_i + 1, 1)
        return start, end

    def _load_aoi_wgs84(self) -> gpd.GeoDataFrame:
        gdf = gpd.read_file(self.aoi_path)
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326)
        else:
            gdf = gdf.to_crs(epsg=4326)
        return gdf

    def _clip_to_aoi(self, ndvi: np.ndarray, transform: rasterio.Affine, aoi: gpd.GeoDataFrame) -> np.ndarray:
        mask_outside = geometry_mask(
            geometries=list(aoi.geometry),
            invert=False,
            out_shape=ndvi.shape,
            transform=transform,
            all_touched=False,
        )
        out = ndvi.copy()
        out[mask_outside] = np.nan
        return out

    def _write_geotiff(self, out_tif: Path, ndvi: np.ndarray, transform: rasterio.Affine) -> None:
        out_tif.parent.mkdir(parents=True, exist_ok=True)

        profile = {
            "driver": "GTiff",
            "height": ndvi.shape[0],
            "width": ndvi.shape[1],
            "count": 1,
            "dtype": "float32",
            "crs": "EPSG:4326",
            "transform": transform,
            "nodata": np.nan,
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
            "compress": "DEFLATE",
            "predictor": 2,
        }

        with rasterio.open(out_tif, "w", **profile) as dst:
            dst.write(ndvi, 1)
            dst.build_overviews([2, 4, 8, 16], Resampling.average)
            dst.update_tags(ns="rio_overview", resampling="average")

    def _write_preview(self, out_png: Path, ndvi: np.ndarray) -> None:
        out_png.parent.mkdir(parents=True, exist_ok=True)
        plt.figure()
        plt.imshow(ndvi, vmin=-0.2, vmax=0.9)
        plt.colorbar(label="NDVI")
        plt.title(f"NDVI (cloud-masked) {self.params.month} — EL522")
        plt.axis("off")
        plt.savefig(out_png, dpi=200, bbox_inches="tight")
        plt.close()

    def _append_stats(self, stats_csv: Path, month: str, ndvi: np.ndarray) -> None:
        stats_csv.parent.mkdir(parents=True, exist_ok=True)

        finite = np.isfinite(ndvi)
        total = ndvi.size
        valid = int(finite.sum())
        ratio = valid / total if total else 0.0
        mean = float(np.nanmean(ndvi)) if valid else float("nan")
        median = float(np.nanmedian(ndvi)) if valid else float("nan")

        write_header = not stats_csv.exists()
        with stats_csv.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(["month", "mean_ndvi", "median_ndvi", "valid_pixel_ratio", "valid_pixels", "total_pixels"])
            w.writerow([month, mean, median, ratio, valid, total])

    def run(self) -> Tuple[Path, Path, Path]:
        start, end = self._month_interval()
        req_params = NdviRequestParams(resolution_m=self.params.resolution_m, maxcc=self.params.maxcc)

        ndvi, transform = self.ndvi_service.request_ndvi_array(self.aoi_path, start, end, req_params)

        aoi = self._load_aoi_wgs84()
        ndvi = self._clip_to_aoi(ndvi, transform, aoi)

        out_tif = RepoPaths.cog(f"ndvi_{self.params.month}_el522.tif")
        out_png = RepoPaths.figure(f"ndvi_{self.params.month}_preview.png")
        out_csv = RepoPaths.table("ndvi_monthly_stats.csv")

        self._write_geotiff(out_tif, ndvi, transform)
        self._write_preview(out_png, ndvi)
        self._append_stats(out_csv, self.params.month, ndvi)

        print(f"GeoTIFF: {out_tif}")
        print(f"Preview: {out_png}")
        print(f"Stats:   {out_csv}")
        return out_tif, out_png, out_csv
