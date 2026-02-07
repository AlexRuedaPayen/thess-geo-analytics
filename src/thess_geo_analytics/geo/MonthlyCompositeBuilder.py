from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

import numpy as np
import pandas as pd
import rasterio
from rasterio.warp import reproject
from rasterio.enums import Resampling

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.geo.CloudMasker import CloudMasker
from thess_geo_analytics.geo.NdviProcessor import NdviProcessor

from thess_geo_analytics.geo.AoiTargetGrid import AoiTargetGrid

from thess_geo_analytics.services.CdseAssetDownloader import CdseAssetDownloader
from thess_geo_analytics.services.CdseTokenService import CdseTokenService

@dataclass(frozen=True)
class MonthlyCompositeConfig:
    nodata: float = -9999.0
    max_scenes: Optional[int] = None
    composite_method: str = "median"
    verbose: bool = False
    download_missing: bool = True


class MonthlyCompositeBuilder:
    """
    Produces an AOI-wide NDVI monthly composite (~10m) using Sentinel-2.
    Supports multi-tile (T34/T35) scenes via AOI target grid reproject.
    """

    def __init__(
        self,
        aoi_path: Path,
        aoi_id: str = "el522",
        cfg: MonthlyCompositeConfig | None = None,
    ) -> None:

        self.aoi_path = aoi_path
        self.aoi_id = aoi_id
        self.cfg = cfg or MonthlyCompositeConfig()

        self.ndvi = NdviProcessor()
        self.masker = CloudMasker()

        self.token_service = CdseTokenService()
        self.downloader = CdseAssetDownloader(self.token_service)

    def run(self, month: str) -> tuple[Path, Path]:
        manifest = RepoPaths.table(f"assets_manifest_{month}.csv")
        if not manifest.exists():
            raise FileNotFoundError(f"Missing manifest: {manifest}")

        df = pd.read_csv(manifest)

        required = [
            "scene_id", "href_b04", "href_b08", "href_scl",
            "local_b04", "local_b08", "local_scl"
        ]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Manifest missing column {col}")

        if self.cfg.max_scenes is not None:
            df = df.head(self.cfg.max_scenes)

        if df.empty:
            raise RuntimeError("No scenes found for this month.")

        target = AoiTargetGrid(
            aoi_path=self.aoi_path,
            target_crs="EPSG:32634",
            resolution=10.0,
        ).build()

        out_profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "count": 1,
            "crs": target.crs,
            "transform": target.transform,
            "width": target.width,
            "height": target.height,
            "nodata": self.cfg.nodata,
            "tiled": True,
            "compress": "deflate",
        }

        ndvi_stack: List[np.ndarray] = []
        processed = 0
        skipped = 0

        for _, row in df.iterrows():
            try:
                self._ensure_assets(row)

                b04_path = Path(row["local_b04"])
                b08_path = Path(row["local_b08"])
                scl_path = Path(row["local_scl"])

                if not (b04_path.exists() and b08_path.exists() and scl_path.exists()):
                    print(f"[WARN] Missing assets for {row['scene_id']}, skipping.")
                    skipped += 1
                    continue

                with rasterio.open(b04_path) as ds_r, rasterio.open(b08_path) as ds_n:
                    red = ds_r.read(1).astype(np.float32)
                    nir = ds_n.read(1).astype(np.float32)

                    nd_native = self.ndvi.compute_ndvi(red, nir)
                    src_transform = ds_r.transform
                    src_crs = ds_r.crs

                nd_target = np.empty(
                    (target.height, target.width), dtype=np.float32
                )

                reproject(
                    source=nd_native,
                    destination=nd_target,
                    src_transform=src_transform,
                    src_crs=src_crs,
                    dst_transform=target.transform,
                    dst_crs=target.crs,
                    resampling=Resampling.bilinear,
                    dst_nodata=np.nan,
                )

                with rasterio.open(scl_path) as sds:
                    scl_native = rasterio.band(sds, 1)
                    scl_nodata = sds.nodata

                    scl_target = np.empty(
                        (target.height, target.width), dtype=np.uint16
                    )

                    reproject(
                        source=scl_native,
                        destination=scl_target,
                        src_transform=sds.transform,
                        src_crs=sds.crs,
                        dst_transform=target.transform,
                        dst_crs=target.crs,
                        resampling=Resampling.nearest,
                        dst_nodata=scl_nodata,
                    )

                invalid_cloud = self.masker.build_invalid_mask_from_scl(
                    scl_target, scl_nodata
                )

                nd_target[invalid_cloud] = np.nan
                nd_target[~target.aoi_mask] = np.nan

                ndvi_stack.append(nd_target)
                processed += 1

            except Exception as e:
                print(f"[WARN] Skipping {row['scene_id']} → {e}")
                skipped += 1

        if processed == 0:
            raise RuntimeError("No scenes processed successfully.")

        stack = np.stack(ndvi_stack, axis=0)
        composite = np.nanmedian(stack, axis=0).astype(np.float32)

        valid = composite[~np.isnan(composite)]
        if valid.size > 0:
            if valid.min() < -1.0001 or valid.max() > 1.0001:
                raise ValueError("Composite NDVI outside [-1,1].")

        cogs_dir = RepoPaths.OUTPUTS / "cogs"
        cogs_dir.mkdir(parents=True, exist_ok=True)

        RepoPaths.FIGURES.mkdir(parents=True, exist_ok=True)

        out_tif = cogs_dir / f"ndvi_{month}_{self.aoi_id}.tif"
        out_png = RepoPaths.figure(f"ndvi_{month}_preview.png")

        self._write_geotiff(out_tif, composite, out_profile)
        self._write_preview_png(out_png, composite)

        print(f"[INFO] Scenes processed: {processed}, skipped: {skipped}")
        print(f"[INFO] Written: {out_tif}")
        print(f"[INFO] Preview: {out_png}")

        return out_tif, out_png

    def _ensure_assets(self, row: pd.Series):
        """
        Self-healing: download missing B04/B08/SCL if needed.
        """
        scene_id = row["scene_id"]

        for href, local_col in [
            (row["href_b04"], "local_b04"),
            (row["href_b08"], "local_b08"),
            (row["href_scl"], "local_scl"),
        ]:
            local = Path(row[local_col])
            local.parent.mkdir(parents=True, exist_ok=True)

            if not local.exists():
                print(f"[INFO] Downloading {scene_id} → {local.name}")
                self.downloader.download(href, local)

    def _write_geotiff(self, path: Path, arr: np.ndarray, profile: dict):
        """Write GeoTIFF with nodata + overviews."""
        out = np.where(np.isnan(arr), profile["nodata"], arr)

        with rasterio.open(path, "w", **profile) as dst:
            dst.write(out.astype(np.float32), 1)
            dst.build_overviews([2, 4, 8, 16], Resampling.nearest)
            dst.update_tags(ns="rio_overview", resampling="nearest")

    def _write_preview_png(self, path: Path, arr: np.ndarray):
        import matplotlib.pyplot as plt

        plt.figure(figsize=(10, 8))
        plt.imshow(arr, vmin=-0.2, vmax=0.8)
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(path, dpi=150, bbox_inches="tight", pad_inches=0)
        plt.close()
