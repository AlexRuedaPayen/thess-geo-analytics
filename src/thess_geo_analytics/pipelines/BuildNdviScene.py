from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.geo.CloudMasker import CloudMasker
from thess_geo_analytics.geo.NdviProcessor import NdviProcessor


class BuildNdviScene:
    def __init__(self) -> None:
        self.masker = CloudMasker()
        self.ndvi = NdviProcessor()

    def run(self) -> None:
        if len(sys.argv) < 3:
            raise SystemExit(
                "Usage: python -m thess_geo_analytics.pipelines.BuildNdviScene YYYY-MM SCENE_ID\n"
                "Example: python -m thess_geo_analytics.pipelines.BuildNdviScene 2026-01 S2A_MSIL2A_..."
            )

        month = sys.argv[1]
        scene_id = sys.argv[2]

        manifest_path = RepoPaths.table(f"assets_manifest_{month}.csv")
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing assets manifest: {manifest_path}")

        df = pd.read_csv(manifest_path)
        row = df[df["scene_id"] == scene_id]
        if row.empty:
            raise ValueError(f"Scene not found in manifest: {scene_id}")

        r = row.iloc[0]
        b04_path = Path(r["local_b04"])
        b08_path = Path(r["local_b08"])
        scl_path = Path(r["local_scl"])

        for p in [b04_path, b08_path, scl_path]:
            if not p.exists():
                raise FileNotFoundError(f"Missing local asset: {p}")

        # Read bands + ensure alignment
        red, nir, out_profile = self.ndvi.read_bands(b04_path, b08_path)

        # Read SCL on the same grid as B04/B08 (reproject/resample if needed)
        scl_on_grid, scl_nodata = self.masker.read_scl_as_target_grid(
            scl_path=scl_path,
            target_profile=out_profile,
        )
        invalid_mask = self.masker.build_invalid_mask_from_scl(scl_on_grid, scl_nodata=scl_nodata)

        # Compute NDVI and apply cloud/shadow mask
        ndvi = self.ndvi.compute_ndvi(red=red, nir=nir)
        ndvi_masked = self.ndvi.apply_mask_to_ndvi(ndvi, invalid_mask=invalid_mask)

        # Acceptance checks
        self._assert_bounds(ndvi_masked)

        # Export NDVI GeoTIFF to outputs/tmp
        RepoPaths.OUTPUTS.mkdir(parents=True, exist_ok=True)
        out_tmp_dir = RepoPaths.OUTPUTS / "tmp"
        out_tmp_dir.mkdir(parents=True, exist_ok=True)

        out_fig_dir = RepoPaths.OUTPUTS / "figures"
        out_fig_dir.mkdir(parents=True, exist_ok=True)

        out_tif = out_tmp_dir / f"ndvi_scene_{scene_id}.tif"
        out_png = out_fig_dir / f"ndvi_scene_{scene_id}_preview.png"

        self._write_geotiff(out_tif, ndvi_masked, out_profile)
        self._write_preview_png(out_png, ndvi_masked)

        print(f"NDVI scene written: {out_tif}")
        print(f"Preview written:    {out_png}")

    def _assert_bounds(self, ndvi_masked: np.ndarray) -> None:
        valid = ndvi_masked[~np.isnan(ndvi_masked)]
        if valid.size == 0:
            raise ValueError("All pixels are masked (no valid NDVI pixels). Check cloud masking / SCL alignment.")

        mn = float(np.min(valid))
        mx = float(np.max(valid))
        if mn < -1.0001 or mx > 1.0001:
            raise ValueError(f"NDVI out of bounds: min={mn}, max={mx} (expected within [-1, 1])")

    def _write_geotiff(self, path: Path, ndvi_masked: np.ndarray, profile: dict) -> None:
        arr = self.ndvi.to_nodata(ndvi_masked)
        prof = profile.copy()
        prof.update(driver="GTiff", compress="deflate", tiled=True)
        with rasterio.open(path, "w", **prof) as dst:
            dst.write(arr, 1)

    def _write_preview_png(self, path: Path, ndvi_masked: np.ndarray) -> None:
        # Minimal dependency preview using matplotlib (no custom colors required)
        import matplotlib.pyplot as plt

        img = ndvi_masked.copy()

        # Keep a visible range for vegetation; avoids the image looking washed out
        vmin, vmax = -0.2, 0.8

        plt.figure()
        plt.imshow(img, vmin=vmin, vmax=vmax)
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(path, dpi=150, bbox_inches="tight", pad_inches=0)
        plt.close()


if __name__ == "__main__":
    BuildNdviScene().run()
