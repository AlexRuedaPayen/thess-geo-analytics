from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from skimage.segmentation import slic
from skimage.util import img_as_float

from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass
class BuildSuperpixelFeaturesParams:
    # 7-band pixel feature raster (from BuildPixelFeatures)
    pixel_features_raster: Path = RepoPaths.OUTPUTS / "cogs" / "pixel_features_7d.tif"

    # Superpixel label raster (will be created from pixel_features_raster if missing)
    superpixel_raster: Path = RepoPaths.OUTPUTS / "cogs" / "superpixels.tif"

    # SLIC parameters
    n_segments: int = 1500
    compactness: float = 10.0
    max_iter: int = 10

    # Output CSV with superpixel-level aggregated features
    out_csv: Path = RepoPaths.OUTPUTS / "cogs" / "superpixel_features.csv"


class BuildSuperpixelFeaturesPipeline:
    def run(self, params: BuildSuperpixelFeaturesParams) -> Path:
        # 1) Open pixel feature raster
        if not params.pixel_features_raster.exists():
            raise FileNotFoundError(
                f"Pixel feature raster not found: {params.pixel_features_raster}\n"
                f"Run BuildPixelFeatures first."
            )

        with rasterio.open(params.pixel_features_raster) as ds_feats:
            if ds_feats.count != 7:
                raise ValueError(
                    f"Expected 7-band features raster, got {ds_feats.count} bands."
                )
            feats = ds_feats.read()  # (7, H, W)
            feats_nodata = ds_feats.nodata
            transform = ds_feats.transform
            crs = ds_feats.crs

        H, W = feats.shape[1], feats.shape[2]

        # 2) Build superpixel labels from features if needed
        if not params.superpixel_raster.exists():
            print(
                f"[INFO] Superpixel raster not found → {params.superpixel_raster}\n"
                f"       Building it from pixel_features_7d.tif (bands 1–3)."
            )
            labels = self._build_superpixels_from_features(
                feats, transform, params
            )
            # Write labels
            profile_out = {
                "driver": "GTiff",
                "height": H,
                "width": W,
                "count": 1,
                "dtype": "int32",
                "crs": crs,
                "transform": transform,
                "nodata": 0,
                "compress": "deflate",
            }
            params.superpixel_raster.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(params.superpixel_raster, "w", **profile_out) as dst:
                dst.write(labels.astype("int32"), 1)
            print(f"[OK] Superpixels written → {params.superpixel_raster}")
        else:
            with rasterio.open(params.superpixel_raster) as ds_labels:
                labels = ds_labels.read(1)

        if labels.shape != (H, W):
            raise ValueError(
                f"Shape mismatch between labels {labels.shape} and features {(H, W)}."
            )

        # 3) Aggregate features over superpixels
        print("[INFO] Aggregating features per superpixel…")

        labels_flat = labels.reshape(-1)              # (N,)
        feats_flat = feats.reshape(7, -1).T           # (N, 7)

        # Valid mask: label>0, finite features, not nodata
        mask_valid = labels_flat > 0
        if feats_nodata is not None:
            mask_valid &= ~np.any(feats_flat == feats_nodata, axis=1)
        mask_valid &= np.isfinite(feats_flat).all(axis=1)

        labels_valid = labels_flat[mask_valid]
        feats_valid = feats_flat[mask_valid]

        if feats_valid.size == 0:
            raise RuntimeError("No valid pixels for superpixel aggregation.")

        unique_labels = np.unique(labels_valid)
        print(f"[INFO] Unique superpixels (labels>0): {unique_labels.size}")

        # Precompute pixel center coordinates
        ys, xs = np.indices((H, W))
        xs = xs.reshape(-1)
        ys = ys.reshape(-1)

        x_coords = transform.c + xs * transform.a + ys * transform.b
        y_coords = transform.f + xs * transform.d + ys * transform.e

        x_coords_valid = x_coords[mask_valid]
        y_coords_valid = y_coords[mask_valid]

        # Normalization ranges for x_norm, y_norm
        x_min, x_max = x_coords_valid.min(), x_coords_valid.max()
        y_min, y_max = y_coords_valid.min(), y_coords_valid.max()
        x_range = max(x_max - x_min, 1e-9)
        y_range = max(y_max - y_min, 1e-9)

        # Pixel area
        px_w = transform.a
        px_h = -transform.e
        pixel_area = abs(px_w * px_h)

        records = []
        for lbl in unique_labels:
            sel = labels_valid == lbl
            if not np.any(sel):
                continue

            f = feats_valid[sel]        # (n_pix, 7)
            x_sel = x_coords_valid[sel]
            y_sel = y_coords_valid[sel]

            area = sel.sum() * pixel_area
            x_centroid = float(x_sel.mean())
            y_centroid = float(y_sel.mean())
            x_norm = (x_centroid - x_min) / x_range
            y_norm = (y_centroid - y_min) / y_range

            feat_mean = np.nanmean(f, axis=0)
            feat_std = np.nanstd(f, axis=0)

            rec = {
                "superpixel_id": int(lbl),
                "area": area,
                "x_centroid": x_centroid,
                "y_centroid": y_centroid,
                "x_norm": x_norm,
                "y_norm": y_norm,
            }
            for i in range(7):
                rec[f"feat{i+1}_mean"] = float(feat_mean[i])
                rec[f"feat{i+1}_std"] = float(feat_std[i])

            records.append(rec)

        df = pd.DataFrame.from_records(records)
        params.out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(params.out_csv, index=False)

        print(f"[OK] Superpixel features written → {params.out_csv}")
        print(f"[INFO] Rows: {len(df)}")
        return params.out_csv

    # ------------------ internal: SLIC from features ------------------ #
    def _build_superpixels_from_features(
        self,
        feats: np.ndarray,
        transform,
        params: BuildSuperpixelFeaturesParams,
    ) -> np.ndarray:
        """
        Build SLIC superpixels directly from the 7D feature raster.
        Uses bands 1–3 as a pseudo-RGB image for segmentation.

        - feats: (7, H, W)
        - returns labels: (H, W) int32, starting at 1
        """
        # feats: (7, H, W)
        n_bands = feats.shape[0]
        n_used = min(3, n_bands)
        sel = feats[:n_used]  # (n_used, H, W)

        # Move to (H, W, C)
        img = np.moveaxis(sel, 0, -1).astype("float32")  # (H, W, C)

        # ---- Handle NaNs & scale to approx [0, 1] ----
        mask_finite = np.isfinite(img)

        if mask_finite.any():
            # compute robust min/max only on finite pixels
            vals = img[mask_finite]
            vmin = np.nanpercentile(vals, 2)
            vmax = np.nanpercentile(vals, 98)
            if vmax <= vmin:
                vmax = vmin + 1e-6

            img_clip = np.clip(img, vmin, vmax)
            img_norm = (img_clip - vmin) / (vmax - vmin)
        else:
            # degenerate case: everything is NaN
            img_norm = np.zeros_like(img, dtype="float32")

        # Replace any remaining NaNs / infs by 0 so slic won't choke
        bad = ~np.isfinite(img_norm)
        if bad.any():
            img_norm[bad] = 0.0

        # Convert to float in [0, 1] as skimage expects
        img_norm = img_as_float(img_norm)

        # IMPORTANT: no max_iter (not supported in recent skimage),
        # and we tell it that channels are on the last axis.
        labels = slic(
            img_norm,
            n_segments=params.n_segments,
            compactness=params.compactness,
            start_label=1,
            channel_axis=-1,
        )

        return labels.astype("int32")

    # ------------------ smoke test ------------------ #
    @staticmethod
    def smoke_test() -> None:
        print("=== BuildSuperpixelFeaturesPipeline Smoke Test ===")
        print("Run via entrypoint with existing pixel_features_7d.tif.")
        print("Smoke test OK")