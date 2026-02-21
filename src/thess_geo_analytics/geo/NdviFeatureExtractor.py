from __future__ import annotations
import numpy as np
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple
import rasterio
from rasterio.enums import Resampling
from scipy.stats import skew


@dataclass
class NdviFeatureExtractorConfig:
    nodata: float = -9999.0
    anomaly_threshold: float = -0.2   # threshold for anomaly_persistence
    years_for_recovery: int = 2       # last N years for recovery_ratio


class NdviFeatureExtractor:
    """
    Computes 7D temporal features from a stack of NDVI rasters:
      F1 = trend_slope
      F2 = seasonal_std
      F3 = min_anomaly
      F4 = recovery_ratio
      F5 = anomaly_persistence
      F6 = ndvi_variance
      F7 = ndvi_skew
    """

    def __init__(self, cfg: NdviFeatureExtractorConfig | None = None):
        self.cfg = cfg or NdviFeatureExtractorConfig()

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------
    def compute_features_from_cogs(
        self,
        ndvi_cog_paths: List[Path],
        timestamps: List[np.datetime64],
    ) -> Tuple[np.ndarray, dict]:
        """
        Load COGs, stack them, then compute 7D features.

        Returns:
            features: np.ndarray with shape (H, W, 7)
            meta:     dict with rasterio profile for writing output
        """
        stack, meta = self._load_stack(ndvi_cog_paths)
        features = self.compute_features(stack, timestamps)
        return features, meta

    # -------------------------------------------------------------------
    # Core feature computation
    # -------------------------------------------------------------------
    def compute_features(
        self,
        stack: np.ndarray,               # shape (T, H, W)
        timestamps: List[np.datetime64], # length T
    ) -> np.ndarray:
        T, H, W = stack.shape
        cfg = self.cfg

        nd = stack.astype("float32")  # ensure float operations
        nd[nd == cfg.nodata] = np.nan

        # ---- 1. Trend slope (linear regression vs time index) ----
        t = np.arange(T, dtype=np.float32)
        t_mean = t.mean()
        t_var = ((t - t_mean) ** 2).sum()

        # Covariance time × NDVI
        cov = np.nansum((t[:, None, None] - t_mean) * (nd - np.nanmean(nd, axis=0)), axis=0)
        slope = cov / t_var

        # ---- 2. seasonal_std (std of monthly anomalies)
        # Compute monthly mean NDVI then anomaly = NDVI - monthly_mean
        months = np.array([int(str(ts)[5:7]) for ts in timestamps])
        monthly_means = np.zeros((12, H, W), dtype=np.float32)

        for m in range(1, 13):
            mask_m = (months == m)
            if mask_m.any():
                monthly_means[m - 1] = np.nanmean(nd[mask_m], axis=0)
            else:
                monthly_means[m - 1] = np.nan

        anomaly = nd - monthly_means[months - 1]
        seasonal_std = np.nanstd(anomaly, axis=0)

        # ---- 3. min_anomaly
        min_anomaly = np.nanmin(anomaly, axis=0)

        # ---- 4. recovery_ratio
        yrs = cfg.years_for_recovery
        months_per_year = 12
        N_last = yrs * months_per_year

        nd_mean_first = np.nanmean(nd[:N_last], axis=0)
        nd_mean_last = np.nanmean(nd[-N_last:], axis=0)
        recovery_ratio = nd_mean_last / nd_mean_first

        # ---- 5. anomaly_persistence
        anomaly_persistence = np.sum(anomaly < cfg.anomaly_threshold, axis=0)

        # ---- 6. ndvi_variance
        ndvi_variance = np.nanvar(nd, axis=0)

        # ---- 7. ndvi_skew
        ndvi_skew = skew(nd, axis=0, nan_policy="omit")

        # ---- combine features (H,W,7)
        feats = np.stack(
            [
                slope,
                seasonal_std,
                min_anomaly,
                recovery_ratio,
                anomaly_persistence,
                ndvi_variance,
                ndvi_skew,
            ],
            axis=-1,
        )

        return feats

    # -------------------------------------------------------------------
    # Stack loader
    # -------------------------------------------------------------------
    def _load_stack(self, paths: List[Path]) -> Tuple[np.ndarray, dict]:
        arrs = []
        meta = None

        for p in paths:
            with rasterio.open(p) as ds:
                if meta is None:
                    meta = ds.profile
                arr = ds.read(1).astype("float32")
                arrs.append(arr)

        stack = np.stack(arrs, axis=0)  # (T,H,W)
        return stack, meta
    

    # -------------------------
    # Smoke test
    # -------------------------
    @staticmethod
    def smoke_test():
        """
        Synthetic check that verifies the feature extraction behaves correctly.
        Creates 3 test pixels:

          Pixel A: decreasing NDVI      → negative slope
          Pixel B: oscillating NDVI     → high seasonal_std
          Pixel C: recovering NDVI      → positive slope, high recovery_ratio

        Prints the extracted 7D feature vectors for visual inspection.
        """
        import numpy as np
        from datetime import datetime

        print("=== NdviFeatureExtractor Smoke Test ===")

        cfg = NdviFeatureExtractorConfig()
        extractor = NdviFeatureExtractor(cfg)

        T = 20
        H, W = 1, 3

        # Pixel A: strong decline
        pix_A = np.linspace(0.8, 0.2, T)

        # Pixel B: sine oscillation
        pix_B = 0.5 + 0.3 * np.sin(np.linspace(0, 4 * np.pi, T))

        # Pixel C: recovery (low → high)
        pix_C = np.linspace(0.3, 0.9, T)

        # Build (T, H, W) array
        stack = np.stack([pix_A, pix_B, pix_C], axis=1).reshape(T, H, W)

        # Fake timestamps
        # Fake monthly timestamps using NumPy only
        timestamps = np.array([
            np.datetime64("2020-01") + np.timedelta64(i, "M")
            for i in range(T)
        ])

        feats = extractor.compute_features(stack, timestamps)  # (1,3,7)

        labels = [
            "trend_slope",
            "seasonal_std",
            "min_anomaly",
            "recovery_ratio",
            "anomaly_persistence",
            "ndvi_variance",
            "ndvi_skew",
        ]

        # Print features for each pixel
        for j in range(W):
            print(f"\n--- Pixel {j} ---")
            for k, name in enumerate(labels):
                print(f"{name:20s}: {feats[0, j, k]:.4f}")

        print("\n✓ Smoke test completed.\n")


if __name__ == "__main__":
    NdviFeatureExtractor.smoke_test()