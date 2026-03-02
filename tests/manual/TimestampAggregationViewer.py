from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import rasterio
from rasterio.merge import merge
from rasterio.enums import Resampling
import matplotlib.pyplot as plt


class TimestampAggregationViewer:
    """
    QA tool to visualize timestamp aggregation BEFORE vs AFTER for B04/B08.

    It expects the synthetic layout used by the tests:

      tests/fixtures/generated/timestamps_aggregation/
        cache_s2/
          <scene_id>/
            B04.tif
            B08.tif
            SCL.tif
        data_raw/
          aggregated/
            <timestamp_sanitized>/
              B04.tif
              B08.tif
              SCL.tif
        tables/
          time_serie.csv

    For each timestamp in time_serie.csv that has an aggregated folder,
    it:

      1) Rebuilds a "before" mosaic for B04 and B08 by merging tiles
         found in cache_s2 for that timestamp.
      2) Reads the "after" aggregated B04/B08 rasters.
      3) Shows a 2x2 matplotlib figure:

            [B04 BEFORE]   [B04 AFTER]
            [B08 BEFORE]   [B08 AFTER]

         with simple stats printed to the console.
    """

    def __init__(self, root: Path | str = "tests/fixtures/generated/timestamps_aggregation") -> None:
        self.root = Path(root).resolve()
        self.cache_s2 = self.root / "cache_s2"
        self.data_raw = self.root / "data_raw"
        self.tables = self.root / "tables"

        self._check_paths()

    # ------------------------------------------------------------------
    # Setup checks
    # ------------------------------------------------------------------
    def _check_paths(self) -> None:
        if not self.cache_s2.exists():
            raise RuntimeError(f"cache_s2 directory not found: {self.cache_s2}")
        if not self.data_raw.exists():
            raise RuntimeError(f"data_raw directory not found: {self.data_raw}")
        ts_csv = self.tables / "time_serie.csv"
        if not ts_csv.exists():
            raise RuntimeError(f"time_serie.csv not found: {ts_csv}")

    # ------------------------------------------------------------------
    # IO helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _read_single(path: Path) -> Tuple[np.ndarray, rasterio.Affine, any]:
        with rasterio.open(path) as ds:
            arr = ds.read(1).astype("float32")
            transform = ds.transform
            crs = ds.crs
        return arr, transform, crs

    @staticmethod
    def _print_stats(label: str, arr: np.ndarray) -> None:
        valid = arr[~np.isnan(arr)]
        print(f"\n--- {label} ---")
        if valid.size == 0:
            print("  no valid pixels")
            return
        print(f"  min : {float(valid.min()):.6f}")
        print(f"  max : {float(valid.max()):.6f}")
        print(f"  mean: {float(valid.mean()):.6f}")

    @staticmethod
    def _split_tile_ids(raw_id) -> List[str]:
        if raw_id is None or (isinstance(raw_id, float) and pd.isna(raw_id)):
            return []
        if isinstance(raw_id, str):
            import re

            parts = re.split(r"[;,|]", raw_id)
            return [p.strip() for p in parts if p.strip()]
        return [str(raw_id)]

    # ------------------------------------------------------------------
    # Mosaic builder ("before" image)
    # ------------------------------------------------------------------
    def _build_before_mosaic(self, timestamp: str, band: str, df: pd.DataFrame) -> np.ndarray:
        """
        Build a "before" mosaic for a given timestamp and band by merging
        all tiles listed in time_serie.csv for that timestamp.
        """
        df_ts = df[df["acq_datetime"] == timestamp]
        if df_ts.empty:
            raise RuntimeError(f"No rows in time_serie.csv for timestamp {timestamp!r}")

        scene_ids: List[str] = []
        for raw in df_ts["tile_ids"].tolist():
            scene_ids.extend(self._split_tile_ids(raw))

        # Deduplicate while preserving order
        seen = set()
        scene_ids = [s for s in scene_ids if not (s in seen or seen.add(s))]

        if not scene_ids:
            raise RuntimeError(f"No tile_ids found for timestamp {timestamp!r}")

        input_files: List[Path] = []
        for sid in scene_ids:
            tif = self.cache_s2 / sid / f"{band}.tif"
            if tif.exists():
                input_files.append(tif)
            else:
                print(f"[WARN] Missing input tile for {band}: {tif}")

        if not input_files:
            raise RuntimeError(f"No input {band} tiles found for timestamp {timestamp!r}")

        # Use rasterio.merge to build a mosaic (similar to TileAggregator, but simpler)
        datasets = [rasterio.open(str(p)) for p in input_files]
        try:
            mosaic, transform = merge(
                datasets,
                method="first",
                resampling=Resampling.nearest,
            )
        finally:
            for ds in datasets:
                ds.close()

        # mosaic.shape = (1, H, W)
        return mosaic[0]

    # ------------------------------------------------------------------
    # "After" raster loader (aggregated)
    # ------------------------------------------------------------------
    def _load_after_raster(self, timestamp: str, band: str) -> np.ndarray:
        safe_ts = timestamp.replace(":", "_")
        folder = self.data_raw / "aggregated" / safe_ts
        tif = folder / f"{band}.tif"
        if not tif.exists():
            raise RuntimeError(f"Aggregated {band} not found for timestamp {timestamp!r}: {tif}")
        arr, _, _ = self._read_single(tif)
        return arr

    # ------------------------------------------------------------------
    # Plotting logic
    # ------------------------------------------------------------------
    def _show_before_after(
        self,
        ts: str,
        before_b04: np.ndarray,
        after_b04: np.ndarray,
        before_b08: np.ndarray,
        after_b08: np.ndarray,
    ) -> None:
        # Print stats + simple diff metrics
        self._print_stats(f"{ts} – B04 BEFORE", before_b04)
        self._print_stats(f"{ts} – B04 AFTER", after_b04)

        diff_b04 = after_b04 - before_b04
        self._print_stats(f"{ts} – B04 AFTER - BEFORE", diff_b04)

        self._print_stats(f"{ts} – B08 BEFORE", before_b08)
        self._print_stats(f"{ts} – B08 AFTER", after_b08)

        diff_b08 = after_b08 - before_b08
        self._print_stats(f"{ts} – B08 AFTER - BEFORE", diff_b08)

        # Color scales: use robust percentiles for each panel
        def _robust_limits(arr: np.ndarray) -> tuple[float, float]:
            valid = arr[~np.isnan(arr)]
            if valid.size == 0:
                return 0.0, 1.0
            return np.percentile(valid, [2, 98])

        b04b_vmin, b04b_vmax = _robust_limits(before_b04)
        b04a_vmin, b04a_vmax = _robust_limits(after_b04)
        b08b_vmin, b08b_vmax = _robust_limits(before_b08)
        b08a_vmin, b08a_vmax = _robust_limits(after_b08)

        fig, axes = plt.subplots(2, 2, figsize=(12, 10))

        im00 = axes[0, 0].imshow(before_b04, cmap="Reds", vmin=b04b_vmin, vmax=b04b_vmax)
        axes[0, 0].set_title("B04 BEFORE (tile mosaic)")
        axes[0, 0].axis("off")
        fig.colorbar(im00, ax=axes[0, 0], fraction=0.046, pad=0.04)

        im01 = axes[0, 1].imshow(after_b04, cmap="Reds", vmin=b04a_vmin, vmax=b04a_vmax)
        axes[0, 1].set_title("B04 AFTER (aggregated)")
        axes[0, 1].axis("off")
        fig.colorbar(im01, ax=axes[0, 1], fraction=0.046, pad=0.04)

        im10 = axes[1, 0].imshow(before_b08, cmap="Greens", vmin=b08b_vmin, vmax=b08b_vmax)
        axes[1, 0].set_title("B08 BEFORE (tile mosaic)")
        axes[1, 0].axis("off")
        fig.colorbar(im10, ax=axes[1, 0], fraction=0.046, pad=0.04)

        im11 = axes[1, 1].imshow(after_b08, cmap="Greens", vmin=b08a_vmin, vmax=b08a_vmax)
        axes[1, 1].set_title("B08 AFTER (aggregated)")
        axes[1, 1].axis("off")
        fig.colorbar(im11, ax=axes[1, 1], fraction=0.046, pad=0.04)

        fig.suptitle(f"Timestamp aggregation – {ts}", fontsize=14)
        plt.tight_layout()
        plt.show()

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------
    def run(self) -> None:
        ts_csv = self.tables / "time_serie.csv"
        df = pd.read_csv(ts_csv)
        if df.empty:
            print("[INFO] time_serie.csv is empty – nothing to visualize.")
            return

        timestamps = sorted(df["acq_datetime"].unique())
        print(f"[INFO] Found {len(timestamps)} timestamps in time_serie.csv")

        for ts in timestamps:
            safe_ts = ts.replace(":", "_")
            agg_folder = self.data_raw / "aggregated" / safe_ts
            if not agg_folder.exists():
                print(f"[WARN] No aggregated folder for timestamp {ts!r}, skipping.")
                continue

            print(f"\n=== Timestamp {ts} ===")

            # Build BEFORE mosaics from tiles
            before_b04 = self._build_before_mosaic(ts, band="B04", df=df)
            before_b08 = self._build_before_mosaic(ts, band="B08", df=df)

            # Load AFTER aggregated rasters
            after_b04 = self._load_after_raster(ts, band="B04")
            after_b08 = self._load_after_raster(ts, band="B08")

            # Show side-by-side
            self._show_before_after(ts, before_b04, after_b04, before_b08, after_b08)

            resp = input("Press <Enter> for next timestamp, or 'q' to quit: ").strip().lower()
            if resp == "q":
                break


if __name__ == "__main__":
    TimestampAggregationViewer().run()