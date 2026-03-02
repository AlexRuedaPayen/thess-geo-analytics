from __future__ import annotations

from pathlib import Path
import numpy as np
import rasterio
import matplotlib.pyplot as plt


class SyntheticNdviViewer:
    """
    Minimal utility to visualize:
      • synthetic B04/B08 from the aggregated generator
      • NDVI COGs produced by NdviAggregatedCompositeBuilder

    Reads files from:
        tests/fixtures/generated/ndvi_aggregated/
    """

    def __init__(self):
        self.root = Path("tests/fixtures/generated/ndvi_aggregated").resolve()
        self.agg_root = self.root / "aggregated"
        self.cogs_root = self.root / "outputs" / "cogs"

        if not self.agg_root.exists():
            raise RuntimeError(f"No synthetic aggregated data found: {self.agg_root}")

        if not self.cogs_root.exists():
            raise RuntimeError(f"No NDVI COGs found: {self.cogs_root}")

    # -----------------------------
    # helpers
    # -----------------------------
    @staticmethod
    def _read_tif(path: Path) -> np.ndarray:
        with rasterio.open(path) as ds:
            arr = ds.read(1).astype("float32")
            nodata = ds.nodata
            if nodata is not None:
                arr = np.where(arr == nodata, np.nan, arr)
        return arr

    @staticmethod
    def _print_stats(name: str, arr: np.ndarray):
        valid = arr[~np.isnan(arr)]
        print(f"\n---- {name} ----")
        if valid.size == 0:
            print("  no valid pixels")
            return
        print("  min:", float(valid.min()))
        print("  max:", float(valid.max()))
        print("  mean:", float(valid.mean()))
        print("  % < 0:", float((valid < 0).mean()) * 100)

    def _show_triplet(self, b04_arr, b08_arr, ndvi_arr, title_prefix: str):
        # Stats
        self._print_stats(f"{title_prefix} – B04", b04_arr)
        self._print_stats(f"{title_prefix} – B08", b08_arr)
        self._print_stats(f"{title_prefix} – NDVI", ndvi_arr)

        # Shared valid mask for vmin/vmax on B04/B08
        v_b04 = b04_arr[~np.isnan(b04_arr)]
        v_b08 = b08_arr[~np.isnan(b08_arr)]
        if v_b04.size > 0:
            b04_vmin, b04_vmax = np.percentile(v_b04, [2, 98])
        else:
            b04_vmin, b04_vmax = 0, 1
        if v_b08.size > 0:
            b08_vmin, b08_vmax = np.percentile(v_b08, [2, 98])
        else:
            b08_vmin, b08_vmax = 0, 1

        fig, axes = plt.subplots(1, 3, figsize=(15, 5))

        im0 = axes[0].imshow(b04_arr, cmap="Reds", vmin=b04_vmin, vmax=b04_vmax)
        axes[0].set_title("B04 (RED)")
        axes[0].axis("off")
        fig.colorbar(im0, ax=axes[0], fraction=0.046, pad=0.04)

        im1 = axes[1].imshow(b08_arr, cmap="Greens", vmin=b08_vmin, vmax=b08_vmax)
        axes[1].set_title("B08 (NIR)")
        axes[1].axis("off")
        fig.colorbar(im1, ax=axes[1], fraction=0.046, pad=0.04)

        im2 = axes[2].imshow(ndvi_arr, cmap="RdYlGn", vmin=-1.0, vmax=1.0)
        axes[2].set_title("NDVI composite")
        axes[2].axis("off")
        fig.colorbar(im2, ax=axes[2], fraction=0.046, pad=0.04)

        fig.suptitle(title_prefix, fontsize=14)
        plt.tight_layout()
        plt.show()

    # -----------------------------
    # public API
    # -----------------------------
    def list_synthetic_timestamps(self):
        folders = sorted(self.agg_root.iterdir())
        return [f for f in folders if f.is_dir()]

    def run(self):
        print("\n=== Synthetic NDVI Viewer ===")
        print("Synthetic aggregated path:", self.agg_root)
        print("NDVI COG output path:", self.cogs_root)

        ts_dirs = self.list_synthetic_timestamps()
        if not ts_dirs:
            print("No timestamp folders found under", self.agg_root)
            return

        for ts_dir in ts_dirs:
            # Derive month label from timestamp folder name: '2024-01-10T...' -> '2024-01'
            ts_name = ts_dir.name  # e.g. '2024-01-10T10_00_00Z'
            month_label = ts_name[:7]  # '2024-01'

            # Find matching NDVI COG (monthly composite)
            matching_cogs = list(self.cogs_root.glob(f"ndvi_{month_label}_*.tif"))
            if not matching_cogs:
                print(f"[WARN] No NDVI COG found for month {month_label}, skipping.")
                continue

            ndvi_path = matching_cogs[0]

            # Read B04/B08/NDVI
            b04_path = next(ts_dir.glob("*B04*.tif"))
            b08_path = next(ts_dir.glob("*B08*.tif"))

            b04_arr = self._read_tif(b04_path)
            b08_arr = self._read_tif(b08_path)
            ndvi_arr = self._read_tif(ndvi_path)

            title = f"Timestamp {ts_name} vs NDVI {ndvi_path.name}"
            self._show_triplet(b04_arr, b08_arr, ndvi_arr, title_prefix=title)


if __name__ == "__main__":
    SyntheticNdviViewer().run()