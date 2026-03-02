from __future__ import annotations

from pathlib import Path
import numpy as np
import rasterio
import matplotlib.pyplot as plt


class SyntheticNdviViewer:
    """
    Visualize synthetic NDVI reconstruction scenes:

      • Ground-truth NDVI:  <name>_NDVI_TRUE.tif
      • RED band (B04):     <name>_B04.tif
      • NIR band (B08):     <name>_B08.tif

    Reads files from:
        tests/fixtures/generated/ndvi_reconstruction/<name>/
    where <name> is e.g. "mini", "large".
    """

    def __init__(self):
        self.root = Path("tests/fixtures/generated/ndvi_reconstruction").resolve()

        if not self.root.exists():
            raise RuntimeError(f"No synthetic NDVI reconstruction data found: {self.root}")

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
        self._print_stats(f"{title_prefix} – NDVI_TRUE", ndvi_arr)

        # Robust vmin/vmax for B04/B08
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
        axes[2].set_title("NDVI TRUE")
        axes[2].axis("off")
        fig.colorbar(im2, ax=axes[2], fraction=0.046, pad=0.04)

        fig.suptitle(title_prefix, fontsize=14)
        plt.tight_layout()
        plt.show()

    # -----------------------------
    # public API
    # -----------------------------
    def list_cases(self):
        """
        List subdirectories under ndvi_reconstruction/, e.g. mini, large.
        """
        if not self.root.exists():
            return []
        return [p for p in sorted(self.root.iterdir()) if p.is_dir()]

    def run(self):
        print("\n=== Synthetic NDVI Viewer (reconstruction scenes) ===")
        print("Root:", self.root)

        cases = self.list_cases()
        if not cases:
            print("No subdirectories found under", self.root)
            return

        for case_dir in cases:
            name = case_dir.name
            print(f"\n=== Case: {name} ===")

            ndvi_path = case_dir / f"{name}_NDVI_TRUE.tif"
            b04_path = case_dir / f"{name}_B04.tif"
            b08_path = case_dir / f"{name}_B08.tif"

            missing = [p for p in (ndvi_path, b04_path, b08_path) if not p.exists()]
            if missing:
                print(f"[WARN] Missing files in case {name}:")
                for m in missing:
                    print("   -", m)
                continue

            ndvi_arr = self._read_tif(ndvi_path)
            b04_arr = self._read_tif(b04_path)
            b08_arr = self._read_tif(b08_path)

            title = f"{name}: NDVI_TRUE vs B04/B08"
            self._show_triplet(b04_arr, b08_arr, ndvi_arr, title_prefix=title)


if __name__ == "__main__":
    SyntheticNdviViewer().run()