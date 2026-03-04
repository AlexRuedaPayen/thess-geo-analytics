from __future__ import annotations

from pathlib import Path
from typing import Tuple, List

import numpy as np
import rasterio
import matplotlib.pyplot as plt


class DownsampleAggregationViewer:
    """
    Visual QA tool to inspect test cases produced by the downsampling tests.

    Expected layout:

      tests/fixtures/generated/downsample_builder/session/

          test_case_name/
              original/timestamp_001/{B04,B08,SCL}.tif
              produced/timestamp_001/{B04,B08,SCL}.tif
              expected/... (optional)

    Each test case is displayed sequentially.
    """

    BANDS = ["B04", "B08", "SCL"]

    def __init__(
        self,
        session_root: Path | str = "tests/fixtures/generated/downsample_builder/session",
    ) -> None:

        self.session_root = Path(session_root).resolve()

        if not self.session_root.exists():
            raise RuntimeError(f"Session folder not found: {self.session_root}")

    # ---------------------------------------------------------
    # Raster helpers
    # ---------------------------------------------------------

    @staticmethod
    def _read(path: Path) -> np.ndarray:

        with rasterio.open(path) as ds:

            arr = ds.read(1).astype("float32")

            nodata = ds.nodata

            if nodata is not None:
                arr = np.where(arr == nodata, np.nan, arr)

        return arr

    @staticmethod
    def _robust_limits(arr: np.ndarray) -> Tuple[float, float]:

        valid = arr[~np.isnan(arr)]

        if valid.size == 0:
            return 0.0, 1.0

        return tuple(np.percentile(valid, [2, 98]))

    @staticmethod
    def _cmap_for(band: str) -> str:

        if band == "B04":
            return "Reds"

        if band == "B08":
            return "Greens"

        if band == "SCL":
            return "tab20"

        return "gray"

    # ---------------------------------------------------------
    # Case discovery
    # ---------------------------------------------------------

    def _cases(self) -> List[Path]:

        return sorted(
            [p for p in self.session_root.iterdir() if p.is_dir()]
        )

    # ---------------------------------------------------------
    # Visualization
    # ---------------------------------------------------------

    def show_case(self, case_dir: Path, timestamp: str = "timestamp_001") -> None:

        original = case_dir / "original" / timestamp
        produced = case_dir / "produced" / timestamp

        if not original.exists():
            print(f"[SKIP] Missing original folder: {original}")
            return

        if not produced.exists():
            print(f"[SKIP] Missing produced folder: {produced}")
            return

        fig, axes = plt.subplots(3, 2, figsize=(12, 12))

        for i, band in enumerate(self.BANDS):

            orig_file = original / f"{band}.tif"
            prod_file = produced / f"{band}.tif"

            if not orig_file.exists() or not prod_file.exists():
                print(f"[SKIP] Missing {band}")
                continue

            arr_orig = self._read(orig_file)
            arr_prod = self._read(prod_file)

            vmin_o, vmax_o = self._robust_limits(arr_orig)
            vmin_p, vmax_p = self._robust_limits(arr_prod)

            cmap = self._cmap_for(band)

            im0 = axes[i, 0].imshow(arr_orig, cmap=cmap, vmin=vmin_o, vmax=vmax_o)
            axes[i, 0].set_title(f"{band} ORIGINAL")
            axes[i, 0].axis("off")
            fig.colorbar(im0, ax=axes[i, 0], fraction=0.046, pad=0.04)

            im1 = axes[i, 1].imshow(arr_prod, cmap=cmap, vmin=vmin_p, vmax=vmax_p)
            axes[i, 1].set_title(f"{band} PRODUCED")
            axes[i, 1].axis("off")
            fig.colorbar(im1, ax=axes[i, 1], fraction=0.046, pad=0.04)

        fig.suptitle(f"Test case: {case_dir.name}", fontsize=14)

        plt.tight_layout()
        plt.show()

    # ---------------------------------------------------------
    # Runner
    # ---------------------------------------------------------

    def run(self) -> None:

        cases = self._cases()

        if not cases:
            print("No test cases found.")
            return

        print(f"[INFO] Found {len(cases)} test cases\n")

        for case in cases:

            print(f"\n=== Test case: {case.name} ===")

            self.show_case(case)

            resp = input("Press <Enter> for next case or 'q' to quit: ").strip().lower()

            if resp == "q":
                break


if __name__ == "__main__":

    DownsampleAggregationViewer().run()