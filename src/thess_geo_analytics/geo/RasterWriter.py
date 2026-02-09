from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import numpy as np
import rasterio

@dataclass(frozen=True)
class RasterWriterConfig:
    compress: str = "deflate"
    tiled: bool = True
    overviews: tuple[int, ...] = (2, 4, 8, 16)

class RasterWriter:
    def __init__(self, cfg: RasterWriterConfig | None = None) -> None:
        self.cfg = cfg or RasterWriterConfig()

    def write_geotiff(self, path: Path, arr: np.ndarray, profile: dict) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)

        prof = profile.copy()
        prof.update(driver="GTiff", compress=self.cfg.compress, tiled=self.cfg.tiled)

        with rasterio.open(path, "w", **prof) as dst:
            dst.write(arr.astype(np.float32), 1)

            # QGIS-friendly pyramids
            try:
                dst.build_overviews(list(self.cfg.overviews), rasterio.enums.Resampling.nearest)
                dst.update_tags(ns="rio_overview", resampling="nearest")
            except Exception:
                # overviews can fail on some drivers/filesystems; not fatal
                pass

        return path

    def write_preview_png(self, path: Path, arr: np.ndarray, *, vmin: float = -0.2, vmax: float = 0.8) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)

        import matplotlib.pyplot as plt

        plt.figure(figsize=(6, 6))
        plt.imshow(arr, vmin=vmin, vmax=vmax)
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(path, dpi=150, bbox_inches="tight", pad_inches=0)
        plt.close()

        return path
