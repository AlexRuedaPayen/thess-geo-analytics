from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
import numpy as np
import rasterio
from rasterio.transform import from_origin


@dataclass
class MockCdseAssetDownloader:
    """
    Offline downloader that understands hrefs like:
      mock://<scene_id>/B04
      mock://<scene_id>/B08
      mock://<scene_id>/SCL

    And generates deterministic GeoTIFFs at the requested local path.
    """

    width: int = 64
    height: int = 64

    def download(self, url: str, out_path: Path) -> Path:
        """
        Keep this method name to match *most* downloader implementations.
        If your RawAssetStorageManager calls a different method name,
        just add an alias wrapper below.
        """
        out_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_fake_geotiff(url, out_path)
        return out_path

    # --- Optional aliases (add as needed to match your real downloader API) ---
    def download_to_file(self, url: str, out_path: Path) -> Path:
        return self.download(url, out_path)

    def download_asset(self, url: str, out_path: Path) -> Path:
        return self.download(url, out_path)

    # -------------------------------------------------------------------------
    def _seed_from_url(self, url: str) -> int:
        h = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return int(h[:8], 16)

    def _write_fake_geotiff(self, url: str, out_path: Path) -> None:
        seed = self._seed_from_url(url)
        rng = np.random.default_rng(seed)

        # Band data:
        # - B04/B08 => uint16 reflectance-ish
        # - SCL => uint8 classes-ish
        is_scl = url.endswith("/SCL") or url.endswith("SCL")

        if is_scl:
            data = rng.integers(0, 12, size=(self.height, self.width), dtype=np.uint8)
            dtype = rasterio.uint8
        else:
            data = rng.integers(0, 10000, size=(self.height, self.width), dtype=np.uint16)
            dtype = rasterio.uint16

        transform = from_origin(23.0, 41.0, 10.0, 10.0)  # arbitrary but valid

        with rasterio.open(
            out_path,
            "w",
            driver="GTiff",
            width=self.width,
            height=self.height,
            count=1,
            dtype=dtype,
            crs="EPSG:4326",
            transform=transform,
        ) as ds:
            ds.write(data, 1)