from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List

import numpy as np
import rasterio
from rasterio.transform import from_origin


@dataclass
class MiniTimestampAggregationSceneConfig:
    """
    Configuration for synthetic timestamp-aggregation test data.

    root:  base folder where "cache_s2" and "tables" will be created.
           This should match the RepoPathsOverride root in tests.
    ts:    acquisition datetime string (as used in time_serie.csv).
    H, W:  base tile height/width (for the small tile).

    pixel: pixel size in same units as the CRS.
    crs:   CRS code (string) used for all rasters.
    """
    root: Path
    ts: str
    H: int
    W: int
    pixel: float = 10.0
    crs: str = "EPSG:32634"


class MiniTimestampAggregationSceneGenerator:
    """
    Helper to generate synthetic B04/B08/SCL tiles and time_serie.csv for
    testing TimestampsAggregationBuilder.

    It writes into:

      <root>/cache_s2/<scene_id>/
        B04.tif
        B08.tif
        SCL.tif

      <root>/tables/time_serie.csv

    The test is responsible for redirecting RepoPaths.CACHE_S2 and
    RepoPaths.TABLES to these same locations via RepoPathsOverride.
    """

    def __init__(self, cfg: MiniTimestampAggregationSceneConfig) -> None:
        self.cfg = cfg
        self.root = cfg.root.resolve()
        self.cache_s2_dir = self.root / "cache_s2"
        self.tables_dir = self.root / "tables"

        self.cache_s2_dir.mkdir(parents=True, exist_ok=True)
        self.tables_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Low-level helper
    # ------------------------------------------------------------------
    def _write_tile(
        self,
        path: Path,
        *,
        data: np.ndarray,
        transform,
        crs: str,
        nodata: float,
        dtype: str,
    ) -> None:
        h, w = data.shape
        profile = {
            "driver": "GTiff",
            "height": h,
            "width": w,
            "count": 1,
            "dtype": dtype,
            "crs": crs,
            "transform": transform,
            "nodata": nodata,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(path, "w", **profile) as ds:
            ds.write(data.astype(dtype), 1)

    # ------------------------------------------------------------------
    # Public API: single-tile case (simple, uniform master)
    # ------------------------------------------------------------------
    def generate_single_tile_case(self, scene_id: str = "SCENE_A") -> Dict[str, Any]:
        """
        Create a synthetic *master* B04/B08 scene of size (H, W), then
        write it as a single tile (scene_id) and time_serie.csv.

        Returns:
          {
            "ts": <timestamp str>,
            "scene_ids": [scene_id],
            "master_red": np.ndarray[H, W],
            "master_nir": np.ndarray[H, W],
            "transform": Affine,
            "cache_s2_dir": Path,
            "tables_dir": Path,
          }
        """
        cfg = self.cfg
        H, W = cfg.H, cfg.W

        transform = from_origin(0.0, 100.0, cfg.pixel, cfg.pixel)

        # Simple uniform master values
        red_val = 0.2
        nir_val = 0.8
        master_red = np.full((H, W), red_val, dtype="float32")
        master_nir = np.full((H, W), nir_val, dtype="float32")

        # Write time_serie.csv
        import pandas as pd

        df_ts = pd.DataFrame(
            [
                {
                    "acq_datetime": cfg.ts,
                    "tile_ids": scene_id,
                }
            ]
        )
        ts_csv = self.tables_dir / "time_serie.csv"
        ts_csv.parent.mkdir(parents=True, exist_ok=True)
        df_ts.to_csv(ts_csv, index=False)

        # Write tiles for scene_id
        scene_dir = self.cache_s2_dir / scene_id
        scene_dir.mkdir(parents=True, exist_ok=True)

        self._write_tile(
            scene_dir / "B04.tif",
            data=master_red,
            transform=transform,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )
        self._write_tile(
            scene_dir / "B08.tif",
            data=master_nir,
            transform=transform,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )

        scl = np.full((H, W), 4, dtype="uint16")  # clear
        self._write_tile(
            scene_dir / "SCL.tif",
            data=scl,
            transform=transform,
            crs=cfg.crs,
            nodata=0,
            dtype="uint16",
        )

        return {
            "ts": cfg.ts,
            "scene_ids": [scene_id],
            "master_red": master_red,
            "master_nir": master_nir,
            "transform": transform,
            "cache_s2_dir": self.cache_s2_dir,
            "tables_dir": self.tables_dir,
        }

    # ------------------------------------------------------------------
    # Public API: two-tiles case (split master into left/right, no overlap)
    # ------------------------------------------------------------------
    def generate_two_tiles_split_case(
        self,
        scene_left: str = "SCENE_LEFT",
        scene_right: str = "SCENE_RIGHT",
    ) -> Dict[str, Any]:
        """
        Build a synthetic MASTER scene of size (H, 2W):

          - Left half:  RED=0.2, NIR=0.8
          - Right half: RED=0.6, NIR=0.4

        Then split into two tiles:

          scene_left:  x ∈ [0,        W*pixel)
          scene_right: x ∈ [W*pixel,  2W*pixel)

        Writes time_serie.csv with both scene IDs and all tiles to
        <root>/cache_s2.

        Returns:
          {
            "ts": <timestamp>,
            "scene_ids": [scene_left, scene_right],
            "master_red": np.ndarray[H, 2W],
            "master_nir": np.ndarray[H, 2W],
            "master_transform": Affine,
            "cache_s2_dir": Path,
            "tables_dir": Path,
          }
        """
        cfg = self.cfg
        H, W = cfg.H, cfg.W
        W_total = 2 * W

        # Master transform for full width (origin at x=0)
        master_transform = from_origin(0.0, 100.0, cfg.pixel, cfg.pixel)

        # Build MASTER arrays
        master_red = np.empty((H, W_total), dtype="float32")
        master_nir = np.empty((H, W_total), dtype="float32")

        # Left half: NDVI ~ 0.6
        master_red[:, :W] = 0.2
        master_nir[:, :W] = 0.8

        # Right half: NDVI ~ -0.2
        master_red[:, W:] = 0.6
        master_nir[:, W:] = 0.4

        # Write time_serie.csv
        import pandas as pd

        df_ts = pd.DataFrame(
            [
                {
                    "acq_datetime": cfg.ts,
                    "tile_ids": f"{scene_left};{scene_right}",
                }
            ]
        )
        ts_csv = self.tables_dir / "time_serie.csv"
        ts_csv.parent.mkdir(parents=True, exist_ok=True)
        df_ts.to_csv(ts_csv, index=False)

        # Tile transforms
        transform_left = from_origin(0.0, 100.0, cfg.pixel, cfg.pixel)
        transform_right = from_origin(W * cfg.pixel, 100.0, cfg.pixel, cfg.pixel)

        # Left tile data
        red_left = master_red[:, :W]
        nir_left = master_nir[:, :W]

        # Right tile data
        red_right = master_red[:, W:]
        nir_right = master_nir[:, W:]

        # Write tiles for left scene
        dir_left = self.cache_s2_dir / scene_left
        dir_left.mkdir(parents=True, exist_ok=True)

        self._write_tile(
            dir_left / "B04.tif",
            data=red_left,
            transform=transform_left,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )
        self._write_tile(
            dir_left / "B08.tif",
            data=nir_left,
            transform=transform_left,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )

        scl_left = np.full((H, W), 4, dtype="uint16")
        self._write_tile(
            dir_left / "SCL.tif",
            data=scl_left,
            transform=transform_left,
            crs=cfg.crs,
            nodata=0,
            dtype="uint16",
        )

        # Write tiles for right scene
        dir_right = self.cache_s2_dir / scene_right
        dir_right.mkdir(parents=True, exist_ok=True)

        self._write_tile(
            dir_right / "B04.tif",
            data=red_right,
            transform=transform_right,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )
        self._write_tile(
            dir_right / "B08.tif",
            data=nir_right,
            transform=transform_right,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )

        scl_right = np.full((H, W), 4, dtype="uint16")
        self._write_tile(
            dir_right / "SCL.tif",
            data=scl_right,
            transform=transform_right,
            crs=cfg.crs,
            nodata=0,
            dtype="uint16",
        )

        return {
            "ts": cfg.ts,
            "scene_ids": [scene_left, scene_right],
            "master_red": master_red,
            "master_nir": master_nir,
            "master_transform": master_transform,
            "cache_s2_dir": self.cache_s2_dir,
            "tables_dir": self.tables_dir,
        }

    # ------------------------------------------------------------------
    # Public API: more complex case – overlapping tiles + gradients + clouds
    # ------------------------------------------------------------------
    def generate_two_tiles_overlap_case(
        self,
        scene_a: str = "SCENE_OVERLAP_A",
        scene_b: str = "SCENE_OVERLAP_B",
    ) -> Dict[str, Any]:
        """
        More complex synthetic case:

          - MASTER scene: size (H, 2W), non-uniform RED/NIR (horizontal gradient)
          - Two tiles with horizontal overlap:
              * scene_a: x ∈ [0,         W*pixel)
              * scene_b: x ∈ [W/2*pixel, 2W*pixel)

            so columns [W/2 .. W-1] are covered by BOTH tiles.

          - B04/B08 values come from the MASTER grid in both tiles, so
            in the overlapping region both tiles carry the SAME values.
            For merge_method='first', the aggregated mosaic is exactly
            equal to MASTER.

          - SCL contains "cloudy" patches (code 9) in scene_b only,
            so later NDVI / cloud-masking code can be exercised.

        Writes:
          - cache_s2/<scene_a|scene_b>/{B04,B08,SCL}.tif
          - tables/time_serie.csv with both scene IDs.

        Returns:
          {
            "ts": <timestamp>,
            "scene_ids": [scene_a, scene_b],
            "master_red": np.ndarray[H, 2W],
            "master_nir": np.ndarray[H, 2W],
            "master_transform": Affine,
            "cache_s2_dir": Path,
            "tables_dir": Path,
          }
        """
        cfg = self.cfg
        H, W = cfg.H, cfg.W
        W_total = 2 * W

        # MASTER transform (origin x=0)
        master_transform = from_origin(0.0, 100.0, cfg.pixel, cfg.pixel)

        # Build a horizontal gradient for B04/B08 instead of constants
        # Example:
        #   col_frac = j / (W_total - 1) in [0,1]
        #   RED = 0.2 + 0.4 * col_frac   in [0.2, 0.6]
        #   NIR = 0.8 - 0.4 * col_frac   in [0.8, 0.4]
        j = np.arange(W_total, dtype="float32")
        col_frac = j / max(W_total - 1, 1)
        red_cols = 0.2 + 0.4 * col_frac   # shape (W_total,)
        nir_cols = 0.8 - 0.4 * col_frac

        master_red = np.tile(red_cols, (H, 1))
        master_nir = np.tile(nir_cols, (H, 1))

        # time_serie.csv → both scenes at same timestamp
        import pandas as pd

        df_ts = pd.DataFrame(
            [
                {
                    "acq_datetime": cfg.ts,
                    "tile_ids": f"{scene_a};{scene_b}",
                }
            ]
        )
        ts_csv = self.tables_dir / "time_serie.csv"
        ts_csv.parent.mkdir(parents=True, exist_ok=True)
        df_ts.to_csv(ts_csv, index=False)

        # Tile transforms:
        #   scene_a: [0,          W*pixel)
        #   scene_b: [W/2*pixel,  2W*pixel)
        # which creates an overlap of W/2 columns.
        offset_b_cols = W // 2
        offset_b_x = offset_b_cols * cfg.pixel

        transform_a = from_origin(0.0, 100.0, cfg.pixel, cfg.pixel)
        transform_b = from_origin(offset_b_x, 100.0, cfg.pixel, cfg.pixel)

        # Extract tile arrays from MASTER
        # scene_a covers columns 0..W-1
        red_a = master_red[:, :W]
        nir_a = master_nir[:, :W]

        # scene_b covers columns offset_b_cols .. offset_b_cols + W - 1
        start_b = offset_b_cols
        end_b = start_b + W
        red_b = master_red[:, start_b:end_b]
        nir_b = master_nir[:, start_b:end_b]

        # Write tiles for scene_a
        dir_a = self.cache_s2_dir / scene_a
        dir_a.mkdir(parents=True, exist_ok=True)

        self._write_tile(
            dir_a / "B04.tif",
            data=red_a,
            transform=transform_a,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )
        self._write_tile(
            dir_a / "B08.tif",
            data=nir_a,
            transform=transform_a,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )

        # Clear SCL for scene_a
        scl_a = np.full((H, W), 4, dtype="uint16")  # clear
        self._write_tile(
            dir_a / "SCL.tif",
            data=scl_a,
            transform=transform_a,
            crs=cfg.crs,
            nodata=0,
            dtype="uint16",
        )

        # Write tiles for scene_b
        dir_b = self.cache_s2_dir / scene_b
        dir_b.mkdir(parents=True, exist_ok=True)

        self._write_tile(
            dir_b / "B04.tif",
            data=red_b,
            transform=transform_b,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )
        self._write_tile(
            dir_b / "B08.tif",
            data=nir_b,
            transform=transform_b,
            crs=cfg.crs,
            nodata=-9999.0,
            dtype="float32",
        )

        # SCL for scene_b: mostly clear, with a "cloudy" patch (code 9)
        scl_b = np.full((H, W), 4, dtype="uint16")
        # Put a simple cloud patch in the center
        r0, r1 = H // 4, 3 * H // 4
        c0, c1 = W // 4, 3 * W // 4
        scl_b[r0:r1, c0:c1] = 9  # cloud
        self._write_tile(
            dir_b / "SCL.tif",
            data=scl_b,
            transform=transform_b,
            crs=cfg.crs,
            nodata=0,
            dtype="uint16",
        )

        return {
            "ts": cfg.ts,
            "scene_ids": [scene_a, scene_b],
            "master_red": master_red,
            "master_nir": master_nir,
            "master_transform": master_transform,
            "cache_s2_dir": self.cache_s2_dir,
            "tables_dir": self.tables_dir,
            "overlap_cols": (start_b, end_b),
        }