from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import math
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT  #reprojection


class TileAggregationError(RuntimeError):
    """
    High-level error raised when something goes wrong during tile aggregation.
    """
    pass


@dataclass
class TileAggregator:
    """
    Robust helper to merge multiple raster tiles into a single mosaic.

    - Reprojects all inputs to the CRS of the first tile if needed
      (via WarpedVRT), so CRS mismatches are handled automatically.
    """

    merge_method: str = "first"
    resampling: str = "nearest"
    nodata: Optional[float] = float("nan")
    promote_to_float_if_needed: bool = True
    strict_dtype: bool = True

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def aggregate_band(
        self,
        input_files: List[Path],
        output_path: Path,
    ) -> None:
        if not input_files:
            raise TileAggregationError("No input files provided for aggregation.")

        # Ensure all files exist
        missing_paths = [p for p in input_files if not Path(p).is_file()]
        if missing_paths:
            raise TileAggregationError(
                "Some input files do not exist:\n  " +
                "\n  ".join(str(p) for p in missing_paths)
            )

        allowed_methods = {"first", "last", "min", "max", "sum", "count"}
        if self.merge_method not in allowed_methods:
            raise TileAggregationError(
                f"Unsupported merge_method={self.merge_method!r}. "
                f"Allowed: {sorted(allowed_methods)}"
            )

        # Resampling enum
        try:
            resampling_enum = getattr(
                Resampling,
                self.resampling.lower(),
                Resampling.nearest,
            )
        except Exception as e:
            raise TileAggregationError(
                f"Failed to map resampling='{self.resampling}' to a rasterio Resampling enum."
            ) from e

        datasets: List[rasterio.io.DatasetReader] = []
        vrts: List[WarpedVRT] = []

        try:
            # -----------------------------
            # 1) Open datasets
            # -----------------------------
            for p in input_files:
                try:
                    ds = rasterio.open(str(p))
                except Exception as e:
                    raise TileAggregationError(
                        f"Failed to open raster file: {p}"
                    ) from e
                datasets.append(ds)

            if not datasets:
                raise TileAggregationError("No datasets opened from input_files.")

            # Consistency: dtype, band count
            dtypes = {ds.dtypes[0] for ds in datasets}
            band_counts = {ds.count for ds in datasets}

            if self.strict_dtype and len(dtypes) > 1:
                raise TileAggregationError(
                    "Input rasters have mixed dtypes, which is disallowed in strict mode: "
                    f"{sorted(dtypes)}"
                )

            if len(band_counts) > 1:
                raise TileAggregationError(
                    "Input rasters have different band counts; aggregation assumes "
                    "same number of bands in all tiles. Band counts found: "
                    f"{sorted(band_counts)}"
                )

            src_dtype = next(iter(dtypes))
            src_band_count = next(iter(band_counts))

            # Nodata consistency (just for first pass – might still be None)
            src_nodata_values = {ds.nodata for ds in datasets}
            src_nodata = (
                src_nodata_values.pop()
                if len(src_nodata_values) == 1
                else None
            )

            # Decide output dtype & nodata
            out_dtype, merge_nodata = self._decide_dtype_and_nodata(
                src_dtype=src_dtype,
                src_nodata=src_nodata,
            )

            # Target CRS = CRS of first dataset
            target_crs = datasets[0].crs
            if target_crs is None:
                raise TileAggregationError(
                    f"First dataset {input_files[0]} has no CRS; cannot choose target CRS."
                )

            # -----------------------------
            # 2) Build merge dataset list with reprojection (WarpedVRT)
            # -----------------------------
            merge_datasets: List[rasterio.io.DatasetReader] = []

            for ds in datasets:
                if ds.crs != target_crs:
                    # Reproject to target_crs via WarpedVRT
                    vrt_kwargs = {
                        "crs": target_crs,
                        "resampling": resampling_enum,
                        "dtype": out_dtype,
                    }
                    if src_nodata is not None:
                        vrt_kwargs["src_nodata"] = src_nodata
                    if merge_nodata is not None:
                        vrt_kwargs["dst_nodata"] = merge_nodata

                    try:
                        vrt = WarpedVRT(ds, **vrt_kwargs)
                    except Exception as e:
                        raise TileAggregationError(
                            f"Failed to create WarpedVRT for dataset {ds.name} "
                            f"to target CRS {target_crs}."
                        ) from e

                    merge_datasets.append(vrt)
                    vrts.append(vrt)
                else:
                    merge_datasets.append(ds)

            # -----------------------------
            # 3) Rasterio merge
            # -----------------------------
            try:
                # Only pass nodata to merge if it is a finite real number.
                merge_kwargs = {
                    "method": self.merge_method,
                    "resampling": resampling_enum,
                }
                if merge_nodata is not None:
                    if not (isinstance(merge_nodata, float) and math.isnan(merge_nodata)):
                        merge_kwargs["nodata"] = merge_nodata

                mosaic, out_transform = merge(
                    merge_datasets,
                    **merge_kwargs,
                )
            except Exception as e:
                raise TileAggregationError(
                    "Raster merge failed. "
                    f"merge_method={self.merge_method!r}, "
                    f"resampling={self.resampling!r}, "
                    f"nodata={merge_nodata!r}, "
                    f"input_files={[str(p) for p in input_files]!r}"
                ) from e

            # Ensure mosaic dtype matches out_dtype
            if mosaic.dtype != np.dtype(out_dtype):
                try:
                    mosaic = mosaic.astype(out_dtype)
                except Exception as e:
                    raise TileAggregationError(
                        f"Failed to cast mosaic array from dtype={mosaic.dtype} "
                        f"to out_dtype={out_dtype!r}."
                    ) from e

        finally:
            # Close VRTs first, then base datasets
            for vrt in vrts:
                try:
                    vrt.close()
                except Exception:
                    pass

            for ds in datasets:
                try:
                    ds.close()
                except Exception:
                    pass

        # -----------------------------
        # 4) Prepare output metadata
        # -----------------------------
        try:
            with rasterio.open(str(input_files[0])) as src0:
                meta = src0.meta.copy()
        except Exception as e:
            raise TileAggregationError(
                f"Failed to read metadata from first input file: {input_files[0]}"
            ) from e

        # Update meta to match mosaic
        meta.update(
            {
                "driver": "GTiff",
                "height": mosaic.shape[1],
                "width": mosaic.shape[2],
                "transform": out_transform,
                "dtype": out_dtype,
                "count": src_band_count,
                "crs": target_crs,
            }
        )

        if merge_nodata is not None:
            meta["nodata"] = merge_nodata
        else:
            meta.pop("nodata", None)

        # -----------------------------
        # 5) Write output
        # -----------------------------
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise TileAggregationError(
                f"Failed to create output directory: {output_path.parent}"
            ) from e

        try:
            with rasterio.open(str(output_path), "w", **meta) as dst:
                dst.write(mosaic)
        except Exception as e:
            raise TileAggregationError(
                f"Failed to write output raster: {output_path}"
            ) from e

    # ------------------------------------------------------------------
    # Internals: dtype + nodata logic
    # ------------------------------------------------------------------
    def _decide_dtype_and_nodata(
        self,
        *,
        src_dtype: str,
        src_nodata: Optional[float],
    ) -> Tuple[str, Optional[float]]:
        requested = self.nodata

        def _is_nan(x: Optional[float]) -> bool:
            return isinstance(x, float) and math.isnan(x)

        src_is_int = np.issubdtype(np.dtype(src_dtype), np.integer)
        src_is_float = np.issubdtype(np.dtype(src_dtype), np.floating)

        # Case 0: no explicit nodata requested → inherit source if possible
        if requested is None:
            return src_dtype, src_nodata

        # Case 1: finite nodata requested → trust the user
        if not _is_nan(requested):
            return src_dtype, requested

        # Case 2: requested is NaN
        if src_is_float:
            return src_dtype, requested

        if src_is_int:
            if self.promote_to_float_if_needed:
                return "float32", requested

            if src_nodata is not None:
                return src_dtype, src_nodata

            raise TileAggregationError(
                "Requested nodata=NaN for integer dtype raster, but "
                "promote_to_float_if_needed=False and source nodata is None. "
                "Please specify a finite nodata value (e.g. 0 or 65535) or "
                "enable promote_to_float_if_needed."
            )

        # Fallback: unknown dtype
        return src_dtype, requested