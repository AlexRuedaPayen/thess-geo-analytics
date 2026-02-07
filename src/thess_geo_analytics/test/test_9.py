from pathlib import Path
import numpy as np
import pandas as pd
import rasterio
import geopandas as gpd
from rasterio.warp import reproject
from rasterio.enums import Resampling

MONTH = "2026-01"
AOI_ID = "el522"

COG_PATH = Path(f"outputs/cogs/ndvi_{MONTH}_{AOI_ID}.tif")
MANIFEST_PATH = Path(f"outputs/tables/assets_manifest_{MONTH}.csv")
AOI_PATH = Path("aoi/EL522_Thessaloniki.geojson")

CLOUD_SCL_CLASSES = {3, 8, 9, 10}
MAX_SCENES = 9999


def fmt_bool(ok: bool) -> str:
    return "PASS " if ok else "FAIL "


def read_cog_as_nan(path: Path):
    with rasterio.open(path) as ds:
        arr = ds.read(1).astype(float)
        nodata = ds.nodata
        if nodata is not None:
            arr[arr == nodata] = np.nan
        profile = ds.profile.copy()
        bounds = ds.bounds
    return arr, nodata, profile, bounds


def reproject_scl_to_cog_grid(scl_path: Path, cog_profile: dict):
    h = int(cog_profile["height"])
    w = int(cog_profile["width"])
    dst = np.empty((h, w), dtype=np.uint16)

    with rasterio.open(scl_path) as src:
        scl_nodata = src.nodata
        reproject(
            source=rasterio.band(src, 1),
            destination=dst,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=cog_profile["transform"],
            dst_crs=cog_profile["crs"],
            dst_nodata=scl_nodata,
            resampling=Resampling.nearest,
        )

    return dst, scl_nodata


def main():
    print("\n=== test_9 (informal QA) ===")

    # --- basic existence ---
    if not COG_PATH.exists():
        print(f"{fmt_bool(False)} Missing COG: {COG_PATH}")
        return
    if not AOI_PATH.exists():
        print(f"{fmt_bool(False)} Missing AOI: {AOI_PATH}")
        return
    if not MANIFEST_PATH.exists():
        print(f"{fmt_bool(False)} Missing manifest: {MANIFEST_PATH}")
        return

    # --- 1) GeoTIFF loads ---
    try:
        ndvi, nodata, prof, bounds = read_cog_as_nan(COG_PATH)
        ok_load = True
    except Exception as e:
        ok_load = False
        print(f"{fmt_bool(False)} GeoTIFF load: {e}")
        return

    print(f"{fmt_bool(ok_load)} GeoTIFF loads with rasterio")
    print(f"  CRS: {prof.get('crs')}")
    print(f"  Shape: {prof.get('height')} x {prof.get('width')}")
    print(f"  Nodata: {nodata}")
    print(f"  Bounds: {bounds}")

    # --- NDVI range ---
    valid = ndvi[~np.isnan(ndvi)]
    if valid.size == 0:
        print(f"{fmt_bool(False)} NDVI values: all pixels are nodata/NaN")
        return

    mn, mx = float(valid.min()), float(valid.max())
    ok_range = (mn >= -1.0001) and (mx <= 1.0001)
    print(f"{fmt_bool(ok_range)} NDVI values within [-1, 1]")
    print(f"  NDVI min/max: {mn:.4f} .. {mx:.4f}")

    # --- 2) extent matches AOI (bbox coverage) ---
    aoi = gpd.read_file(AOI_PATH)
    aoi = aoi.to_crs(prof["crs"])
    minx, miny, maxx, maxy = aoi.total_bounds

    px_w = abs(prof["transform"].a)
    px_h = abs(prof["transform"].e)
    tol_x = px_w * 1.5
    tol_y = px_h * 1.5

    ok_extent = (
        bounds.left <= minx + tol_x and
        bounds.bottom <= miny + tol_y and
        bounds.right >= maxx - tol_x and
        bounds.top >= maxy - tol_y
    )
    print(f"{fmt_bool(ok_extent)} Raster extent covers AOI bounds (± ~1 pixel tolerance)")
    print(f"  AOI bounds:   ({minx:.2f}, {miny:.2f}, {maxx:.2f}, {maxy:.2f})")

    # --- 3) cloud/shadow masking check (composite-correct) ---
    df = pd.read_csv(MANIFEST_PATH)
    needed = ["local_scl", "local_b04", "local_b08"]
    ok_cols = all(c in df.columns for c in needed)
    print(f"{fmt_bool(ok_cols)} Manifest contains local paths needed for cloud check")
    if not ok_cols:
        print("  Need columns:", needed)
        return

    chosen = []
    for _, r in df.iterrows():
        if Path(r["local_scl"]).exists() and Path(r["local_b04"]).exists() and Path(r["local_b08"]).exists():
            chosen.append(r)
        if len(chosen) >= MAX_SCENES:
            break

    if len(chosen) == 0:
        print("SKIP cloud check: no local scenes found (download assets first).")
        return

    all_cloud = None
    any_cloud_pixels = 0

    for r in chosen:
        scl_path = Path(r["local_scl"])
        scl_grid, scl_nodata = reproject_scl_to_cog_grid(scl_path, prof)

        cloud = np.isin(scl_grid, list(CLOUD_SCL_CLASSES))
        if scl_nodata is not None:
            cloud = cloud | (scl_grid == scl_nodata)

        any_cloud_pixels += int(cloud.sum())
        all_cloud = cloud if all_cloud is None else (all_cloud & cloud)

    if any_cloud_pixels == 0:
        print("SKIP cloud check: no cloud/shadow pixels detected in chosen scenes.")
        return

    all_cloud_pixels = int(all_cloud.sum())
    if all_cloud_pixels == 0:
        print("SKIP cloud check: clouds exist, but not persistently across all chosen scenes.")
        return

    # where all scenes are cloudy => composite should be nodata
    composite_has_value = int(np.sum(all_cloud & ~np.isnan(ndvi)))
    ok_cloud = (composite_has_value == 0)

    print(f"{fmt_bool(ok_cloud)} Cloud/shadow masking: pixels cloudy in ALL scenes are nodata in composite")
    print(f"  all-cloud pixels: {all_cloud_pixels}")
    print(f"  all-cloud pixels with NDVI value (should be 0): {composite_has_value}")

    # --- summary ---
    print("\n=== Summary ===")
    print("GeoTIFF:", fmt_bool(ok_load))
    print("NDVI range:", fmt_bool(ok_range))
    print("Extent:", fmt_bool(ok_extent))
    print("Cloud mask:", "PASS  / SKIP  / FAIL  (see above)")


if __name__ == "__main__":
    main()
