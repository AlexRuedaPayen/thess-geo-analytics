import os
from pathlib import Path
import numpy as np
import rasterio

TMP_DIR = Path("outputs/tmp")  # adjust if needed


def check_ndvi_range(arr):
    valid = arr[~np.isnan(arr)]
    if valid.size == 0:
        return False, "ALL_PIXELS_MASKED"
    mn, mx = float(valid.min()), float(valid.max())
    ok = (-1.0 <= mn <= mx <= 1.0)
    return ok, (mn, mx)


def check_nodata_fraction(arr):
    total = arr.size
    nodata = np.isnan(arr).sum()
    frac = nodata / total
    return frac


def check_file(path: Path):
    print(f"\n=== Checking {path.name} ===")

    with rasterio.open(path) as ds:
        ndvi = ds.read(1).astype(float)
        ndvi[ndvi == ds.nodata] = np.nan

    # 1) Range check
    ok_range, info = check_ndvi_range(ndvi)
    if ok_range:
        print(f"NDVI range OK: min={info[0]:.4f}, max={info[1]:.4f}")
    else:
        print(f"NDVI range FAIL: {info}")

    # 2) Masking check (clouds/shadows = NaN)
    nodata_frac = check_nodata_fraction(ndvi)
    print(f"ℹ masked fraction (nodata): {nodata_frac:.4%}")

    if nodata_frac < 0.01:
        print("WARNING: almost no masked pixels — SCL masking might not be applied correctly")
    else:
        print("masking seems plausible")

    return ok_range


def main():
    if not TMP_DIR.exists():
        print(f"Directory {TMP_DIR} does not exist.")
        return

    tifs = list(TMP_DIR.glob("ndvi_scene_*.tif"))
    if not tifs:
        print("No NDVI scene TIFFs found in outputs/tmp/")
        return

    print(f"Found {len(tifs)} NDVI scenes.\n")

    all_ok = True
    for tif in tifs:
        if not check_file(tif):
            all_ok = False

    print("\n=== Summary ===")
    if all_ok:
        print("ALL NDVI scenes passed the acceptance criteria.")
    else:
        print("Some NDVI scenes failed. Investigate values above.")


if __name__ == "__main__":
    main()
