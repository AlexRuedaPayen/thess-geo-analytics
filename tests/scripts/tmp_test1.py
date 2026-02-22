import rasterio
import numpy as np

fp = "outputs/cogs/pixel_features_7d.tif"

with rasterio.open(fp) as ds:
    print(ds.count)
    arr = ds.read()  # shape (7, H, W)
    print("min:", np.nanmin(arr))
    print("max:", np.nanmax(arr))
    print("unique:", np.unique(np.isnan(arr)))