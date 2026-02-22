import rasterio
import numpy as np

# superpixel raster
superp = rasterio.open("outputs/cogs/superpixels_id.tif").read(1)  
# features (band-major)
feats = rasterio.open("outputs/cogs/pixel_features_7d.tif").read()  # (7,H,W)

mask = superp == 1  # first region
for b in range(7):
    print("band", b, "mean:", np.nanmean(feats[b][mask]))