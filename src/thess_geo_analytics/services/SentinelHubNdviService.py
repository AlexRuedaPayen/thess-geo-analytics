from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import numpy as np
from rasterio.transform import from_bounds

from sentinelhub import (
    BBox,
    CRS,
    DataCollection,
    MimeType,
    SentinelHubRequest,
    bbox_to_dimensions,
)

from thess_geo_analytics.services.SentinelHubAuthService import SentinelHubAuthService

@dataclass(frozen=True)
class NdviRequestParams:
    resolution_m: int = 10
    maxcc: float = 0.2 # 20%

class SentinelHubNdviService:
    def __init__(self) -> None:
        self.config = SentinelHubAuthService().build_config()


    def _load_bbox_from_aoi(self, aoi_path: Path) -> BBox:
        gdf = gpd.read_file(aoi_path)
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326)
        else:
            gdf = gdf.to_crs(epsg=4326)

        minx, miny, maxx, maxy = gdf.total_bounds
        return BBox(bbox=(minx, miny, maxx, maxy), crs=CRS.WGS84)

    def request_ndvi_array(
        self,
        aoi_path: Path,
        start: date,
        end: date,
        params: NdviRequestParams,
    ) -> Tuple[np.ndarray, "rasterio.Affine"]:
        bbox = self._load_bbox_from_aoi(aoi_path)
        size = bbox_to_dimensions(bbox, resolution=params.resolution_m)

        nodata = -9999.0
        evalscript = f"""
        //VERSION=3
        function setup() {{
        return {{
            input: ["B04", "B08", "SCL"],
            output: {{ bands: 1, sampleType: "FLOAT32", nodataValue: {nodata} }}
        }};
        }}

        function evaluatePixel(s) {{
        if (s.SCL === 3 || s.SCL === 8 || s.SCL === 9 || s.SCL === 10) {{
            return [{nodata}];
        }}
        let denom = (s.B08 + s.B04);
        if (denom === 0) {{
            return [{nodata}];
        }}
        let ndvi = (s.B08 - s.B04) / denom;
        return [ndvi];
        }}
        """

        request = SentinelHubRequest(
            evalscript=evalscript,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=DataCollection.SENTINEL2_L2A,
                    time_interval=(start.isoformat(), end.isoformat()),
                    maxcc=params.maxcc,
                )
            ],
            responses=[SentinelHubRequest.output_response("default", MimeType.TIFF)],
            bbox=bbox,
            size=size,
            config=self.config,
        )

        arr = request.get_data(save_data=False)[0]
        ndvi = arr[:, :, 0].astype("float32")
        ndvi[ndvi <= -9998] = np.nan  # back to NaN for stats

        transform = from_bounds(
            bbox.min_x, bbox.min_y, bbox.max_x, bbox.max_y,
            ndvi.shape[1], ndvi.shape[0]
        )

        return ndvi, transform
