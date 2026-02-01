from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Tuple

import geopandas as gpd
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


    def _load_bbox_from_aoi(self, aoi_path: Path) -> Tuple[BBox, CRS]:
        gdf = gpd.read_file(aoi_path).to_crs(epsg=4326)
        minx, miny, maxx, maxy = gdf.total_bounds
        return BBox(bbox=(minx, miny, maxx, maxy), crs=CRS.WGS84), CRS.WGS84

    def request_ndvi_tiff(self, aoi_path: Path, start: date, end: date, out_tiff: Path, params: NdviRequestParams) -> Path:
        bbox, _ = self._load_bbox_from_aoi(aoi_path)
        size = bbox_to_dimensions(bbox, resolution=params.resolution_m)

        evalscript = """
            //VERSION=3
            function setup() {
            return {
                input: ["B04", "B08", "SCL"],
                output: { bands: 1, sampleType: "FLOAT32" }
            };
            }
            function evaluatePixel(sample) {
            // mask clouds + shadows using SCL:
            // 3 = cloud shadow, 8/9/10 = clouds, 11 = snow (optional)
            if (sample.SCL === 3 || sample.SCL === 8 || sample.SCL === 9 || sample.SCL === 10) {
                return [NaN];
            }
            let ndvi = (sample.B08 - sample.B04) / (sample.B08 + sample.B04);
            return [ndvi];
            }
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

        out_tiff.parent.mkdir(parents=True, exist_ok=True)
        data = request.get_data(save_data=False)[0]  
        request.save_data(folder=str(out_tiff.parent), data_filter="default")

        return out_tiff.parent
