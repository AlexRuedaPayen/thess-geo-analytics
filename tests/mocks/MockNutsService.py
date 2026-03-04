from __future__ import annotations

from pathlib import Path
import geopandas as gpd
from shapely.geometry import box

class MockNutsService:
    """
    Drop-in replacement for NutsService used in ExtractAoiPipeline.
    """
    def __init__(self, nuts_path: Path | None = None, target_crs: str = "EPSG:4326"):
        # Create a single fake region polygon
        geom = box(22.85, 40.55, 23.05, 40.75)  # Thessaloniki-ish bbox
        self.gdf = gpd.GeoDataFrame(
            {
                "NUTS_ID": ["EL522"],
                "NAME_LATN": ["Thessaloniki"],
                "LEVL_CODE": [3],
                "CNTR_CODE": ["EL"],
                "geometry": [geom],
            },
            crs=target_crs,
        )

    def find_code_by_name_exact(self, region_name: str) -> str:
        if region_name.lower() != "thessaloniki":
            raise ValueError("Region name not found in MockNutsService")
        return "EL522"

    def get_by_code(self, nuts_code: str):
        out = self.gdf[self.gdf["NUTS_ID"] == nuts_code]
        if out.empty:
            raise ValueError("NUTS code not found in MockNutsService")
        return out