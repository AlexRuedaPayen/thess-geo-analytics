from __future__ import annotations

from pathlib import Path
import geopandas as gpd


class AoiBuilder:
    """Build and export AOI geometries."""

    def build_aoi(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # Dissolve into single geometry to make it a clean AOI artifact
        geom = gdf.unary_union
        return gpd.GeoDataFrame(geometry=[geom], crs=gdf.crs)

    def export_geojson(self, gdf: gpd.GeoDataFrame, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(destination, driver="GeoJSON")
        return destination
