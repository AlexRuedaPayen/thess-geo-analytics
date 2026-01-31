import geopandas as gpd
from pathlib import Path

class NutsExtractor:
    """Extract NUTS regions (Eurostat GISCO) and export subsets as GeoJSON."""

    def __init__(self, source: Path):
        if not source.exists():
            raise FileNotFoundError(f"NUTS file not found: {source}")
        self.source = source
        self.gdf = None

    def load(self) -> "NutsExtractor":
        gdf = gpd.read_file(self.source)

        # Ensure WGS84 for geo standards
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        else:
            gdf = gdf.to_crs("EPSG:4326")

        self.gdf = gdf
        return self
    

    def filter_by(self, key:str,  val: str) -> gpd.GeoDataFrame:
        if self.gdf is None:
            raise RuntimeError("Call .load() before filtering.")


        subset = self.gdf[self.gdf[key] == val]
        if subset.empty:
            raise ValueError(f"NUTS code '{val}' not found in dataset.")
        return subset
    
    def filter_by_code(self, nuts_code: str) -> gpd.GeoDataFrame:
        return self.filter_by(key="NUTS_ID",  val=nuts_code)

    def filter_by_name(self, region_name: str) -> str:
        if self.gdf is None:
            raise RuntimeError("Call .load() before filtering.")

        s = self.gdf["NAME_LATN"].astype(str).str.lower()
        hits = self.gdf[s == region_name.lower()]
        if hits.empty:
            raise ValueError(f"Region name '{region_name}' not found in dataset.")
        return hits["NUTS_ID"].iloc[0]

    def export(self, gdf: gpd.GeoDataFrame, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(destination, driver="GeoJSON")
        return destination