from __future__ import annotations

from pathlib import Path
from typing import Optional, Iterable
import geopandas as gpd


class NutsService:
    """
    Data-access service for GISCO NUTS boundaries.
    Owns: loading, CRS normalization, schema validation, query methods.
    Does NOT own: output naming, export paths, pipeline decisions.
    """

    REQUIRED_COLUMNS = {"NUTS_ID", "NAME_LATN", "LEVL_CODE", "CNTR_CODE", "geometry"}

    def __init__(self, nuts_path: Path, target_crs: str = "EPSG:4326"):
        if not nuts_path.exists():
            raise FileNotFoundError(f"NUTS dataset not found: {nuts_path}")

        gdf = gpd.read_file(nuts_path)

        # Schema validation
        missing = self.REQUIRED_COLUMNS - set(gdf.columns)
        if missing:
            raise ValueError(f"NUTS dataset missing columns: {sorted(missing)}")

        # CRS normalization
        if gdf.crs is None:
            gdf = gdf.set_crs(target_crs)
        else:
            gdf = gdf.to_crs(target_crs)

        self.gdf = gdf









    def get_by_code(self, nuts_code: str) -> gpd.GeoDataFrame:
        out = self.gdf[self.gdf["NUTS_ID"] == nuts_code]
        if out.empty:
            raise ValueError(f"NUTS code not found: {nuts_code}")
        return out

    def find_code_by_name_exact(self, region_name: str) -> str:
        s = self.gdf["NAME_LATN"].astype(str).str.lower()
        hits = self.gdf[s == region_name.lower()]
        if hits.empty:
            raise ValueError(f"Region name not found: {region_name}")
        return hits["NUTS_ID"].iloc[0]

 



    def search_by_name_contains(self, text: str, *, limit: int = 10) -> gpd.GeoDataFrame:
        s = self.gdf["NAME_LATN"].astype(str).str.lower()
        hits = self.gdf[s.str.contains(text.lower(), na=False)]
        return hits.head(limit)

    def filter(
            self,
            *,
            level: Optional[int] = None,        # 0/1/2/3
            country: Optional[str] = None,      # "EL", "CY"
            ids: Optional[Iterable[str]] = None # list of NUTS_IDs
        ) -> gpd.GeoDataFrame:
        out = self.gdf

        if level is not None:
            out = out[out["LEVL_CODE"] == int(level)]

        if country is not None:
            out = out[out["CNTR_CODE"] == country.upper()]

        if ids is not None:
            out = out[out["NUTS_ID"].isin(list(ids))]

        return out
    
    
    @staticmethod
    def smoke_test():
        from thess_geo_analytics.utils.RepoPaths import RepoPaths

        print("=== NutsService Smoke Test ===")
        nuts_path = RepoPaths.raw("NUTS_RG_01M_2024_4326.geojson")
        print("Source:", nuts_path)

        svc = NutsService(nuts_path)
        print("Rows:", len(svc.gdf))

        # Test name lookup
        try:
            code = svc.find_code_by_name_exact("Thessaloniki")
            print("Found code:", code)
        except Exception as e:
            print("!! Name lookup error:", e)
            raise

        # Test geometry fetch
        try:
            g = svc.get_by_code(code)
            print("Geometry rows:", len(g))
        except Exception as e:
            print("!! Geometry error:", e)
            raise

        print("✓ Smoke test OK")

if __name__ == "__main__":
    NutsService.smoke_test()