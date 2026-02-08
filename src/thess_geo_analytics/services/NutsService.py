from __future__ import annotations

from pathlib import Path
import requests
import geopandas as gpd

from thess_geo_analytics.core.constants import (
    GISCO_NUTS_URL,
    HTTP_TIMEOUT,
    DEFAULT_NUTS_FILENAME,
)
from thess_geo_analytics.core.settings import (
    DATA_RAW_DIR,
    NUTS_LOCAL_PATH,
    AUTO_DOWNLOAD_GISCO,
)


class NutsService:
    """
    Data-access service for GISCO NUTS boundaries.
    Responsibilities:
      • Ensure file exists (auto-download if needed)
      • Load/normalize CRS
      • Validate schema
      • Provide lookup/filter/query utilities

    Does NOT own:
      • File naming
      • Downstream outputs
      • Pipeline logic
    """

    REQUIRED_COLUMNS = {"NUTS_ID", "NAME_LATN", "LEVL_CODE", "CNTR_CODE", "geometry"}

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    def __init__(self, nuts_path: Path | None = None, target_crs: str = "EPSG:4326"):
        self.nuts_path = nuts_path or NUTS_LOCAL_PATH

        if not self.nuts_path.exists():
            if AUTO_DOWNLOAD_GISCO:
                self._download_gisco()
            else:
                raise FileNotFoundError(f"NUTS dataset missing: {self.nuts_path}")

        self.gdf = self._load_and_normalize(target_crs)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------
    def _download_gisco(self) -> None:
        """Download GISCO NUTS boundaries into DATA_RAW_DIR."""
        print(f"[INFO] NUTS not found → Downloading from GISCO…")
        DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

        r = requests.get(GISCO_NUTS_URL, timeout=HTTP_TIMEOUT)
        r.raise_for_status()

        self.nuts_path.write_bytes(r.content)

        size_mb = self.nuts_path.stat().st_size / (1024 * 1024)
        print(f"[INFO] Saved {self.nuts_path.name} ({size_mb:.2f} MB)")

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------
    def _load_and_normalize(self, target_crs: str) -> gpd.GeoDataFrame:
        gdf = gpd.read_file(self.nuts_path)

        # Schema validation
        missing = self.REQUIRED_COLUMNS - set(gdf.columns)
        if missing:
            raise ValueError(f"NUTS dataset missing columns: {sorted(missing)}")

        # CRS normalization
        if gdf.crs is None:
            gdf = gdf.set_crs(target_crs)
        else:
            gdf = gdf.to_crs(target_crs)

        return gdf

    # ------------------------------------------------------------------
    # Lookup utilities
    # ------------------------------------------------------------------
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
        level: int | None = None,
        country: str | None = None,
        ids: list[str] | None = None,
    ) -> gpd.GeoDataFrame:

        out = self.gdf

        if level is not None:
            out = out[out["LEVL_CODE"] == int(level)]

        if country is not None:
            out = out[out["CNTR_CODE"] == country.upper()]

        if ids is not None:
            out = out[out["NUTS_ID"].isin(ids)]

        return out

    # ------------------------------------------------------------------
    # Smoke test (for development only)
    # ------------------------------------------------------------------
    @staticmethod
    def smoke_test() -> None:
        print("=== NutsService Smoke Test ===")

        svc = NutsService()  # uses default path + auto-download
        print("Rows loaded:", len(svc.gdf))

        # Test lookup
        try:
            code = svc.find_code_by_name_exact("Thessaloniki")
            print("Found code:", code)
        except Exception as e:
            print("!! ERROR resolving region:", e)
            raise

        # Test geometry access
        try:
            gdf = svc.get_by_code(code)
            print("Geometry rows:", len(gdf))
        except Exception as e:
            print("!! ERROR loading geometry:", e)
            raise

        print("✓ Smoke test OK")

# Allow quick test: python -m thess_geo_analytics.services.NutsService
if __name__ == "__main__":
    NutsService.smoke_test()
