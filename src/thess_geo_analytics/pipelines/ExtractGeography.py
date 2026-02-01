import sys
import re
from pathlib import Path

import geopandas as gpd

from thess_geo_analytics.RepoPaths import RepoPaths
from thess_geo_analytics.NutsExtractor import NutsExtractor


def slugify(text: str) -> str:
    text = text.strip()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^A-Za-z0-9_\-]+", "", text)
    return text


class ExtractGeography:
    def __init__(self, region_name: str):
        self.region_name = region_name

    def find_nuts_code(self) -> str:
       
        nuts_file = RepoPaths.raw("NUTS_RG_01M_2024_4326.geojson")

        if not nuts_file.exists():
            raise FileNotFoundError(f"Missing boundary file: {nuts_file}")
        
        extractor= NutsExtractor(nuts_file).load()
        return extractor.filter_by_name(self.region_name)

        
    def run(self) -> Path:
        nuts_code = self.find_nuts_code()

        nuts_file = RepoPaths.raw("NUTS_RG_01M_2024_4326.geojson")
        if not nuts_file.exists():
            raise FileNotFoundError(f"Missing boundary file: {nuts_file}")

        out_name = f"{nuts_code}_{slugify(self.region_name)}.geojson"
        output_file = RepoPaths.aoi(out_name)

        extractor = NutsExtractor(nuts_file).load()

        gdf = extractor.filter_by_code(nuts_code)
        extractor.export(gdf, output_file)

        print(f"AOI extracted for {nuts_code} ({self.region_name})")
        print(f"Saved to: {output_file}")
        return output_file


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m thess_geo_analytics.extract_geography <RegionName>")

    region = sys.argv[1]
    ExtractGeography(region).run()


if __name__ == "__main__":
    main()
