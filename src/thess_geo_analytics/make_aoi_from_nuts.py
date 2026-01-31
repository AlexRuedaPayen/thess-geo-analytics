import geopandas as gpd
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2] 

gdf = gpd.read_file("data_raw/NUTS_RG_01M_2024_4326.geojson")
thessaloniki_nuts_code=gdf[gdf["NAME_LATN"] == "Thessaloniki"][["NUTS_ID","NAME_LATN","LEVL_CODE"]].NUTS_ID.iloc[0]




INPUT = Path("data_raw/NUTS_RG_01M_2024_4326.geojson")

OUTPUT = Path(f"aoi/{thessaloniki_nuts_code}_thessaloniki.geojson")

def main():
    print("Loading NUTS dataset...")
    gdf = gpd.read_file(INPUT)

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    print(f"Filtering for {thessaloniki_nuts_code}...")
    gdf_thess = gdf[gdf["NUTS_ID"] == thessaloniki_nuts_code]

    if gdf_thess.empty:
        raise ValueError(f"{thessaloniki_nuts_code} not found in the NUTS dataset. Check dataset version or file path.")

    print("Saving AOI to", OUTPUT)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    gdf_thess.to_file(OUTPUT, driver="GeoJSON")
    
    print("Done! AOI saved.")

if __name__ == "__main__":
    main()