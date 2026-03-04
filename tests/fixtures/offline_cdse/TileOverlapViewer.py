from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import shape


ROOT = Path("tests/fixtures/offline_cdse").resolve()


def load_aoi():

    with open(ROOT / "aoi.geojson", "r", encoding="utf-8") as f:
        obj = json.load(f)

    if obj["type"] == "FeatureCollection":
        geom = shape(obj["features"][0]["geometry"])
    elif obj["type"] == "Feature":
        geom = shape(obj["geometry"])
    else:
        geom = shape(obj)

    gdf = gpd.GeoDataFrame({"name": ["AOI"]}, geometry=[geom], crs="EPSG:4326")
    return gdf


def load_tiles():

    tiles = gpd.read_file(ROOT / "tiles.geojson")
    tiles = tiles.to_crs("EPSG:4326")

    return tiles


def main():

    aoi = load_aoi()
    tiles = load_tiles()

    # Determine intersecting tiles
    intersects = tiles.intersects(aoi.geometry.iloc[0])
    tiles_hit = tiles[intersects]
    tiles_miss = tiles[~intersects]

    print("Tiles intersecting AOI:", len(tiles_hit))
    print("Tiles not intersecting:", len(tiles_miss))

    fig, ax = plt.subplots(figsize=(8, 8))

    # Non intersecting tiles
    if len(tiles_miss) > 0:
        tiles_miss.plot(ax=ax, facecolor="lightgrey", edgecolor="grey", alpha=0.5)

    # Intersecting tiles
    if len(tiles_hit) > 0:
        tiles_hit.plot(ax=ax, facecolor="orange", edgecolor="black", alpha=0.7)

    # AOI
    aoi.plot(ax=ax, facecolor="none", edgecolor="red", linewidth=2)

    # Label tiles
    for _, row in tiles_hit.iterrows():

        x, y = row.geometry.centroid.coords[0]

        tile_id = row.get("tile_id", "")

        ax.text(
            x,
            y,
            tile_id,
            fontsize=8,
            ha="center",
            va="center",
        )

    ax.set_title("AOI vs Tiles")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    plt.tight_layout()

    out = ROOT / "tiles_vs_aoi.png"

    plt.savefig(out, dpi=200)

    print("Saved figure:", out)

    plt.show()


if __name__ == "__main__":
    main()