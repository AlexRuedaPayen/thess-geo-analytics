from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import geopandas as gpd
from shapely.geometry import shape, box
from shapely.affinity import rotate, translate


@dataclass(frozen=True)
class TileGenConfig:
    # Grid size around AOI centroid
    nx: int = 5
    ny: int = 5

    # Tile footprint size in degrees (synthetic)
    tile_w: float = 0.20
    tile_h: float = 0.20

    # Spacing between tile centers in degrees
    dx: float = 0.12
    dy: float = 0.12

    # Deterministic rotation/jitter
    rotate_deg: float = 7.0
    jitter_deg: float = 0.01

    # Assign overlap classes based on distance to center
    major_radius_cells: int = 1
    seed: int = 1234


def _load_aoi_geom(aoi_geojson: Path):
    obj = json.loads(aoi_geojson.read_text(encoding="utf-8"))
    t = obj.get("type")
    if t == "FeatureCollection":
        return shape(obj["features"][0]["geometry"])
    if t == "Feature":
        return shape(obj["geometry"])
    return shape(obj)


def generate_tiles(aoi_geojson: Path, cfg: TileGenConfig) -> gpd.GeoDataFrame:
    aoi = _load_aoi_geom(aoi_geojson)
    cx, cy = aoi.centroid.x, aoi.centroid.y

    # Centered grid indices
    ix0 = -(cfg.nx // 2)
    iy0 = -(cfg.ny // 2)

    features: List[Dict[str, Any]] = []

    # deterministic pseudo-jitter without random module (stable across python versions)
    def jitter(i: int, j: int) -> Tuple[float, float]:
        # tiny deterministic offsets in [-jitter, +jitter]
        a = ((i * 1315423911) ^ (j * 2654435761) ^ cfg.seed) & 0xFFFFFFFF
        b = ((j * 97531) ^ (i * 864197532) ^ (cfg.seed + 17)) & 0xFFFFFFFF
        jx = (a / 0xFFFFFFFF) * 2 - 1
        jy = (b / 0xFFFFFFFF) * 2 - 1
        return (jx * cfg.jitter_deg, jy * cfg.jitter_deg)

    tile_id_counter = 0
    for iy in range(cfg.ny):
        for ix in range(cfg.nx):
            gi = ix0 + ix
            gj = iy0 + iy

            # base rectangle around origin, then move to (cx,cy) + grid offsets
            geom = box(-cfg.tile_w / 2, -cfg.tile_h / 2, cfg.tile_w / 2, cfg.tile_h / 2)

            # rotate then translate
            geom = rotate(geom, cfg.rotate_deg, origin=(0, 0), use_radians=False)

            offx = gi * cfg.dx
            offy = gj * cfg.dy
            jx, jy = jitter(gi, gj)
            geom = translate(geom, xoff=cx + offx + jx, yoff=cy + offy + jy)

            # overlap class by grid distance to center
            dist_cells = max(abs(gi), abs(gj))
            overlap_class = "major" if dist_cells <= cfg.major_radius_cells else "minor"

            tile_id = f"TILE_{tile_id_counter:03d}"
            tile_id_counter += 1

            features.append(
                {
                    "tile_id": tile_id,
                    "overlap_class": overlap_class,
                    "grid_i": gi,
                    "grid_j": gj,
                    "geometry": geom,
                }
            )

    gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
    return gdf


def write_tiles(aoi_geojson: Path, out_geojson: Path, cfg: TileGenConfig) -> Path:
    out_geojson.parent.mkdir(parents=True, exist_ok=True)
    gdf = generate_tiles(aoi_geojson, cfg)
    gdf.to_file(out_geojson, driver="GeoJSON")
    return out_geojson


if __name__ == "__main__":
    root = Path("tests/fixtures/offline_cdse").resolve()
    aoi_path = root / "aoi.geojson"
    out_path = root / "tiles.geojson"

    if not aoi_path.exists():
        raise FileNotFoundError(f"Missing AOI: {aoi_path}")

    cfg = TileGenConfig()
    p = write_tiles(aoi_path, out_path, cfg)
    print("[OK] wrote:", p)



    ### SANITY CHECK
    root = Path("tests/fixtures/offline_cdse")
    aoi = json.loads((root/"aoi.geojson").read_text())
    geom = shape(aoi["features"][0]["geometry"]) if aoi["type"]=="FeatureCollection" else shape(aoi["geometry"])

    tiles = gpd.read_file(root/"tiles.geojson").to_crs("EPSG:4326")
    hits = tiles[tiles.intersects(geom)]
    print("intersecting tiles:", len(hits))
    print(hits[["tile_id","overlap_class","grid_i","grid_j"]].head(20))