from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import yaml
from shapely.affinity import rotate, translate
from shapely.geometry import box, shape


@dataclass(frozen=True)
class SyntheticGridTileConfig:
    # Grid size around AOI centroid
    nx: int = 5
    ny: int = 5

    # Synthetic footprint size in degrees
    tile_w: float = 0.20
    tile_h: float = 0.20

    # Spacing between centers in degrees
    dx: float = 0.12
    dy: float = 0.12

    # Deterministic transform noise
    rotate_deg: float = 7.0
    jitter_deg: float = 0.01

    # Central tiles marked as "major"
    major_radius_cells: int = 1

    # Deterministic seed
    seed: int = 1234

    @staticmethod
    def from_yaml(path: str | Path) -> "SyntheticGridTileConfig":
        p = Path(path)
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}

        grid = raw.get("grid", {})
        geom = raw.get("geometry", {})

        return SyntheticGridTileConfig(
            nx=int(grid.get("nx", 5)),
            ny=int(grid.get("ny", 5)),
            tile_w=float(geom.get("tile_w", 0.20)),
            tile_h=float(geom.get("tile_h", 0.20)),
            dx=float(geom.get("dx", 0.12)),
            dy=float(geom.get("dy", 0.12)),
            rotate_deg=float(geom.get("rotate_deg", 7.0)),
            jitter_deg=float(geom.get("jitter_deg", 0.01)),
            major_radius_cells=int(grid.get("major_radius_cells", 1)),
            seed=int(raw.get("random_seed", 1234)),
        )


class SyntheticGridTile:
    """
    Shared synthetic footprint grid generator.

    Purpose:
      - build a deterministic set of synthetic acquisition footprints around an AOI
      - reusable for NDVI and VV/VH simulations

    Output columns:
      - tile_id
      - overlap_class
      - grid_i
      - grid_j
      - geometry
    """

    def __init__(self, *, aoi_geojson: str | Path, config: SyntheticGridTileConfig) -> None:
        self.aoi_geojson = Path(aoi_geojson)
        self.config = config


    def load_aoi_geom(self):
        obj = json.loads(self.aoi_geojson.read_text(encoding="utf-8"))
        t = obj.get("type")

        if t == "FeatureCollection":
            return shape(obj["features"][0]["geometry"])
        if t == "Feature":
            return shape(obj["geometry"])
        return shape(obj)


    def _jitter(self, i: int, j: int) -> tuple[float, float]:
        """
        Tiny deterministic offsets in [-jitter_deg, +jitter_deg].
        Avoids dependence on Python random implementation details.
        """
        a = ((i * 1315423911) ^ (j * 2654435761) ^ self.config.seed) & 0xFFFFFFFF
        b = ((j * 97531) ^ (i * 864197532) ^ (self.config.seed + 17)) & 0xFFFFFFFF

        jx = (a / 0xFFFFFFFF) * 2 - 1
        jy = (b / 0xFFFFFFFF) * 2 - 1

        return (jx * self.config.jitter_deg, jy * self.config.jitter_deg)


    def generate(self) -> gpd.GeoDataFrame:
        aoi = self.load_aoi_geom()
        cx, cy = aoi.centroid.x, aoi.centroid.y

        ix0 = -(self.config.nx // 2)
        iy0 = -(self.config.ny // 2)

        features: list[dict[str, Any]] = []
        tile_id_counter = 0

        for iy in range(self.config.ny):
            for ix in range(self.config.nx):
                gi = ix0 + ix
                gj = iy0 + iy

                geom = box(
                    -self.config.tile_w / 2,
                    -self.config.tile_h / 2,
                    self.config.tile_w / 2,
                    self.config.tile_h / 2,
                )

                geom = rotate(
                    geom,
                    self.config.rotate_deg,
                    origin=(0, 0),
                    use_radians=False,
                )

                offx = gi * self.config.dx
                offy = gj * self.config.dy
                jx, jy = self._jitter(gi, gj)

                geom = translate(
                    geom,
                    xoff=cx + offx + jx,
                    yoff=cy + offy + jy,
                )

                dist_cells = max(abs(gi), abs(gj))
                overlap_class = (
                    "major"
                    if dist_cells <= self.config.major_radius_cells
                    else "minor"
                )

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

        return gpd.GeoDataFrame(features, crs="EPSG:4326")


    def write(self, out_geojson: str | Path) -> Path:
        out = Path(out_geojson)
        out.parent.mkdir(parents=True, exist_ok=True)

        gdf = self.generate()
        gdf.to_file(out, driver="GeoJSON")
        return out


if __name__ == "__main__":
    root = Path("tests/fixtures/offline_cdse").resolve()
    aoi_path = root / "aoi.geojson"
    out_path = root / "tiles.geojson"

    if not aoi_path.exists():
        raise FileNotFoundError(f"Missing AOI: {aoi_path}")

    cfg = SyntheticGridTileConfig()
    gen = SyntheticGridTile(aoi_geojson=aoi_path, config=cfg)
    out = gen.write(out_path)

    print("[OK] wrote:", out)

    tiles = gpd.read_file(out).to_crs("EPSG:4326")
    aoi = gen.load_aoi_geom()
    hits = tiles[tiles.intersects(aoi)]

    print("intersecting tiles:", len(hits))
    print(hits[["tile_id", "overlap_class", "grid_i", "grid_j"]].head(20))