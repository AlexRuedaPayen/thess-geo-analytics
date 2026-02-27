from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any, List

import matplotlib.pyplot as plt
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from tests.fixtures.generators.SceneCatalogTestDataGenerator import (
    SceneCatalogTestDataConfig,
    SceneCatalogTestDataGenerator,
)


def _load_preview_geojson(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _plot_polygon(ax, geom: BaseGeometry, **kwargs):
    """
    Plot a Polygon/MultiPolygon on a matplotlib axis.
    """
    if geom.geom_type == "Polygon":
        polys = [geom]
    elif geom.geom_type == "MultiPolygon":
        polys = list(geom.geoms)
    else:
        return

    for poly in polys:
        x, y = poly.exterior.xy
        ax.fill(x, y, **kwargs)


def main() -> None:
    p = argparse.ArgumentParser(
        description=(
            "Visualize AOI and tiles generated for scene catalog tests.\n\n"
            "This script reads the preview GeoJSON produced by "
            "SceneCatalogTestDataGenerator and creates a PNG."
        )
    )
    p.add_argument(
        "--data-dir",
        type=Path,
        default=Path("tests/fixtures/generated/scene_catalog"),
        help="Directory where AOI/items/preview are stored "
             "(default: tests/fixtures/generated/scene_catalog).",
    )
    p.add_argument(
        "--regenerate",
        action="store_true",
        help="If set, regenerate AOI + items before plotting.",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=Path("tests/artifacts/scene_catalog/aoi_tiles_preview.png"),
        help="Path to output PNG (default: tests/artifacts/scene_catalog/aoi_tiles_preview.png).",
    )

    args = p.parse_args()

    # Ensure artifacts dir exists
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Optionally regenerate data using the high-level generator
    if args.regenerate:
        cfg = SceneCatalogTestDataConfig(
            output_dir=args.data_dir,
            n_timestamps=3,
            tiles_per_timestamp=3,
            preview_geojson=True,
            preview_csv=True,
        )
        gen = SceneCatalogTestDataGenerator(cfg)
        gen.run()

    preview_path = args.data_dir / "scene_catalog_preview.geojson"
    if not preview_path.exists():
        raise FileNotFoundError(
            f"Preview GeoJSON not found: {preview_path}\n"
            f"Run with --regenerate or run tests that call SceneCatalogTestDataGenerator first."
        )

    fc = _load_preview_geojson(preview_path)

    aoi_geoms: List[BaseGeometry] = []
    tile_features: List[Dict[str, Any]] = []

    for feat in fc.get("features", []):
        props = feat.get("properties", {}) or {}
        layer = props.get("layer")

        geom = shape(feat.get("geometry"))
        if layer == "aoi":
            aoi_geoms.append(geom)
        elif layer == "tile":
            tile_features.append({"geom": geom, "props": props})

    fig, ax = plt.subplots(figsize=(6, 6))

    # Plot AOI(s)
    for g in aoi_geoms:
        _plot_polygon(ax, g, alpha=0.3, edgecolor="black", facecolor="none", linewidth=2)

    # Plot tiles, colored by tile_type
    tile_type_to_style = {
        "central": dict(alpha=0.4, facecolor="tab:blue", edgecolor="black", linewidth=1),
        "west": dict(alpha=0.4, facecolor="tab:orange", edgecolor="black", linewidth=1),
        "north": dict(alpha=0.4, facecolor="tab:green", edgecolor="black", linewidth=1),
    }

    for t in tile_features:
        geom = t["geom"]
        props = t["props"]
        tile_type = props.get("tile_type", "unknown")
        cloud = props.get("cloud_cover", None)
        style = tile_type_to_style.get(tile_type, dict(alpha=0.3, facecolor="grey", edgecolor="black"))

        _plot_polygon(ax, geom, **style)

        # Optionally drop a small text label at centroid
        cx, cy = geom.centroid.x, geom.centroid.y
        label = tile_type
        if cloud is not None:
            label = f"{tile_type}\n{cloud:.0f}%"
        ax.text(cx, cy, label, fontsize=7, ha="center", va="center")

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Scene Catalog Test Data – AOI & Tiles")

    ax.set_aspect("equal", "box")

    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    print(f"[OK] AOI + tiles preview written to: {args.output}")


if __name__ == "__main__":
    main()