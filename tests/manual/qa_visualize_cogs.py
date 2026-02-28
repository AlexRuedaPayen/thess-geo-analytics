from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Callable

import numpy as np
import rasterio
import matplotlib.pyplot as plt


# -----------------------------
# Low-level plotting helpers
# -----------------------------
def _show_single_band(arr, title: str, vmin=None, vmax=None, cmap="RdYlGn"):
    if vmin is None or vmax is None:
        # Avoid outliers: robust percentiles
        data = arr.compressed() if np.ma.isMaskedArray(arr) else arr.ravel()
        if data.size > 0:
            vmin = np.percentile(data, 2)
            vmax = np.percentile(data, 98)
        else:
            vmin, vmax = -1, 1

    plt.figure(figsize=(8, 6))
    plt.imshow(arr, cmap=cmap, vmin=vmin, vmax=vmax)
    plt.title(title)
    plt.colorbar()
    plt.axis("off")
    plt.tight_layout()
    plt.show()


def show_ndvi_like_raster(path: Path, label: str):
    """Standard NDVI-ish single-band visualization."""
    with rasterio.open(path) as src:
        arr = src.read(1, masked=True)
    title = f"{label} – {path.name}"
    _show_single_band(arr, title=title, cmap="RdYlGn", vmin=-1, vmax=1)


def show_multiband_raster(path: Path, label: str):
    """
    Visualize each band of a multi-band raster (e.g. pixel_features_7d)
    one after another, with a small prompt between bands.
    """
    with rasterio.open(path) as src:
        count = src.count
        for b in range(1, count + 1):
            arr = src.read(b, masked=True)
            title = f"{label} – {path.name} – band {b}/{count}"
            _show_single_band(arr, title=title, cmap="viridis")

            if b < count:
                resp = input(
                    f"[{path.name}] Shown band {b}/{count}. "
                    "Press <Enter> for next band, or 'q' to stop this raster: "
                ).strip().lower()
                if resp == "q":
                    break


# -----------------------------
# Grouping & interactive menu
# -----------------------------
def group_rasters(cogs_dir: Path) -> Dict[str, List[Path]]:
    files = sorted(cogs_dir.glob("*.tif"))

    groups: Dict[str, List[Path]] = {
        "ndvi_quarterly": [],
        "ndvi_anomaly": [],
        "ndvi_climatology": [],
        "pixel_features": [],
        "other": [],
    }

    for p in files:
        name = p.name
        if name.startswith("ndvi_") and "anomaly" not in name and "climatology" not in name:
            groups["ndvi_quarterly"].append(p)
        elif "ndvi_anomaly" in name:
            groups["ndvi_anomaly"].append(p)
        elif "ndvi_climatology" in name:
            groups["ndvi_climatology"].append(p)
        elif "pixel_features_7d" in name:
            groups["pixel_features"].append(p)
        else:
            groups["other"].append(p)

    # Remove empty groups to keep menu clean
    return {k: v for k, v in groups.items() if v}


def ask_groups_to_visualize(groups: Dict[str, List[Path]]) -> List[str]:
    if not groups:
        print("No .tif files found.")
        return []

    print("\nAvailable raster groups:")
    key_map: Dict[str, str] = {}
    for idx, (key, paths) in enumerate(groups.items(), start=1):
        label = {
            "ndvi_quarterly": "NDVI quarterly composites",
            "ndvi_anomaly": "NDVI anomalies",
            "ndvi_climatology": "NDVI climatology median",
            "pixel_features": "Pixel features (7D)",
            "other": "Other rasters",
        }.get(key, key)

        key_str = str(idx)
        key_map[key_str] = key
        print(f"  [{idx}] {label}  (n={len(paths)})")

    print("  [a] All groups")
    choice = input(
        "Enter group numbers separated by commas (e.g. 1,3), "
        "'a' for all, or leave empty to cancel: "
    ).strip()

    if not choice:
        return []

    if choice.lower() == "a":
        return list(groups.keys())

    selected: List[str] = []
    for token in choice.split(","):
        t = token.strip()
        if t in key_map:
            selected.append(key_map[t])

    return selected


# -----------------------------
# Main entry point
# -----------------------------
def main():
    parser = argparse.ArgumentParser(
        description=(
            "Manual QA tool to visualize NDVI COGs and pixel feature rasters.\n"
            "Opens interactive matplotlib windows; close each window to continue."
        )
    )
    parser.add_argument(
        "--cogs-dir",
        type=Path,
        default=Path("outputs/cogs"),
        help="Directory containing output COGs (default: outputs/cogs).",
    )
    args = parser.parse_args()

    if not args.cogs_dir.exists():
        print(f"Directory not found: {args.cogs_dir}")
        return

    groups = group_rasters(args.cogs_dir)
    selected_keys = ask_groups_to_visualize(groups)

    if not selected_keys:
        print("No groups selected. Exiting.")
        return

    print("\nStarting visualization…")

    for key in selected_keys:
        paths = groups[key]
        if not paths:
            continue

        label = {
            "ndvi_quarterly": "NDVI quarterly",
            "ndvi_anomaly": "NDVI anomaly",
            "ndvi_climatology": "NDVI climatology median",
            "pixel_features": "Pixel features (7D)",
            "other": "Other raster",
        }.get(key, key)

        print(f"\n=== Group: {label} (n={len(paths)}) ===")

        for p in paths:
            if key == "pixel_features":
                show_multiband_raster(p, label=label)
            else:
                show_ndvi_like_raster(p, label=label)

            # Small chance to break out of group early
            resp = input(
                "Press <Enter> for next raster, or 'q' to stop this group: "
            ).strip().lower()
            if resp == "q":
                break

    print("\n[QA] Visualization complete.")


if __name__ == "__main__":
    main()