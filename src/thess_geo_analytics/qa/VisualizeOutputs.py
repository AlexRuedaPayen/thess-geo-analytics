from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import numpy as np
import rasterio


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Manual QA tool to visualize output rasters.\n"
            "Two modes are available:\n"
            "  - interactive mode: opens matplotlib windows\n"
            "  - headless mode: saves PNG previews to disk (recommended in Docker)"
        )
    )
    parser.add_argument(
        "--cogs-dir",
        type=Path,
        default=Path("outputs/cogs"),
        help="Directory containing output COGs (default: outputs/cogs).",
    )
    parser.add_argument(
        "--save-previews",
        action="store_true",
        help="Save PNG previews instead of opening interactive windows.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("outputs/figures"),
        help="Directory where PNG previews are written in headless mode.",
    )
    parser.add_argument(
        "--no-prompt",
        action="store_true",
        help="Disable interactive prompts and process everything automatically.",
    )
    return parser.parse_args()


ARGS = parse_args()

if ARGS.save_previews:
    import matplotlib

    matplotlib.use("Agg")

import matplotlib.pyplot as plt


# -----------------------------
# Low-level plotting helpers
# -----------------------------
def _compute_vrange(arr: np.ndarray | np.ma.MaskedArray, vmin=None, vmax=None):
    if vmin is not None and vmax is not None:
        return vmin, vmax

    data = arr.compressed() if np.ma.isMaskedArray(arr) else arr.ravel()
    if data.size == 0:
        return -1, 1

    if vmin is None:
        vmin = np.percentile(data, 2)
    if vmax is None:
        vmax = np.percentile(data, 98)

    return vmin, vmax


def _slugify_filename(name: str) -> str:
    safe = []
    for ch in name:
        if ch.isalnum() or ch in ("-", "_", "."):
            safe.append(ch)
        elif ch in (" ", "/", "\\", ":", "–"):
            safe.append("_")
    return "".join(safe).strip("_")


def _render_single_band(
    arr: np.ndarray | np.ma.MaskedArray,
    title: str,
    *,
    cmap: str = "RdYlGn",
    vmin=None,
    vmax=None,
    save_path: Path | None = None,
) -> None:
    vmin, vmax = _compute_vrange(arr, vmin=vmin, vmax=vmax)

    plt.figure(figsize=(8, 6))
    plt.imshow(arr, cmap=cmap, vmin=vmin, vmax=vmax)
    plt.title(title)
    plt.colorbar()
    plt.axis("off")
    plt.tight_layout()

    if save_path is not None:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"[QA] Saved preview: {save_path}")
    else:
        plt.show()
        plt.close()


def show_ndvi_like_raster(
    path: Path,
    label: str,
    *,
    save_previews: bool = False,
    out_dir: Path | None = None,
) -> None:
    with rasterio.open(path) as src:
        arr = src.read(1, masked=True)

    title = f"{label} - {path.name}"
    save_path = None
    if save_previews:
        assert out_dir is not None
        save_name = _slugify_filename(path.stem) + ".png"
        save_path = out_dir / save_name

    _render_single_band(
        arr,
        title=title,
        cmap="RdYlGn",
        vmin=-1,
        vmax=1,
        save_path=save_path,
    )


def show_multiband_raster(
    path: Path,
    label: str,
    *,
    save_previews: bool = False,
    out_dir: Path | None = None,
    no_prompt: bool = False,
) -> None:
    with rasterio.open(path) as src:
        count = src.count

        for b in range(1, count + 1):
            arr = src.read(b, masked=True)
            title = f"{label} - {path.name} - band {b}/{count}"

            save_path = None
            if save_previews:
                assert out_dir is not None
                save_name = _slugify_filename(f"{path.stem}_band_{b}") + ".png"
                save_path = out_dir / save_name

            _render_single_band(
                arr,
                title=title,
                cmap="viridis",
                save_path=save_path,
            )

            if save_previews or no_prompt:
                continue

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
        elif "pixel_features" in name:
            groups["pixel_features"].append(p)
        else:
            groups["other"].append(p)

    return {k: v for k, v in groups.items() if v}


def ask_groups_to_visualize(groups: Dict[str, List[Path]], *, no_prompt: bool = False) -> List[str]:
    if not groups:
        print("No .tif files found.")
        return []

    if no_prompt:
        return list(groups.keys())

    print("\nAvailable raster groups:")
    key_map: Dict[str, str] = {}

    for idx, (key, paths) in enumerate(groups.items(), start=1):
        label = {
            "ndvi_quarterly": "NDVI quarterly composites",
            "ndvi_anomaly": "NDVI anomalies",
            "ndvi_climatology": "NDVI climatology median",
            "pixel_features": "Pixel features",
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
def main() -> None:
    if not ARGS.cogs_dir.exists():
        print(f"Directory not found: {ARGS.cogs_dir}")
        return

    if ARGS.save_previews:
        ARGS.out_dir.mkdir(parents=True, exist_ok=True)

    groups = group_rasters(ARGS.cogs_dir)
    selected_keys = ask_groups_to_visualize(groups, no_prompt=ARGS.no_prompt or ARGS.save_previews)

    if not selected_keys:
        print("No groups selected. Exiting.")
        return

    mode_label = "headless preview export" if ARGS.save_previews else "interactive visualization"
    print(f"\nStarting {mode_label}...")

    for key in selected_keys:
        paths = groups[key]
        if not paths:
            continue

        label = {
            "ndvi_quarterly": "NDVI quarterly",
            "ndvi_anomaly": "NDVI anomaly",
            "ndvi_climatology": "NDVI climatology median",
            "pixel_features": "Pixel features",
            "other": "Other raster",
        }.get(key, key)

        print(f"\n=== Group: {label} (n={len(paths)}) ===")

        for p in paths:
            group_out_dir = ARGS.out_dir / key if ARGS.save_previews else None

            if key == "pixel_features":
                show_multiband_raster(
                    p,
                    label=label,
                    save_previews=ARGS.save_previews,
                    out_dir=group_out_dir,
                    no_prompt=ARGS.no_prompt or ARGS.save_previews,
                )
            else:
                show_ndvi_like_raster(
                    p,
                    label=label,
                    save_previews=ARGS.save_previews,
                    out_dir=group_out_dir,
                )

            if ARGS.save_previews or ARGS.no_prompt:
                continue

            resp = input(
                "Press <Enter> for next raster, or 'q' to stop this group: "
            ).strip().lower()
            if resp == "q":
                break

    print("\n[QA] Visualization complete.")


if __name__ == "__main__":
    main()