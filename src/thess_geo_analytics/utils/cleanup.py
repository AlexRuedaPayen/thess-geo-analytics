from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Callable, Dict


def _safe_unlink(path: Path) -> None:
    try:
        if path.is_file() or path.is_symlink():
            path.unlink(missing_ok=True)
    except Exception as e:
        print(f"[WARN] Could not remove file {path}: {e}")


def _safe_rmtree(path: Path) -> None:
    try:
        if path.exists():
            shutil.rmtree(path)
    except Exception as e:
        print(f"[WARN] Could not remove directory {path}: {e}")


def clean_outputs() -> None:
    """
    Mirror `make clean`:
      - outputs/tables/*.csv
      - outputs/tables/*.parquet
      - outputs/cogs/*.tif
      - outputs/png/*.png
      - outputs/composites/ (entire dir)
    """
    print("[CLEAN] Removing generated output files...")

    # tables
    tables = Path("outputs/tables")
    if tables.exists():
        for p in tables.glob("*.csv"):
            _safe_unlink(p)
        for p in tables.glob("*.parquet"):
            _safe_unlink(p)

    # cogs
    cogs = Path("outputs/cogs")
    if cogs.exists():
        for p in cogs.glob("*.tif"):
            _safe_unlink(p)

    # pngs
    png_dir = Path("outputs/png")
    if png_dir.exists():
        for p in png_dir.glob("*.png"):
            _safe_unlink(p)

    # composites dir as a whole
    composites = Path("outputs/composites")
    _safe_rmtree(composites)

    print("[CLEAN] Done.")


def clean_hard() -> None:
    """
    Mirror `make clean-hard` (excluding clean_outputs, which Makefile already calls):
      - aoi/*.geojson
      - cache/s2
      - cache/s2_downloads
      - cache/nuts
    """
    print("[CLEAN HARD] Removing AOI cache, scene cache, raw S2 downloads...")

    aoi = Path("aoi")
    if aoi.exists():
        for p in aoi.glob("*.geojson"):
            _safe_unlink(p)

    _safe_rmtree(Path("cache/s2"))
    _safe_rmtree(Path("cache/s2_downloads"))
    _safe_rmtree(Path("cache/nuts"))

    print("[CLEAN HARD] All cached data removed.")


def clean_cache_s2() -> None:
    """
    Mirror `make clean-cache-s2`:
      - DATA_LAKE/cache/s2
    """
    print("[CLEAN] Removing Sentinel-2 cache (DATA_LAKE/cache/s2)...")
    _safe_rmtree(Path("DATA_LAKE/cache/s2"))
    print("[CLEAN] Done.")


def clean_aggregated_raw() -> None:
    """
    Mirror `make clean-aggregated-raw`:
      - DATA_LAKE/data_raw/aggregated
    """
    print("[CLEAN] Removing aggregated raw rasters (DATA_LAKE/data_raw/aggregated)...")
    _safe_rmtree(Path("DATA_LAKE/data_raw/aggregated"))
    print("[CLEAN] Done.")


MODES: Dict[str, Callable[[], None]] = {
    "outputs": clean_outputs,
    "hard": clean_hard,
    "cache_s2": clean_cache_s2,
    "aggregated_raw": clean_aggregated_raw,
}


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    if not argv or argv[0] not in MODES:
        print("Usage: python -m thess_geo_analytics.tools.cleanup "
              "[outputs|hard|cache_s2|aggregated_raw]")
        raise SystemExit(1)

    mode = argv[0]
    MODES[mode]()


if __name__ == "__main__":
    main()