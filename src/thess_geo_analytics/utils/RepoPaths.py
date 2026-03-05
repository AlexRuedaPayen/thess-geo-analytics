from __future__ import annotations

import os
from pathlib import Path


class RepoPaths:
    """
    Central path registry.

    Backward-compatible:
      - Keeps legacy constants (ROOT, AOI, OUTPUTS, TABLES, etc.) anchored on repo ROOT
        so older code doesn't break.
      - Adds runtime-resolved "run_root()" and makes helpers (aoi/table/outputs/tmp/figure)
        respect THESS_RUN_ROOT dynamically (so tests can redirect outputs).
    """

    # -----------------------------
    # Repo root (source tree)
    # -----------------------------
    _default_root = Path(__file__).resolve().parents[3]
    ROOT = Path(os.environ.get("THESS_GEO_ROOT", str(_default_root))).resolve()

    # -----------------------------
    # Data lake (keep as before)
    # -----------------------------
    DATA_LAKE = Path(os.environ.get("DATA_LAKE", str(ROOT / "DATA_LAKE"))).resolve()
    DATA_RAW = DATA_LAKE / "data_raw"
    CACHE_S2 = DATA_LAKE / "cache" / "s2"

    # -----------------------------
    # Legacy output locations (repo-root based)
    # -----------------------------
    AOI = ROOT / "aoi"
    OUTPUTS = ROOT / "outputs"
    TABLES = OUTPUTS / "tables"
    FIGURES = OUTPUTS / "figures"
    TMP = OUTPUTS / "tmp"

    # ---------------------------------------------------
    # NEW: runtime-resolved run root for outputs
    # ---------------------------------------------------
    @staticmethod
    def run_root() -> Path:
        """
        If THESS_RUN_ROOT is set, outputs go there.
        Otherwise outputs go to repo ROOT (default).
        """
        return Path(os.environ.get("THESS_RUN_ROOT", str(RepoPaths.ROOT))).resolve()

    # ---------------------------------------------------
    # Preferred helpers (dynamic, respects THESS_RUN_ROOT)
    # ---------------------------------------------------
    @staticmethod
    def aoi(filename: str) -> Path:
        return RepoPaths.run_root() / "aoi" / filename

    @staticmethod
    def outputs(subpath: str = "") -> Path:
        base = RepoPaths.run_root() / "outputs"
        return base / subpath if subpath else base

    @staticmethod
    def table(filename: str) -> Path:
        return RepoPaths.outputs("tables") / filename

    @staticmethod
    def figure(filename: str) -> Path:
        return RepoPaths.outputs("figures") / filename

    @staticmethod
    def tmp(filename: str) -> Path:
        return RepoPaths.outputs("tmp") / filename

    @staticmethod
    def raw(filename: str) -> Path:
        return RepoPaths.DATA_RAW / filename


if __name__ == "__main__":
    print("ROOT:", RepoPaths.ROOT)
    print("RUN_ROOT:", RepoPaths.run_root())
    print("DATA_LAKE:", RepoPaths.DATA_LAKE)
    print("AOI (legacy const):", RepoPaths.AOI)
    print("AOI (dynamic):", RepoPaths.aoi("demo.geojson"))
    print("TABLES (dynamic):", RepoPaths.table("demo.csv"))