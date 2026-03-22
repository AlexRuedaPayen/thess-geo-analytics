from __future__ import annotations

import os
from pathlib import Path


class RepoPaths:
    """
    Central path registry.

    Backward-compatible:
      - Keeps legacy constants (ROOT, AOI, OUTPUTS, TABLES, etc.) anchored on repo ROOT
        so older code doesn't break.
      - Adds runtime-resolved "run_root()" and makes helpers respect THESS_RUN_ROOT dynamically.
      - Adds new step-aware helpers for the new pipeline architecture.
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
    # Runtime-resolved run root for outputs
    # ---------------------------------------------------
    @staticmethod
    def run_root() -> Path:
        """
        If THESS_RUN_ROOT is set, outputs go there.
        Otherwise outputs go to repo ROOT (default).
        """
        return Path(os.environ.get("THESS_RUN_ROOT", str(RepoPaths.ROOT))).resolve()

    # ---------------------------------------------------
    # Generic mkdir helper
    # ---------------------------------------------------
    @staticmethod
    def _ensure_dir(p: Path) -> Path:
        p.mkdir(parents=True, exist_ok=True)
        return p

    # ---------------------------------------------------
    # Preferred helpers (dynamic, respects THESS_RUN_ROOT)
    # ---------------------------------------------------
    @staticmethod
    def aoi(filename: str) -> Path:
        base = RepoPaths._ensure_dir(RepoPaths.run_root() / "aoi")
        return base / filename

    @staticmethod
    def outputs(subpath: str = "") -> Path:
        base = RepoPaths._ensure_dir(RepoPaths.run_root() / "outputs")
        if not subpath:
            return base
        return base / subpath

    @staticmethod
    def table(filename: str) -> Path:
        base = RepoPaths._ensure_dir(RepoPaths.outputs("tables"))
        return base / filename

    @staticmethod
    def figure(filename: str) -> Path:
        base = RepoPaths._ensure_dir(RepoPaths.outputs("figures"))
        return base / filename

    @staticmethod
    def tmp(filename: str) -> Path:
        base = RepoPaths._ensure_dir(RepoPaths.outputs("tmp"))
        return base / filename

    @staticmethod
    def raw(filename: str) -> Path:
        return RepoPaths.DATA_RAW / filename

    # ---------------------------------------------------
    # NEW: step-based architecture helpers
    # ---------------------------------------------------
    @staticmethod
    def steps_dir() -> Path:
        return RepoPaths._ensure_dir(RepoPaths.run_root() / "steps")

    @staticmethod
    def step_dir(step_name: str, modality: str | None = None) -> Path:
        p = RepoPaths.steps_dir() / step_name
        if modality:
            p = p / modality
        return RepoPaths._ensure_dir(p)

    @staticmethod
    def step_file(step_name: str, relpath: str, modality: str | None = None) -> Path:
        p = RepoPaths.step_dir(step_name, modality) / relpath
        p.parent.mkdir(parents=True, exist_ok=True)
        return p


if __name__ == "__main__":
    print("ROOT:", RepoPaths.ROOT)
    print("RUN_ROOT:", RepoPaths.run_root())
    print("DATA_LAKE:", RepoPaths.DATA_LAKE)
    print("AOI (legacy const):", RepoPaths.AOI)
    print("AOI (dynamic):", RepoPaths.aoi("demo.geojson"))
    print("TABLES (dynamic):", RepoPaths.table("demo.csv"))
    print("STEP DIR:", RepoPaths.step_dir("02_scene_catalog", "ndvi"))
    print("STEP FILE:", RepoPaths.step_file("02_scene_catalog", "time_series.csv", "ndvi"))