from pathlib import Path
import os

class RepoPaths:
    ROOT = Path(__file__).resolve().parents[3]

    # Base DATA_LAKE, raw data
    DATA_LAKE = Path(os.environ.get("DATA_LAKE", str(ROOT)))
    DATA_RAW = DATA_LAKE / "data_raw"

    # Geospatial AOI folder
    AOI = ROOT / "aoi"

    # OUTPUTS root
    OUTPUTS = ROOT / "outputs"

    # Subfolders
    TABLES = OUTPUTS / "tables"
    FIGURES = OUTPUTS / "figures"
    TMP = OUTPUTS / "tmp"

    # --- Accessor methods ---
    @staticmethod
    def raw(filename: str) -> Path:
        return RepoPaths.DATA_RAW / filename

    @staticmethod
    def aoi(filename: str) -> Path:
        return RepoPaths.AOI / filename

    @staticmethod
    def table(filename: str) -> Path:
        return RepoPaths.TABLES / filename

    @staticmethod
    def figure(filename: str) -> Path:
        return RepoPaths.FIGURES / filename

    @staticmethod
    def tmp(filename: str) -> Path:
        return RepoPaths.TMP / filename


if __name__ == "__main__":
    print("ROOT:", RepoPaths.ROOT)
    print("TMP:", RepoPaths.TMP)
    print("TABLES:", RepoPaths.TABLES)
    print("FIGURES:", RepoPaths.FIGURES)
