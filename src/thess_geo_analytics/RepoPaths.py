from pathlib import Path
import os

class RepoPaths:
    ROOT = Path(__file__).resolve().parents[2]

    DATA_LAKE = Path(os.environ.get("DATA_LAKE", str(ROOT)))
    DATA_RAW = DATA_LAKE / "data_raw"

    AOI = ROOT / "aoi"
    TABLES = ROOT / "outputs" / "tables"
    FIGURES = ROOT / "outputs" / "figures"

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

if __name__ == '__main__':
    print(RepoPaths.raw("NUTS_BN_01M_2024_4326.geojson"))
    print(RepoPaths.aoi("el522_thessaloniki.geojson"))
