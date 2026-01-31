from pathlib import Path

class RepoPaths:
    """Helper to get absolute paths inside the repo."""
    ROOT = Path(__file__).resolve().parents[2]

    DATA_RAW = ROOT / "data_raw"
    AOI = ROOT / "aoi"

    @staticmethod
    def raw(filename: str) -> Path:
        return RepoPaths.DATA_RAW / filename

    @staticmethod
    def aoi(filename: str) -> Path:
        return RepoPaths.AOI / filename

if __name__ == '__main__':
    print(RepoPaths.raw("NUTS_BN_01M_2024_4326.geojson"))
    print(RepoPaths.aoi("el522_thessaloniki.geojson"))
