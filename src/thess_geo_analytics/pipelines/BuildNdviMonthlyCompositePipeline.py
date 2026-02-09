from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from thess_geo_analytics.geo.MonthlyCompositeBuilder import MonthlyCompositeBuilder


@dataclass(frozen=True)
class BuildNdviMonthlyCompositeParams:
    month: str
    aoi_path: Path
    aoi_id: str = "el522"


class BuildNdviMonthlyCompositePipeline:
    def __init__(self) -> None:
        pass

    def run(self, params: BuildNdviMonthlyCompositeParams) -> tuple[Path, Path]:
        builder = MonthlyCompositeBuilder(aoi_path=params.aoi_path, aoi_id=params.aoi_id)
        return builder.run(params.month)

    @staticmethod
    def smoke_test() -> None:
        print("=== BuildNdviMonthlyCompositePipeline Smoke Test ===")
        print("[SKIP] Use entrypoint with real AOI+month to execute.")
        print("✓ Smoke test OK")


if __name__ == "__main__":
    BuildNdviMonthlyCompositePipeline.smoke_test()
