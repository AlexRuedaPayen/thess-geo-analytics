# tests/auto/unit/test_NdviAggregatedCompositeBuilder.py

from __future__ import annotations

import shutil
import unittest
from pathlib import Path

import numpy as np
import rasterio

from thess_geo_analytics.builders.NdviAggregatedCompositeBuilder import (
    NdviAggregatedCompositeBuilder,
)
from thess_geo_analytics.utils.RepoPaths import RepoPaths
from tests.fixtures.generators.MiniAggregatedTimestampsGenerator import (
    MiniAggregatedTimestampsConfig,
    MiniAggregatedTimestampsGenerator,
)


class RepoPathsOverride:
    """
    Context manager to temporarily redirect RepoPaths.OUTPUTS/TABLES/FIGURES
    to a test root under tests/fixtures/generated.
    """

    def __init__(self, new_root: Path) -> None:
        self.new_root = new_root.resolve()
        self._orig_outputs = RepoPaths.OUTPUTS
        self._orig_tables = RepoPaths.TABLES
        self._orig_figures = RepoPaths.FIGURES

    def __enter__(self):
        RepoPaths.OUTPUTS = self.new_root / "outputs"
        RepoPaths.TABLES = RepoPaths.OUTPUTS / "tables"
        RepoPaths.FIGURES = RepoPaths.OUTPUTS / "figures"
        RepoPaths.OUTPUTS.mkdir(parents=True, exist_ok=True)
        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)
        RepoPaths.FIGURES.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc, tb):
        RepoPaths.OUTPUTS = self._orig_outputs
        RepoPaths.TABLES = self._orig_tables
        RepoPaths.FIGURES = self._orig_figures


class NdviAggregatedCompositeBuilderTest(unittest.TestCase):
    """
    E2E-ish test for NdviAggregatedCompositeBuilder:

      aggregated/<timestamp>/B04.tif,B08.tif
        → NDVI composites in outputs/cogs/
        → status/summary tables in outputs/tables/

    All artifacts are written under:
      tests/fixtures/generated/ndvi_aggregated/outputs/...
    and are **kept** after the test completes.
    """

    def setUp(self) -> None:
        # Fresh test root for each run, but do NOT delete it after the test.
        self.test_root = Path("tests/fixtures/generated/ndvi_aggregated").resolve()
        if self.test_root.exists():
            shutil.rmtree(self.test_root)
        self.test_root.mkdir(parents=True, exist_ok=True)

    # NOTE: no tearDown -> artifacts are kept

    def _write_dummy_aoi(self) -> Path:
        """
        Tiny AOI polygon in EPSG:4326 for AoiTargetGrid.
        """
        aoi_dir = self.test_root / "data"
        aoi_dir.mkdir(parents=True, exist_ok=True)
        aoi_path = aoi_dir / "aoi_minimal.geojson"

        geojson = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [23.0, 40.5],
                        [23.1, 40.5],
                        [23.1, 40.6],
                        [23.0, 40.6],
                        [23.0, 40.5],
                    ]
                ],
            },
            "properties": {},
        }

        import json

        with aoi_path.open("w", encoding="utf-8") as f:
            json.dump(geojson, f)

        return aoi_path

    def test_ndvi_composites_from_aggregated(self) -> None:
        timestamps = [
            "2024-01-10T10:00:00Z",
            "2024-01-20T10:00:00Z",
            "2024-02-05T10:00:00Z",
        ]

        # 1) Create minimal aggregated input
        agg_cfg = MiniAggregatedTimestampsConfig(
            root=self.test_root,
            timestamps=timestamps,
            height=8,
            width=8,
        )
        agg_gen = MiniAggregatedTimestampsGenerator(agg_cfg)
        aggregated_root = agg_gen.generate()

        # 2) Dummy AOI
        aoi_path = self._write_dummy_aoi()
        aoi_id = "el522"

        # 3) Run builder with RepoPaths redirected to test_root
        with RepoPathsOverride(self.test_root):
            builder = NdviAggregatedCompositeBuilder(
                aoi_path=aoi_path,
                aoi_id=aoi_id,
            )

            results = builder.run_monthly_with_fallback(
                aggregated_root=aggregated_root,
                max_scenes=None,
                min_scenes=1,
                fallback=False,
                enable_cloud_masking=False,  # avoid SCL dependence
                verbose=False,
                max_workers=1,
                debug=True,  # raises on errors for easier debugging
            )

            # At least one composite should be produced
            self.assertGreaterEqual(len(results), 1)

            # Check that NDVI COGs exist and have reasonable values
            for label, tif_path, meta_path in results:
                self.assertTrue(tif_path.exists(), f"Missing NDVI COG: {tif_path}")
                self.assertTrue(meta_path.exists(), f"Missing metadata: {meta_path}")

                with rasterio.open(tif_path) as ds:
                    arr = ds.read(1)
                    valid = arr[arr != ds.nodata]
                    if valid.size > 0:
                        self.assertTrue(
                            np.all(valid >= -1.0) and np.all(valid <= 1.0),
                            "NDVI values out of expected range [-1, 1]",
                        )

            # Check status/summary CSVs via the (overridden) RepoPaths
            from thess_geo_analytics.utils.RepoPaths import RepoPaths as RP2

            status_csv = RP2.table("ndvi_aggregated_composites_status.csv")
            summary_csv = RP2.table("ndvi_aggregated_composites_summary.csv")

            self.assertTrue(status_csv.exists(), f"Missing status CSV: {status_csv}")
            self.assertTrue(summary_csv.exists(), f"Missing summary CSV: {summary_csv}")


if __name__ == "__main__":
    unittest.main()