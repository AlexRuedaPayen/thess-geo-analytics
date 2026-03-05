from __future__ import annotations

import os
import shutil
import unittest
from pathlib import Path

import pandas as pd
import rasterio

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.pipelines.ExtractAoiPipeline import ExtractAoiPipeline
from thess_geo_analytics.pipelines.BuildSceneCatalogPipeline import (
    BuildSceneCatalogPipeline,
    BuildSceneCatalogParams,
)
from thess_geo_analytics.pipelines.BuildAssetsManifestPipeline import (
    BuildAssetsManifestPipeline,
    BuildAssetsManifestParams,
)
from thess_geo_analytics.builders.TimestampsAggregationBuilder import (
    TimestampsAggregationBuilder,
    TimestampsAggregationParams,
)
from thess_geo_analytics.pipelines.BuildDownsampledAggregatedTimestampsPipeline import (
    BuildDownsampledAggregatedTimestampsPipeline,
    BuildDownsampledAggregatedTimestampsParams,
)
from thess_geo_analytics.pipelines.BuildNdviAggregatedCompositePipeline import (
    BuildNdviAggregatedCompositePipeline,
    BuildNdviAggregatedCompositeParams,
)
from thess_geo_analytics.pipelines.BuildNdviMonthlyStatisticsPipeline import (
    BuildNdviMonthlyStatisticsPipeline,
    BuildNdviMonthlyStatisticsParams,
)
from thess_geo_analytics.pipelines.BuildNdviClimatologyPipeline import (
    BuildNdviClimatologyPipeline,
    BuildNdviClimatologyParams,
)

from thess_geo_analytics.pipelines.BuildNdviAnomalyMapsPipeline import (
    BuildNdviAnomalyMapsParams,
    BuildNdviAnomalyMapsPipeline,
)

from thess_geo_analytics.utils.RepoPaths import RepoPaths

from tests.mocks.MockNutsService import MockNutsService
from tests.mocks.MockCdseSceneCatalogService import MockCdseSceneCatalogService
from tests.mocks.MockCdseStacService import MockCdseStacService
from tests.mocks.MockCdseAssetDownloader import MockCdseAssetDownloader
from tests.mocks.MockStacAssetResolver import MockStacAssetResolver


class WholePipelineTest(unittest.TestCase):

    # -------------------------------------------------
    # Test environment
    # -------------------------------------------------

    def setUp(self) -> None:
        self.session_root = Path("tests/artifacts/pipeline_runs/session_single").resolve()

        if self.session_root.exists():
            shutil.rmtree(self.session_root)

        self.session_root.mkdir(parents=True)

        os.environ["THESS_RUN_ROOT"] = str(self.session_root)

        self.cfg = load_pipeline_config()

        print("\n[TEST RUN ROOT]", RepoPaths.run_root())

    def tearDown(self) -> None:
        os.environ.pop("THESS_RUN_ROOT", None)

    # -------------------------------------------------
    # Utility guards
    # -------------------------------------------------

    def _assert_exists(self, p: Path, msg: str):
        self.assertTrue(p.exists(), f"{msg} → missing: {p}")

    def _assert_nonempty_csv(self, p: Path, msg: str) -> pd.DataFrame:
        self._assert_exists(p, msg)
        df = pd.read_csv(p)
        self.assertGreater(len(df), 0, f"{msg} → CSV empty: {p}")
        return df

    def _assert_raster_ok(self, p: Path, msg: str):
        self._assert_exists(p, msg)
        with rasterio.open(p) as ds:
            self.assertGreater(ds.width, 0)
            self.assertGreater(ds.height, 0)

    # -------------------------------------------------
    # Step 1 — AOI
    # -------------------------------------------------

    def _step_01_extract_aoi(self) -> Path:

        pipe = ExtractAoiPipeline(nuts_service=MockNutsService())
        pipe.run(self.cfg.region_name)
        aoi_path = self.cfg.aoi_path
        self._assert_exists(aoi_path, "AOI file not produced")

        return aoi_path

    # -------------------------------------------------
    # Step 2 — Scene catalog
    # -------------------------------------------------

    def _step_02_scene_catalog(self, aoi_path: Path):

        svc = MockCdseSceneCatalogService(
            seed=1337,
            tiles_min=2,
            tiles_max=11,
            revisit_days=5,
            full_cover_pad_deg=0.02,
        )

        pipe = BuildSceneCatalogPipeline(aoi_path=aoi_path, service=svc)
        params = BuildSceneCatalogParams(
            date_start="2021-01-01",
            cloud_cover_max=80,
            max_items=300,
            use_tile_selector=True,
            full_cover_threshold=0.95,
            allow_union=False,
            max_union_tiles=1,
            n_anchors=24,
            window_days=30,
        )
        out = pipe.run(params)
        self._assert_exists(out, "Scene catalog output missing")
        tables = RepoPaths.outputs("tables")
        self._assert_nonempty_csv(tables / "scenes_catalog.csv", "scenes_catalog.csv")
        self._assert_nonempty_csv(tables / "scenes_selected.csv", "scenes_selected.csv")
        self._assert_nonempty_csv(tables / "time_serie.csv", "time_serie.csv")

    # -------------------------------------------------
    # Step 3 — Asset manifest + downloads
    # -------------------------------------------------

    def _step_03_assets_manifest(self):

        pipe = BuildAssetsManifestPipeline(
            stac_service=MockCdseStacService(band_resolution=10),
            downloader=MockCdseAssetDownloader(),
            resolver=MockStacAssetResolver(band_resolution=10),
        )
        out = pipe.run(
            BuildAssetsManifestParams(
                download_n=9,
                download_missing=True,
                validate_rasterio=True,
            )
        )
        self._assert_exists(out, "Assets manifest not written")
        tables = RepoPaths.outputs("tables")
        manifest = self._assert_nonempty_csv(
            tables / "assets_manifest_selected.csv",
            "assets manifest",
        )
        status = self._assert_nonempty_csv(
            tables / "assets_download_status.csv",
            "download status",
        )
        self.assertGreater((status["status"] == "success").sum(), 0)
        scene_id = str(manifest.iloc[0]["scene_id"])
        raw_dir = RepoPaths.run_root() / "raw" / "s2" / scene_id
        for band in ["B04", "B08", "SCL"]:
            self._assert_exists(raw_dir / f"{band}.tif", f"raw band missing {band}")

    # -------------------------------------------------
    # Step 4 — Aggregation
    # -------------------------------------------------

    def _step_04_aggregate(self) -> Path:

        params = TimestampsAggregationParams(
            max_workers=2,
            debug=True,
        )
        builder = TimestampsAggregationBuilder(params)
        out = builder.run()
        self.assertGreater(len(out), 0)

        for folder in out[:3]:
            for band in ["B04", "B08", "SCL"]:
                self._assert_raster_ok(folder / f"{band}.tif", "aggregated raster")

        aggregated_root = RepoPaths.run_root() / "data_raw" / "aggregated"
        self._assert_exists(aggregated_root, "aggregated root missing")

        return aggregated_root

    # -------------------------------------------------
    # Step 5 — Downsample
    # -------------------------------------------------

    def _step_05_downsample(self, aggregated_root: Path) -> Path:

        dst_root = RepoPaths.run_root() / "data_raw" / "aggregated_100m"
        pipe = BuildDownsampledAggregatedTimestampsPipeline()
        outputs = pipe.run(
            BuildDownsampledAggregatedTimestampsParams(
                src_root=aggregated_root,
                dst_root=dst_root,
                factor=1,
            )
        )
        self.assertGreater(len(outputs), 0)
        self._assert_raster_ok(outputs[0], "downsample raster")
        return dst_root

    # -------------------------------------------------
    # Step 6 — NDVI composites
    # -------------------------------------------------

    def _step_06_ndvi(self, aggregated_root: Path):

        aoi = self.cfg.aoi_path

        params = BuildNdviAggregatedCompositeParams(
            aoi_path=aoi,
            aoi_id=self.cfg.aoi_id,
            aggregated_root=aggregated_root,
            strategy="monthly",
            max_scenes_per_period=3,
            min_scenes_per_month=1,
            fallback_to_quarterly=True,
            enable_cloud_masking=True,
        )

        pipe = BuildNdviAggregatedCompositePipeline()
        results = pipe.run(params, max_workers=1, debug=True)
        self.assertGreater(len(results), 0)
        cogs = RepoPaths.outputs("cogs")
        self._assert_exists(cogs, "NDVI COG directory missing")
        ndvi = list(cogs.glob(f"ndvi_*_{self.cfg.aoi_id}.tif"))
        self.assertGreater(len(ndvi), 0)
        self._assert_raster_ok(ndvi[0], "ndvi composite")

    # -------------------------------------------------
    # Step 7 — Monthly statistics
    # -------------------------------------------------

    def _step_07_statistics(self):

        params = BuildNdviMonthlyStatisticsParams(
            aoi_id=self.cfg.aoi_id
        )
        pipe = BuildNdviMonthlyStatisticsPipeline()
        parquet, fig = pipe.run(params)

        self._assert_exists(parquet, "timeseries parquet")
        self._assert_exists(fig, "timeseries plot")
        stats_csv = RepoPaths.table("ndvi_period_stats.csv")

        self._assert_exists(stats_csv, "period stats CSV")
        df = self._assert_nonempty_csv(stats_csv, "period stats")
        self.assertIn("mean_ndvi", df.columns)

    # -------------------------------------------------
    # Step 8 — Climatology
    # -------------------------------------------------

    def _step_08_climatology(self):

        params = BuildNdviClimatologyParams(
            aoi_id=self.cfg.aoi_id
        )

        pipe = BuildNdviClimatologyPipeline()
        out_csv, out_fig = pipe.run(params)
        self._assert_exists(out_csv, "climatology CSV")
        self._assert_exists(out_fig, "climatology figure")
        df = self._assert_nonempty_csv(out_csv, "climatology")
        self.assertIn("mean_ndvi_clim", df.columns)

    # -------------------------------------------------
    # Step 9 — NDVI Anomaly Maps
    # -------------------------------------------------

    def _step_09_anomaly_maps(self):

        params = BuildNdviAnomalyMapsParams(
            aoi_id=self.cfg.aoi_id,
            verbose=True,
        )

        pipe = BuildNdviAnomalyMapsPipeline()
        results = pipe.run(params)

        self.assertGreater(len(results), 0)

        # at least one output should exist
        label, tif_path, png_path = results[0]
        self._assert_exists(tif_path, "anomaly GeoTIFF")
        self._assert_exists(png_path, "anomaly preview PNG")

        # sanity: should live in outputs/cogs under the test run root
        cogs = RepoPaths.outputs("cogs")
        self._assert_exists(cogs, "cogs dir missing after anomaly step")
        self.assertGreater(len(list(cogs.glob(f"ndvi_anomaly_*_{self.cfg.aoi_id}.tif"))), 0)

        
    # -------------------------------------------------
    # Orchestrator
    # -------------------------------------------------

    def test_pipeline_smoke(self):

        aoi = self._step_01_extract_aoi()
        self._step_02_scene_catalog(aoi)
        self._step_03_assets_manifest()
        aggregated = self._step_04_aggregate()
        downsampled = self._step_05_downsample(aggregated)
        self._step_06_ndvi(downsampled)
        self._step_07_statistics()
        self._step_08_climatology()
        self._step_09_anomaly_maps()