from __future__ import annotations

import os
import shutil
import unittest
from pathlib import Path

import pandas as pd

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

from tests.mocks.MockNutsService import MockNutsService
from tests.mocks.MockCdseSceneCatalogService import MockCdseSceneCatalogService


class WholePipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.session_root = Path("tests/artifacts/pipeline_runs/session_single").resolve()

        if self.session_root.exists():
            shutil.rmtree(self.session_root)

        self.session_root.mkdir(parents=True)
        os.environ["THESS_RUN_ROOT"] = str(self.session_root)

        self.cfg = load_pipeline_config()

    def tearDown(self) -> None:
        os.environ.pop("THESS_RUN_ROOT", None)

    # ------------------------------
    # Step 1
    # ------------------------------
    def _step_01_extract_aoi(self) -> Path:
        pipeline = ExtractAoiPipeline(nuts_service=MockNutsService())
        pipeline.run(self.cfg.region_name)

        aoi_path = self.cfg.aoi_path
        self.assertTrue(aoi_path.exists(), "AOI file not produced")
        return aoi_path

    # ------------------------------
    # Step 2
    # ------------------------------
    def _step_02_scene_catalog(self, aoi_path: Path) -> None:
        # Mock STAC catalog: many scenes, spread across [date_start..today]
        svc = MockCdseSceneCatalogService(
            seed=1337,
            tiles_min=2,
            tiles_max=11,
            revisit_days=5,
            full_cover_pad_deg=0.02,
        )

        pipeline = BuildSceneCatalogPipeline(aoi_path=aoi_path, service=svc)

        params = BuildSceneCatalogParams(
            date_start="2021-01-01",
            cloud_cover_max=80.0,   # allow most mock scenes
            max_items=300,          # IMPORTANT: this should give you ~300 rows in scenes_catalog.csv
            use_tile_selector=True,
            full_cover_threshold=0.95,  # <--- THIS is where you define "full coverage"
            allow_union=False,          # single full-cover tile exists per timestamp
            max_union_tiles=1,
            n_anchors=24,
            window_days=30,
        )

        out = pipeline.run(params)
        self.assertTrue(out.exists(), "Pipeline did not return an output path")

        tables = self.session_root / "outputs" / "tables"
        self.assertTrue((tables / "scenes_catalog.csv").exists(), "scenes_catalog.csv missing")
        self.assertTrue((tables / "timestamps_coverage.csv").exists(), "timestamps_coverage.csv missing")
        self.assertTrue((tables / "scenes_selected.csv").exists(), "scenes_selected.csv missing")
        self.assertTrue((tables / "time_serie.csv").exists(), "time_serie.csv missing")

        raw = pd.read_csv(tables / "scenes_catalog.csv")
        self.assertGreater(len(raw), 0, "scenes_catalog.csv should not be empty")
        # typically should be very close to max_items (unless cloud filter removes a lot)
        self.assertGreaterEqual(len(raw), 250, "expected a large catalog (near max_items)")

        sel = pd.read_csv(tables / "scenes_selected.csv")
        ts = pd.read_csv(tables / "time_serie.csv")

        self.assertGreater(len(sel), 0, "scenes_selected.csv should not be empty")
        self.assertGreater(len(ts), 0, "time_serie.csv should not be empty")

    # -------------------------------------------------
    # Step 3 — Assets manifest
    # -------------------------------------------------


    def _step_03_assets_manifest(self):

        from thess_geo_analytics.entrypoints.BuildAssetsManifest import run
        from tests.mocks.MockCdseStacService import MockCdseStacService
        from tests.mocks.MockCdseAssetDownloader import MockCdseAssetDownloader



        run(
            stac_service=MockCdseStacService(),
            asset_downloader=MockCdseAssetDownloader(),
        )

        tables = self.session_root / "outputs" / "tables"

        manifest = tables / "assets_manifest_selected.csv"
        self.assertTrue(manifest.exists())

    # -------------------------------------------------
    # Orchestrator
    # -------------------------------------------------

    def test_pipeline_smoke(self) -> None:

        aoi_path = self._step_01_extract_aoi()
        self._step_02_scene_catalog(aoi_path)
        self._step_03_assets_manifest()