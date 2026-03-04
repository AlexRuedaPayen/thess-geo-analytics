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

from tests.mocks.MockNutsService import MockNutsService
from tests.mocks.MockCdseSceneCatalogService import MockCdseSceneCatalogService


class WholePipelineTest(unittest.TestCase):

    def setUp(self):

        self.session_root = Path(
            "tests/artifacts/pipeline_runs/session_single"
        ).resolve()

        if self.session_root.exists():
            shutil.rmtree(self.session_root)

        self.session_root.mkdir(parents=True)

        os.environ["THESS_RUN_ROOT"] = str(self.session_root)

        self.cfg = load_pipeline_config()

    def tearDown(self):
        os.environ.pop("THESS_RUN_ROOT", None)

    # ------------------------------
    # Step 1
    # ------------------------------
    def _step_01_extract_aoi(self):

        pipeline = ExtractAoiPipeline(
            nuts_service=MockNutsService()
        )

        pipeline.run(self.cfg.region_name)

        aoi_path = self.cfg.aoi_path

        self.assertTrue(aoi_path.exists(), "AOI file not produced")

        return aoi_path

    # ------------------------------
    # Step 2
    # ------------------------------
    def _step_02_scene_catalog(self):
        aoi_file = next((self.session_root / "aoi").glob("*.geojson"))

        svc = MockCdseSceneCatalogService(
            n_timestamps=8,
            step_days=7,
            buffer_deg=0.05,
        )

        pipeline = BuildSceneCatalogPipeline(aoi_path=aoi_file, service=svc)

        params = BuildSceneCatalogParams(
            date_start="2021-01-01",
            cloud_cover_max=50.0,
            max_items=1000,
            use_tile_selector=True,
            full_cover_threshold=0.999,
            allow_union=False,     # 1 tile already fully covers, keep it simple
            max_union_tiles=1,
            n_anchors=64,
            window_days=30,
        )

        out = pipeline.run(params)

        tables = self.session_root / "outputs" / "tables"

        ts_path = tables / "time_serie.csv"
        sel_path = tables / "scenes_selected.csv"

        self.assertTrue(ts_path.exists())
        self.assertTrue(sel_path.exists())

        ts = pd.read_csv(ts_path)   # now safe because headers exist
        sel = pd.read_csv(sel_path)

        self.assertGreater(len(ts), 0)
        self.assertGreater(len(sel), 0)
            # ------------------------------
    # Orchestrator
    # ------------------------------
    def test_pipeline_smoke(self):

        self._step_01_extract_aoi()
        self._step_02_scene_catalog()