from __future__ import annotations

import os
import shutil
import unittest
from pathlib import Path

from thess_geo_analytics.pipelines.ExtractAoiPipeline import ExtractAoiPipeline
from tests.mocks.MockNutsService import MockNutsService


class WholePipelineTest(unittest.TestCase):

    def setUp(self):

        self.session_root = Path(
            "tests/artifacts/pipeline_runs/session_single"
        ).resolve()

        if self.session_root.exists():
            shutil.rmtree(self.session_root)

        self.session_root.mkdir(parents=True)

        os.environ["THESS_RUN_ROOT"] = str(self.session_root)

    def tearDown(self):
        os.environ.pop("THESS_RUN_ROOT", None)

    def test_01_extract_aoi(self):

        pipeline = ExtractAoiPipeline(
            nuts_service=MockNutsService()
        )

        pipeline.run("Thessaloniki")

        aoi_dir = self.session_root / "aoi"
        files = list(aoi_dir.glob("*.geojson"))

        self.assertTrue(files, "AOI file was not produced")