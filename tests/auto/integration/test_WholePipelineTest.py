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
from tests.mocks.MockCdseStacService import MockCdseStacService
from tests.mocks.MockCdseAssetDownloader import MockCdseAssetDownloader
from tests.mocks.MockStacAssetResolver import MockStacAssetResolver


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
            cloud_cover_max=80.0,
            max_items=300,
            use_tile_selector=True,
            full_cover_threshold=0.95,
            allow_union=False,
            max_union_tiles=1,
            n_anchors=24,
            window_days=30,
        )

        out = pipeline.run(params)
        self.assertTrue(out.exists(), "Pipeline did not return an output path")

        tables_dir = self.session_root / "outputs" / "tables"
        self.assertTrue((tables_dir / "scenes_catalog.csv").exists(), "scenes_catalog.csv missing")
        self.assertTrue((tables_dir / "timestamps_coverage.csv").exists(), "timestamps_coverage.csv missing")
        self.assertTrue((tables_dir / "scenes_selected.csv").exists(), "scenes_selected.csv missing")
        self.assertTrue((tables_dir / "time_serie.csv").exists(), "time_serie.csv missing")

        raw_df = pd.read_csv(tables_dir / "scenes_catalog.csv")
        self.assertGreater(len(raw_df), 0, "scenes_catalog.csv should not be empty")
        self.assertGreaterEqual(len(raw_df), 250, "expected a large catalog (near max_items)")

        sel_df = pd.read_csv(tables_dir / "scenes_selected.csv")
        ts_df = pd.read_csv(tables_dir / "time_serie.csv")

        self.assertGreater(len(sel_df), 0, "scenes_selected.csv should not be empty")
        self.assertGreater(len(ts_df), 0, "time_serie.csv should not be empty")

    # -------------------------------------------------
    # Step 3 — Assets manifest
    # -------------------------------------------------
    def _step_03_assets_manifest(self) -> None:
        pipe = BuildAssetsManifestPipeline(
            stac_service=MockCdseStacService(band_resolution=10),
            downloader=MockCdseAssetDownloader(),
            resolver=MockStacAssetResolver(band_resolution=10),
        )

        out_path = pipe.run(
            BuildAssetsManifestParams(
                max_scenes=None,
                date_start="2023-01-01",
                sort_mode="cloud_then_time",
                download_n=9,
                download_missing=True,
                validate_rasterio=True,
                out_name="assets_manifest_selected.csv",
                raw_storage_mode="url_to_local",
                band_resolution=10,
                max_download_workers=4,
            )
        )

        self.assertTrue(out_path.exists(), "Pipeline did not write manifest CSV")

        tables_dir = self.session_root / "outputs" / "tables"

        manifest_path = tables_dir / "assets_manifest_selected.csv"
        self.assertTrue(manifest_path.exists(), "assets_manifest_selected.csv missing")

        status_path = tables_dir / "assets_download_status.csv"
        self.assertTrue(status_path.exists(), "assets_download_status.csv missing")

        manifest_df = pd.read_csv(manifest_path)
        self.assertGreater(len(manifest_df), 0, "manifest CSV is empty")
        self.assertFalse(
            manifest_df[["href_b04", "href_b08", "href_scl"]].isna().any().any(),
            "Missing hrefs in manifest",
        )

        status_df = pd.read_csv(status_path)
        self.assertGreater(
            (status_df["status"] == "success").sum(),
            0,
            "No successful downloads",
        )

    # -------------------------------------------------
    # Orchestrator
    # -------------------------------------------------
    def test_pipeline_smoke(self) -> None:
        aoi_path = self._step_01_extract_aoi()
        self._step_02_scene_catalog(aoi_path)
        self._step_03_assets_manifest()