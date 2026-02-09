from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.geo.CloudMasker import CloudMasker
from thess_geo_analytics.geo.NdviProcessor import NdviProcessor
from thess_geo_analytics.geo.RasterWriter import RasterWriter



@dataclass(frozen=True)
class BuildNdviSceneParams:
    month: str
    scene_id: str


class BuildNdviScenePipeline:
    def __init__(self) -> None:
        self.masker = CloudMasker()
        self.ndvi = NdviProcessor()
        self.writer = RasterWriter()


    def run(self, params: BuildNdviSceneParams) -> tuple[Path, Path]:
        manifest_path = RepoPaths.table(f"assets_manifest_{params.month}.csv")
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing assets manifest: {manifest_path}")

        df = pd.read_csv(manifest_path)
        row = df[df["scene_id"] == params.scene_id]
        if row.empty:
            raise ValueError(f"Scene not found in manifest: {params.scene_id}")

        r = row.iloc[0]
        b04_path = Path(r["local_b04"])
        b08_path = Path(r["local_b08"])
        scl_path = Path(r["local_scl"])

        for p in [b04_path, b08_path, scl_path]:
            if not p.exists():
                raise FileNotFoundError(f"Missing local asset: {p}")

        red, nir, out_profile = self.ndvi.read_bands(b04_path, b08_path)
        scl_on_grid, scl_nodata = self.masker.read_scl_as_target_grid(scl_path=scl_path, target_profile=out_profile)
        invalid_mask = self.masker.build_invalid_mask_from_scl(scl_on_grid, scl_nodata=scl_nodata)

        ndvi = self.ndvi.compute_ndvi(red=red, nir=nir)
        ndvi_masked = self.ndvi.apply_mask_to_ndvi(ndvi, invalid_mask=invalid_mask)

        # outputs
        RepoPaths.TMP.mkdir(parents=True, exist_ok=True)
        RepoPaths.FIGURES.mkdir(parents=True, exist_ok=True)

        out_tif = RepoPaths.tmp(f"ndvi_scene_{params.scene_id}.tif")
        out_png = RepoPaths.figure(f"ndvi_scene_{params.scene_id}_preview.png")

        arr_out = self.ndvi.to_nodata(ndvi_masked)
        self.writer.write_geotiff(out_tif, arr_out, out_profile)
        self.writer.write_preview_png(out_png, ndvi_masked)


        return out_tif, out_png

    @staticmethod
    def smoke_test() -> None:
        print("=== BuildNdviScenePipeline Smoke Test ===")
        # This requires a real manifest + local assets. You can set env vars or edit manually.
        print("[SKIP] Provide a real month+scene_id in entrypoint to run fully.")
        print("✓ Smoke test OK")
        

if __name__ == "__main__":
    BuildNdviScenePipeline.smoke_test()
