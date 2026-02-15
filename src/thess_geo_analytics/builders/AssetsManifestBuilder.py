from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd

from thess_geo_analytics.core.settings import CACHE_S2_DIR, DEFAULT_COLLECTION
from thess_geo_analytics.services.CdseStacService import CdseStacService
from thess_geo_analytics.services.StacAssetResolver import StacAssetResolver


@dataclass(frozen=True)
class AssetsManifestBuildParams:
    month: str                      # "YYYY-MM"
    max_scenes: int = 10
    collection: str = DEFAULT_COLLECTION
    cache_root: Path = CACHE_S2_DIR


class AssetsManifestBuilder:
    """
    Builds the assets manifest DataFrame:
      scene_id, datetime, cloud_cover,
      href_b04, href_b08, href_scl,
      local_b04, local_b08, local_scl

    No file I/O and no downloads here.
    """

    REQUIRED_SCENES_COLS = {"id", "datetime", "cloud_cover"}

    def __init__(
        self,
        stac: CdseStacService | None = None,
        resolver: StacAssetResolver | None = None,
    ) -> None:
        self.stac = stac or CdseStacService()
        self.resolver = resolver or StacAssetResolver()

    def build_assets_manifest_df(self, scenes_df: pd.DataFrame, params: AssetsManifestBuildParams) -> pd.DataFrame:
        missing = self.REQUIRED_SCENES_COLS - set(scenes_df.columns)
        if missing:
            raise ValueError(f"Scenes catalog missing columns: {sorted(missing)}")

        df = scenes_df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
        df = df.dropna(subset=["datetime"])

        df_month = df[df["datetime"].dt.strftime("%Y-%m") == params.month].copy()
        df_month = df_month.sort_values(["cloud_cover", "datetime"], ascending=[True, True]).head(params.max_scenes)

        if df_month.empty:
            raise ValueError(f"No scenes found for month={params.month}")

        rows: List[dict] = []

        for _, r in df_month.iterrows():
            item_id = str(r["id"])
            item_json = self.stac.fetch_item(params.collection, item_id)
            hrefs = self.resolver.resolve_b04_b08_scl(item_json)

            scene_dir = params.cache_root / item_id

            rows.append(
                {
                    "scene_id": item_id,
                    "datetime": r["datetime"].isoformat(),
                    "cloud_cover": r.get("cloud_cover"),
                    "href_b04": hrefs.get("href_b04"),
                    "href_b08": hrefs.get("href_b08"),
                    "href_scl": hrefs.get("href_scl"),
                    "local_b04": str(scene_dir / "B04.tif"),
                    "local_b08": str(scene_dir / "B08.tif"),
                    "local_scl": str(scene_dir / "SCL.tif"),
                }
            )

        return pd.DataFrame(rows)

    @staticmethod
    def smoke_test() -> None:
        print("=== AssetsManifestBuilder Smoke Test ===")
        import pandas as pd
        from thess_geo_analytics.utils.RepoPaths import RepoPaths

        scenes_csv = RepoPaths.table("scenes_selected.csv")
        if not scenes_csv.exists():
            raise FileNotFoundError(f"Missing scenes catalog: {scenes_csv}")

        scenes_df = pd.read_csv(scenes_csv)
        builder = AssetsManifestBuilder()
        params = AssetsManifestBuildParams(month="2026-01", max_scenes=3)
        out_df = builder.build_assets_manifest_df(scenes_df, params)

        print("[OK] rows:", len(out_df))
        print(out_df.head(2)[["scene_id", "href_b04", "local_b04"]])
        print("✓ Smoke test OK")


if __name__ == "__main__":
    AssetsManifestBuilder.smoke_test()
